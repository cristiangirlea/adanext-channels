package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/cristiangirlea/adanext-channels/internal/ws"
	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type gatewayEvent struct {
	Op int
	T  string
	S  *int64
	D  json.RawMessage
}

type session struct {
	id, endpoint, self string
	seq                *int64
	// failedResumes counts consecutive Resume attempts that ended without RESUMED.
	failedResumes int
}

func (s *session) canResume() bool {
	return s.id != "" && s.endpoint != "" && s.seq != nil
}

var discordGatewayHost = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.discord\.gg(:443)?$`)
var errCannotResume = errors.New("Discord invalidated the session")

func gatewayURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "wss" || u.User != nil || u.Fragment != "" {
		return "", errors.New("refused Discord gateway URL")
	}
	// Require ASCII before case folding, which can otherwise turn Unicode into valid labels.
	if len(u.Host) != utf8.RuneCountInString(u.Host) || !discordGatewayHost.MatchString(strings.ToLower(u.Host)) {
		return "", errors.New("refused Discord gateway URL")
	}
	u.RawQuery = "v=10&encoding=json"
	return u.String(), nil
}

func (s *service) connect(ctx context.Context, session *session) (*ws.Conn, error) {
	raw := session.endpoint
	if !session.canResume() {
		if err := s.seedChannels(ctx); err != nil {
			return nil, err
		}
		var bot struct {
			URL   string
			Limit *struct {
				Remaining  int
				ResetAfter int64 `json:"reset_after"`
			} `json:"session_start_limit"`
		}
		if err := s.call(ctx, "GET", "/gateway/bot", nil, &bot); err != nil {
			return nil, err
		}
		// Wait before opening the socket so the Identify delay cannot starve heartbeats.
		delay := s.lastIdentify.Add(5 * time.Second).Sub(s.now())
		if limit := bot.Limit; limit != nil && limit.Remaining == 0 {
			if limit.ResetAfter < 0 || limit.ResetAfter > math.MaxInt64/int64(time.Millisecond) {
				return nil, errors.New("invalid Discord session start limit reset interval")
			}
			reset := time.Duration(limit.ResetAfter) * time.Millisecond
			s.log.Printf("Discord session start limit exhausted, waiting %v", reset)
			if err := sdk.Status(ctx, p.StateReconnecting, "waiting for Discord session start limit"); err != nil {
				return nil, err
			}
			delay = max(delay, reset)
		}
		if delay > 0 {
			if err := s.wait(ctx, delay); err != nil {
				return nil, err
			}
		}
		raw = bot.URL
	}
	addr, err := gatewayURL(raw)
	if err != nil {
		return nil, err
	}
	header := http.Header{}
	header.Set("User-Agent", userAgent)
	return s.dialGateway(ctx, addr, header)
}

var terminalCloseCauses = map[int]string{
	4004: "authentication failed",
	4010: "invalid shard",
	4011: "sharding required",
	4012: "invalid API version",
	4013: "invalid intents",
	4014: "disallowed intents: enable the Message Content intent in the developer portal or remove ADA_DISCORD_GUILD/ADA_DISCORD_CHANNELS",
}

func (s *service) gateway(ctx context.Context) error {
	var sess session
	for attempt := 0; ctx.Err() == nil; attempt++ {
		healthy, err := s.connection(ctx, &sess)
		if healthy {
			attempt = 0
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		state := p.StateReconnecting
		var delay time.Duration
		var closeErr *ws.CloseError
		if errors.As(err, &closeErr) {
			if cause, fatal := terminalCloseCauses[closeErr.Code]; fatal {
				state = p.StateStopped
				if closeErr.Code == 4004 {
					state = p.StateUnauthorised
				}
				s.log.Printf("Discord gateway closed (%d): %s", closeErr.Code, cause)
				if err := sdk.Status(ctx, state, cause); err != nil {
					return err
				}
				// Keep Receive idle while the SDK services outbound REST deliveries.
				<-ctx.Done()
				return ctx.Err()
			}
			if closeErr.Code == 4007 || closeErr.Code == 4009 {
				sess = session{}
			}
		}
		if errors.Is(err, errCannotResume) {
			sess = session{}
			delay = s.invalidSessionDelay()
		}
		var de *sdk.DeliveryError
		if errors.As(err, &de) {
			delay = de.RetryAfter
			if de.Code == p.CodeUnauthorised {
				state = p.StateUnauthorised
			}
		}
		s.log.Printf("Discord gateway: %v", err)
		if err := sdk.Status(ctx, state, ""); err != nil {
			return fmt.Errorf("report Discord connection: %w", err)
		}
		if err := s.backoff.Wait(ctx, attempt, delay); err != nil {
			return fmt.Errorf("wait for Discord: %w", err)
		}
	}
	return ctx.Err()
}

func readEvent(ctx context.Context, conn *ws.Conn) (gatewayEvent, error) {
	var e gatewayEvent
	data, err := conn.Read(ctx)
	if err != nil {
		return e, err
	}
	if err = json.Unmarshal(data, &e); err != nil {
		return e, errors.New("invalid Discord gateway event")
	}
	return e, nil
}

func writeEvent(ctx context.Context, c *ws.Conn, op int, d any) error {
	body, err := json.Marshal(struct {
		Op int `json:"op"`
		D  any `json:"d"`
	}{op, d})
	if err != nil {
		return fmt.Errorf("encode gateway command: %w", err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.Write(writeCtx, body)
}

func (s *service) authenticate(ctx context.Context, c *ws.Conn, sess *session) error {
	if sess.canResume() {
		return writeEvent(ctx, c, 6, map[string]any{"token": s.token, "session_id": sess.id, "seq": *sess.seq})
	}
	intents := 1 << 12
	if s.guild != "" {
		intents |= 1<<9 | 1<<15
	}
	err := writeEvent(ctx, c, 2, map[string]any{"token": s.token, "intents": intents, "properties": map[string]string{"os": runtime.GOOS, "browser": "ada-channel-discord", "device": "ada-channel-discord"}})
	s.lastIdentify = s.now()
	return err
}

type heartbeat struct {
	sequence atomic.Pointer[int64]
	pending  atomic.Bool
}

func (h *heartbeat) send(ctx context.Context, c *ws.Conn) error {
	h.pending.Store(true)
	return writeEvent(ctx, c, 1, h.sequence.Load())
}

func (h *heartbeat) run(ctx context.Context, c *ws.Conn, interval time.Duration) error {
	timer := time.NewTimer(time.Duration(rand.Int64N(int64(interval))))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if h.pending.Load() {
				return errors.New("Discord heartbeat was not acknowledged")
			}
			if err := h.send(ctx, c); err != nil {
				return err
			}
			timer.Reset(interval)
		}
	}
}

// Read control traffic independently of history pagination and the host's acknowledgements.
func (s *service) pump(ctx context.Context, c *ws.Conn, h *heartbeat, events chan<- gatewayEvent, callbacks chan<- callback) error {
	for {
		e, err := readEvent(ctx, c)
		if err != nil {
			return err
		}
		if e.S != nil {
			h.sequence.Store(e.S)
		}
		switch e.Op {
		case 1:
			if err := h.send(ctx, c); err != nil {
				return err
			}
		case 11:
			h.pending.Store(false)
		case 7:
			return errors.New("Discord requested reconnect")
		case 9:
			if string(e.D) != "true" {
				return errCannotResume
			}
			return errors.New("invalid Discord session")
		case 0:
			if e.T == "INTERACTION_CREATE" {
				var q callback
				if json.Unmarshal(e.D, &q) == nil {
					select {
					case callbacks <- q:
					default:
						s.log.Printf("Discord callback queue is full, dropping acknowledgement")
					}
				}
			}
			// Resume from the last processed dispatch if the host falls behind.
			select {
			case events <- e:
			case <-ctx.Done():
				return ctx.Err()
			default:
				return errors.New("Discord dispatch queue is full")
			}
		}
	}
}

func (s *service) connection(ctx context.Context, sess *session) (healthy bool, err error) {
	resumePending := sess.canResume()
	defer func() {
		if resumePending {
			// Rediscover the gateway when the resume host twice fails to confirm the session.
			sess.failedResumes++
			if sess.failedResumes >= 2 {
				*sess = session{}
			}
		}
	}()
	c, err := s.connect(ctx, sess)
	if err != nil {
		return false, err
	}
	opened := s.now()
	defer c.Close(4000, "reconnect")
	helloCtx, stopHello := context.WithTimeout(ctx, 20*time.Second)
	hello, err := readEvent(helloCtx, c)
	stopHello()
	if err != nil {
		return false, err
	}
	var data struct {
		Interval int64 `json:"heartbeat_interval"`
	}
	if hello.Op != 10 || json.Unmarshal(hello.D, &data) != nil || data.Interval <= 0 || data.Interval > int64((24*time.Hour)/time.Millisecond) {
		return false, errors.New("invalid Discord Hello")
	}
	if err := s.authenticate(ctx, c, sess); err != nil {
		return false, err
	}
	ioCtx, stop := context.WithCancelCause(ctx)
	var beats heartbeat
	beats.sequence.Store(sess.seq)
	events := make(chan gatewayEvent, 64)
	callbacks := make(chan callback, 64)
	var workers sync.WaitGroup
	var disconnected time.Time
	var pumpErr error
	var caughtUp bool
	workers.Go(func() {
		for {
			select {
			case <-ioCtx.Done():
				return
			case q := <-callbacks:
				s.answer(ioCtx, q)
			}
		}
	})
	workers.Go(func() { stop(beats.run(ioCtx, c, time.Duration(data.Interval)*time.Millisecond)) })
	workers.Go(func() {
		defer close(events)
		pumpErr = s.pump(ioCtx, c, &beats, events, callbacks)
		disconnected = s.now()
		stop(pumpErr)
	})
	defer func() {
		stop(context.Canceled)
		workers.Wait()
		healthy = healthy || caughtUp && disconnected.Sub(opened) >= 30*time.Second
		// Preserve the gateway's instructions even if dispatch processing failed concurrently.
		var closed *ws.CloseError
		if errors.As(pumpErr, &closed) || errors.Is(pumpErr, errCannotResume) {
			err = pumpErr
		}
	}()
	for e := range events {
		switch e.T {
		case "READY":
			var ready struct {
				ID   string `json:"session_id"`
				URL  string `json:"resume_gateway_url"`
				User user
			}
			if err := json.Unmarshal(e.D, &ready); err != nil || ready.ID == "" || snowflake(ready.User.ID) == 0 {
				return false, errors.New("invalid Discord READY")
			}
			*sess = session{self: ready.User.ID}
			if err := sdk.Status(ctx, p.StateConnected, ""); err != nil {
				return healthy, err
			}
			if err := s.catchUp(ctx, sess.self); err != nil {
				return healthy, err
			}
			caughtUp = true
			// A failed catch-up must Identify again before accepting any live dispatch.
			if addr, err := gatewayURL(ready.URL); err == nil {
				sess.endpoint = addr
				sess.id = ready.ID
			}
		case "RESUMED":
			resumePending = false
			sess.failedResumes = 0
			healthy = true
			if err := sdk.Status(ctx, p.StateConnected, ""); err != nil {
				return healthy, err
			}
		default:
			if sess.self != "" {
				if err := s.live(ctx, e, sess.self); err != nil {
					return healthy, err
				}
			}
		}
		if e.S != nil {
			sess.seq = e.S
		}
	}
	return healthy, context.Cause(ioCtx)
}
