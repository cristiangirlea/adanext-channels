package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cristiangirlea/adanext-channels/internal/ws"
	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

var socketHost = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.slack\.com(:443)?$`)

func socketURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	// Check ASCII before folding so Unicode cannot alias a trusted hostname.
	if err != nil || u.Scheme != "wss" || u.User != nil || u.Fragment != "" || len(u.Host) != utf8.RuneCountInString(u.Host) || !socketHost.MatchString(strings.ToLower(u.Host)) {
		return "", errors.New("refused Slack socket URL")
	}
	return u.String(), nil
}

type envelope struct {
	Type, Reason, Error string
	ID                  string `json:"envelope_id"`
	Payload             json.RawMessage
}

func readEnvelope(ctx context.Context, c *ws.Conn) (envelope, error) {
	var e envelope
	b, err := c.Read(ctx)
	if err != nil {
		return e, fmt.Errorf("read Slack socket: %w", err)
	}
	if json.Unmarshal(b, &e) != nil {
		return e, errors.New("invalid Slack socket envelope")
	}
	if e.Error == "invalid_auth" || e.Error == "not_authed" || e.Reason == "invalid_auth" || e.Reason == "not_authed" {
		return e, failure(p.CodeUnauthorised, "Slack socket authentication failed")
	}
	return e, nil
}

func (s *service) open(ctx context.Context) (*ws.Conn, error) {
	r, err := s.call(ctx, "POST", "apps.connections.open", nil, nil)
	if err != nil {
		return nil, err
	}
	addr, err := socketURL(r.URL)
	if err != nil {
		return nil, err
	}
	helloCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := s.dial(helloCtx, addr, nil)
	if err != nil {
		return nil, fmt.Errorf("open Slack socket: %w", err)
	}
	e, err := readEnvelope(helloCtx, conn)
	if err == nil && e.Type == "hello" {
		return conn, nil
	}
	conn.Close(1000, "")
	if err != nil {
		return nil, err
	}
	return nil, errors.New("expected Slack socket hello")
}

type receipt struct {
	conn *ws.Conn
	id   string
}

func (r receipt) ack(ctx context.Context) error {
	if r.conn == nil {
		return nil
	}
	data, err := json.Marshal(struct {
		ID string `json:"envelope_id"`
	}{r.id})
	if err != nil {
		return fmt.Errorf("encode Slack acknowledgement: %w", err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.conn.Write(writeCtx, data); err != nil {
		return fmt.Errorf("acknowledge Slack envelope: %w", err)
	}
	return nil
}

func (s *service) enqueue(ctx context.Context, in incoming) error {
	select {
	case s.events <- in:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *service) listen(ctx context.Context, c *ws.Conn) (refresh bool, err error) {
	if err := sdk.Status(ctx, p.StateConnected, ""); err != nil {
		return false, err
	}
	// The barrier precedes this socket's live events even while Ada is committing an older message.
	if err := s.enqueue(ctx, incoming{history: true}); err != nil {
		return false, err
	}
	return s.readLive(ctx, c)
}

func (s *service) readLive(ctx context.Context, c *ws.Conn) (refresh bool, err error) {
	for {
		e, err := readEnvelope(ctx, c)
		if err != nil {
			return false, err
		}
		if e.Type == "disconnect" {
			return e.Reason == "refresh_requested" || e.Reason == "warning", errors.New("Slack requested a new socket")
		}
		if e.ID == "" {
			continue
		}
		in, ok := s.decode(e)
		r := receipt{c, e.ID}
		if !ok {
			if err := r.ack(ctx); err != nil {
				return false, err
			}
			continue
		}
		in.receipt = r
		if err := s.enqueue(ctx, in); err != nil {
			return false, err
		}
	}
}

func (s *service) replace(ctx context.Context, old *ws.Conn) (*ws.Conn, error) {
	if old == nil {
		return s.open(ctx)
	}
	// Slack may still deliver envelopes on the old socket until the replacement says hello.
	readCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.readLive(readCtx, old) }()
	conn, err := s.open(ctx)
	cancel()
	<-done
	old.Close(1000, "")
	return conn, err
}

func (s *service) gateway(ctx context.Context) {
	defer close(s.done)
	defer close(s.events)
	var old *ws.Conn
	defer func() {
		if old != nil {
			old.Close(1000, "")
		}
	}()
	for attempt := 0; ctx.Err() == nil; attempt++ {
		conn, err := s.replace(ctx, old)
		old = nil
		if err == nil {
			start := time.Now()
			var refresh bool
			refresh, err = s.listen(ctx, conn)
			if refresh {
				old = conn
			} else {
				conn.Close(1000, "")
			}
			if time.Since(start) >= 30*time.Second {
				attempt = 0
			}
		}
		if ctx.Err() != nil {
			return
		}
		s.log.Printf("Slack socket: %v", err)
		var delay time.Duration
		var de *sdk.DeliveryError
		if errors.As(err, &de) {
			if de.Code == p.CodeUnauthorised {
				sdk.Status(ctx, p.StateUnauthorised, de.Message)
				return
			}
			delay = de.RetryAfter
		}
		if err := sdk.Status(ctx, p.StateReconnecting, ""); err != nil {
			return
		}
		// A scheduled refresh can overlap the sockets without waiting for a failure backoff.
		if old == nil {
			if err := s.backoff.Wait(ctx, attempt, delay); err != nil {
				return
			}
		}
	}
}
