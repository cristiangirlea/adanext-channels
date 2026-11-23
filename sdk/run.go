package sdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/cristiangirlea/adanext-channels/internal/jsonline"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

var ErrRefused = errors.New("channel handshake refused")

const writeGrace = 250 * time.Millisecond

type hostLine struct {
	data []byte
	err  error
}

func readHost(ctx context.Context, cancel context.CancelFunc, in io.Reader, ended chan struct{}, drainGrace time.Duration) <-chan hostLine {
	lines := make(chan hostLine)
	go func() {
		defer close(lines)
		r := bufio.NewReader(in)
		for {
			data, err := jsonline.Read(r)
			last := err != nil && !errors.Is(err, jsonline.ErrTooLong)
			if last {
				close(ended)
				timer := time.AfterFunc(drainGrace, cancel)
				defer timer.Stop()
			}
			select {
			case lines <- hostLine{data, err}:
			case <-ctx.Done():
				return
			}
			if last {
				<-ctx.Done()
				return
			}
		}
	}()
	return lines
}

// A single lock covers sequence assignment, validation and the complete line write.
type output struct {
	mu        sync.Mutex
	w         io.Writer
	hostEnded <-chan struct{}
	seq       int64
	err       error
}

var errInvalidOutput = errors.New("validate channel output")
var errWriteOutput = errors.New("write channel output")

func (o *output) send(ctx context.Context, frame any) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	seq := o.seq + 1
	switch f := frame.(type) {
	case *p.Ready:
		f.V, f.Seq = 1, seq
	case *p.Error:
		f.V, f.Seq = 1, seq
	case *p.Inbound:
		f.V, f.Seq = 1, seq
	case *p.Delivered:
		f.V, f.Seq = 1, seq
	case *p.DeliveryFailed:
		f.V, f.Seq = 1, seq
	case *p.Status:
		f.V, f.Seq = 1, seq
	default:
		return fmt.Errorf("unsupported channel output %T", frame)
	}
	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("encode channel output: %w", err)
	}
	if _, err = p.ParsePluginLineV2(data, p.KindChannel); err != nil {
		return fmt.Errorf("%w: %w", errInvalidOutput, err)
	}
	if closer, ok := o.w.(io.Closer); ok {
		stop := o.watchWrite(ctx, closer)
		defer stop()
	}
	data = append(data, '\n')
	n, err := o.w.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		o.err = fmt.Errorf("%w: %w", errWriteOutput, err)
		return o.err
	}
	o.seq = seq
	return nil
}

// Limit each blocked write after EOF, including writes that started before EOF.
// A service can still finish work and start a fresh write later in the drain period.
func (o *output) watchWrite(ctx context.Context, closer io.Closer) func() {
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-done:
			return
		case <-ctx.Done():
		case <-o.hostEnded:
			select {
			case <-done:
				return
			case <-ctx.Done():
			case <-time.After(writeGrace):
			}
		}
		closer.Close()
	}()
	return func() { close(done); <-stopped }
}

func handshake(ctx context.Context, info Info, lines <-chan hostLine, out *output) error {
	var line hostLine
	select {
	case <-ctx.Done():
		return ctx.Err()
	case line = <-lines:
	}
	if line.err != nil && !errors.Is(line.err, jsonline.ErrTooLong) {
		return fmt.Errorf("read hello: %w", line.err)
	}
	frame, _ := p.ParseHostLine(line.data)
	hello, ok := frame.(*p.Hello)
	problem := p.Error{Type: p.TypeError, Code: p.CodeBadRequest, Message: "expected a valid hello"}
	if ok {
		supported := []string{p.V2}
		if version, ok := p.Negotiate(hello.Protocols, supported); ok {
			program := info.Name
			// Channel names may start with a digit or hyphen, but program names require a letter.
			if !p.ValidName(program) {
				program = "channel"
			}
			return out.send(ctx, &p.Ready{
				Type: p.TypeReady, Protocol: version, Kind: p.KindChannel,
				Plugin:  p.Program{Name: program, Version: info.Version},
				Channel: &p.Channel{Name: info.Name, Label: info.Label},
			})
		}
		problem.Code = p.CodeUnsupportedProtocol
		problem.Message = "channel requires v2"
		problem.Protocols = supported
	}
	if err := out.send(ctx, &problem); err != nil {
		return err
	}
	return ErrRefused
}

type result struct {
	id    string
	frame any
}

type inboundPosition struct {
	id       string
	localAck bool
}

type session struct {
	ctx                context.Context
	svc                Service
	log                *Logger
	out                output
	workers            sync.WaitGroup
	sent               chan inboundPosition
	commits            chan []string
	committed          chan int
	outstandingCommits int
	commitQueue        []string
	serviceDone        chan error
	results            chan result
	position           Position
	// A count of waiting requests ensures every repeat gets an answer, even during a send.
	waiting     map[string]int
	answered    map[string]any
	answerOrder []string
}

func Run(ctx context.Context, info Info, svc Service, stdin io.Reader, stdout io.Writer, logger *Logger) error {
	if svc == nil || stdin == nil || stdout == nil {
		return errors.New("run channel: missing service or stream")
	}
	ctx, cancel := context.WithCancel(ctx)
	hostEnded := make(chan struct{})
	s := session{
		svc:         svc,
		log:         logger,
		out:         output{w: stdout, hostEnded: hostEnded},
		sent:        make(chan inboundPosition),
		commits:     make(chan []string),
		committed:   make(chan int),
		serviceDone: make(chan error, 1),
		results:     make(chan result),
		waiting:     make(map[string]int),
		answered:    make(map[string]any),
	}
	s.ctx = context.WithValue(ctx, statusKey{}, reportStatus(s.status))
	defer func() {
		cancel()
		if closer, ok := stdin.(io.Closer); ok {
			closer.Close()
		}
	}()
	if info.DrainGrace <= 0 {
		info.DrainGrace = 5 * time.Second
	}
	lines := readHost(ctx, cancel, stdin, hostEnded, info.DrainGrace)
	if err := handshake(ctx, info, lines, &s.out); err != nil {
		return err
	}
	s.workers.Go(func() { s.serviceDone <- s.receive() })
	err := s.loop(lines)
	cancel()
	s.workers.Wait()
	// A host may already have closed its read end or stopped draining stdout.
	stopCtx, stop := context.WithTimeout(context.Background(), writeGrace)
	defer stop()
	stopErr := s.out.send(stopCtx, &p.Status{Type: p.TypeStatus, State: p.StateStopped})
	return errors.Join(err, stopErr)
}

func (s *session) receive() error {
	for {
		messages, err := s.svc.Receive(s.ctx)
		if err != nil {
			return fmt.Errorf("receive messages: %w", err)
		}
		if len(messages) == 0 {
			return errors.New("receive messages: empty batch")
		}
		received := time.Now()
		// The service commits its original IDs, even when the host sees repaired IDs.
		// Repairs can map several service IDs to one wire ID; each of them is committed with it.
		original := make(map[string][]string, len(messages))
		for _, in := range messages {
			msg, fixes := normalize(in, received)
			for _, fix := range fixes {
				s.log.Printf("inbound %q: %s", msg.ID, fix)
			}
			if ids, seen := original[msg.ID]; seen {
				s.log.Printf("skipping duplicate inbound %q", msg.ID)
				if !slices.Contains(ids, in.ID) {
					original[msg.ID] = append(ids, in.ID)
				}
				continue
			}
			original[msg.ID] = []string{in.ID}
			select {
			case s.sent <- inboundPosition{id: msg.ID}:
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
			msg.Type = p.TypeInbound
			if err := s.out.send(s.ctx, &msg); err != nil {
				if s.drainingWriteError(err) {
					break // Commit earlier acks while the host's output is closed.
				}
				if !errors.Is(err, errInvalidOutput) {
					return err
				}
				s.log.Printf("inbound %q: acknowledging locally after validation failed: %v", msg.ID, err)
				select {
				case s.sent <- inboundPosition{id: msg.ID, localAck: true}:
				case <-s.ctx.Done():
					return s.ctx.Err()
				}
			}
		}
		// Receive reads from the committed position, so finish this batch before polling again.
		for remaining := len(original); remaining > 0; {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case ids := <-s.commits:
				for _, wireID := range ids {
					for _, id := range original[wireID] {
						if err := s.svc.Commit(s.ctx, id); err != nil {
							return fmt.Errorf("commit message %q: %w", id, err)
						}
					}
				}
				remaining -= len(ids)
				select {
				case s.committed <- len(ids):
				case <-s.ctx.Done():
					return s.ctx.Err()
				}
			}
		}
	}
}

// The final status write reports the sticky output error. Until then, let accepted
// work finish even if its response cannot reach the host.
func (s *session) drainingWriteError(err error) bool {
	if !errors.Is(err, errWriteOutput) {
		return false
	}
	select {
	case <-s.out.hostEnded:
		return true
	default:
		return false
	}
}

func (s *session) status(ctx context.Context, state, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(text) > 1024 {
		return errors.New("status.text: exceeds 1024 bytes")
	}
	return s.out.send(s.ctx, &p.Status{Type: p.TypeStatus, State: state, Text: s.log.clean(text)})
}

func (s *session) loop(lines <-chan hostLine) error {
	for {
		if lines == nil && len(s.waiting) == 0 && s.outstandingCommits == 0 {
			return nil
		}
		var commits chan<- []string
		if len(s.commitQueue) > 0 {
			commits = s.commits
		}
		select {
		case <-s.ctx.Done():
			if lines == nil {
				return nil
			}
			return s.ctx.Err()
		case line := <-lines:
			if errors.Is(line.err, io.EOF) {
				lines = nil
				continue
			}
			if line.err != nil && !errors.Is(line.err, jsonline.ErrTooLong) && !errors.Is(line.err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("read host: %w", line.err)
			}
			if err := s.host(line.data); err != nil && !s.drainingWriteError(err) {
				return err
			}
			if errors.Is(line.err, io.ErrUnexpectedEOF) {
				lines = nil
			}
		case err := <-s.serviceDone:
			if s.drainingWriteError(err) {
				continue
			}
			if lines == nil && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case pos := <-s.sent:
			if pos.localAck {
				s.ack(pos.id)
			} else {
				s.position.Sent(pos.id)
			}
		case commits <- s.commitQueue:
			s.commitQueue = nil
		case n := <-s.committed:
			s.outstandingCommits -= n
		case res := <-s.results:
			switch f := res.frame.(type) {
			case *p.Delivered:
				s.remember(res.id, f)
			case *p.DeliveryFailed:
				if f.Error.Code == p.CodeBadRequest {
					s.remember(res.id, f)
				}
			}
			for range s.waiting[res.id] {
				if err := s.out.send(s.ctx, res.frame); err != nil {
					if s.drainingWriteError(err) {
						break
					}
					return err
				}
			}
			delete(s.waiting, res.id)
		}
	}
}

func (s *session) ack(id string) {
	ready := s.position.Ack(id)
	s.commitQueue = append(s.commitQueue, ready...)
	s.outstandingCommits += len(ready)
}

func (s *session) remember(id string, frame any) {
	const limit = 4096
	if len(s.answerOrder) == limit {
		delete(s.answered, s.answerOrder[0])
		s.answerOrder = s.answerOrder[1:]
	}
	s.answerOrder = append(s.answerOrder, id)
	s.answered[id] = frame
}

func (s *session) host(data []byte) error {
	frame, err := p.ParseHostLineV2(data, p.KindChannel)
	if err != nil {
		if id, invalidDestination := rejectedDelivery(data); id != "" {
			frame := failure(id, p.CodeBadRequest, "invalid delivery request")
			if invalidDestination {
				frame.Error.Message = "invalid delivery destination"
				if s.answered[id] == nil && s.waiting[id] == 0 {
					s.remember(id, frame)
				}
			}
			return s.out.send(s.ctx, frame)
		}
		problem := p.Error{Type: p.TypeError, Code: p.CodeBadRequest, Message: "invalid host line"}
		if f, _ := p.ParseHostLine(data); f != nil {
			if call, ok := f.(*p.Call); ok {
				problem.ID = call.ID
				problem.Message = "channels take no calls"
			}
		}
		return s.out.send(s.ctx, &problem)
	}
	switch f := frame.(type) {
	case *p.Ack:
		if already, known := s.position.acked[f.ID]; !known || already {
			s.log.Printf("ignoring unknown or repeated ack %q", f.ID)
			return nil
		}
		s.ack(f.ID)
		return nil
	case *p.Deliver:
		return s.delivery(f)
	default:
		return s.out.send(s.ctx, &p.Error{Type: p.TypeError, Code: p.CodeBadRequest, Message: "unexpected host line"})
	}
}

var deliveryID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// Recover an unambiguous, case-sensitive envelope independently of malformed payload fields.
func rejectedDelivery(data []byte) (string, bool) {
	if !json.Valid(data) {
		return "", false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if first, err := dec.Token(); err != nil || first != json.Delim('{') {
		return "", false
	}
	fields := make(map[string]json.RawMessage)
	duplicate := false
	for dec.More() {
		token, err := dec.Token()
		var value json.RawMessage
		if err != nil || dec.Decode(&value) != nil {
			return "", false
		}
		key := token.(string) // Object keys in valid JSON are strings.
		if _, exists := fields[key]; exists {
			if key == "type" || key == "id" {
				return "", false
			}
			duplicate = true
		}
		fields[key] = value
	}
	var typ, delivery string
	if json.Unmarshal(fields["type"], &typ) != nil || typ != p.TypeDeliver || json.Unmarshal(fields["id"], &delivery) != nil || !deliveryID.MatchString(delivery) {
		return "", false
	}
	if duplicate {
		return delivery, false
	}
	// Remember a failure only when repairing the destination alone makes the request valid.
	fields["chat_id"] = json.RawMessage(`"x"`)
	probe, _ := json.Marshal(fields)
	_, err := p.ParseHostLineV2(probe, p.KindChannel)
	return delivery, err == nil
}

func (s *session) delivery(d *p.Deliver) error {
	id := d.ID
	if frame, ok := s.answered[id]; ok {
		return s.out.send(s.ctx, frame)
	}
	s.waiting[id]++
	if s.waiting[id] > 1 {
		return nil
	}
	s.workers.Go(func() {
		msgID, err := s.svc.Deliver(s.ctx, *d)
		frame := s.outcome(id, msgID, err)
		select {
		case s.results <- result{id, frame}:
		case <-s.ctx.Done():
		}
	})
	return nil
}

func failure(id, code, message string) *p.DeliveryFailed {
	return &p.DeliveryFailed{Type: p.TypeDeliveryFailed, ID: id, Error: p.Problem{Code: code, Message: message}}
}

func (s *session) outcome(id, messageID string, err error) any {
	if err == nil {
		frame := &p.Delivered{V: 1, Seq: 1, Type: p.TypeDelivered, ID: id, MessageID: messageID}
		data, _ := json.Marshal(frame)
		if _, err := p.ParsePluginLineV2(data, p.KindChannel); err == nil {
			return frame
		}
		return failure(id, p.CodeInternal, "service returned an invalid message ID")
	}
	frame := failure(id, p.CodeInternal, "service delivery failed")
	var de *DeliveryError
	if errors.As(err, &de) && de != nil {
		switch de.Code {
		case p.CodeRateLimited, p.CodeUnauthorised, p.CodeBadRequest, p.CodeInternal:
			frame.Error.Code = de.Code
		}
		if message := s.log.clean(de.Message); message != "" && len(message) <= 1024 {
			frame.Error.Message = message
		}
		if de.RetryAfter > 0 {
			ms := de.RetryAfter.Milliseconds()
			frame.RetryAfterMS = &ms
		}
	} else {
		s.log.Printf("delivery %q: %v", id, err)
	}
	return frame
}
