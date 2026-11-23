package sdk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cristiangirlea/adanext-channels/internal/jsonline"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type testHost struct {
	t    *testing.T
	in   *io.PipeWriter
	out  *bufio.Reader
	done chan struct{}
	log  bytes.Buffer
}

func startHost(t *testing.T, svc Service, redact ...string) *testHost {
	t.Helper()
	in, hostIn := io.Pipe()
	hostOut, out := io.Pipe()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	h := &testHost{t: t, in: hostIn, out: bufio.NewReader(hostOut), done: make(chan struct{})}
	go func() {
		defer close(h.done)
		err := Run(ctx, info, svc, in, out, &Logger{Writer: &h.log, Redact: redact})
		out.CloseWithError(err)
	}()
	t.Cleanup(func() { cancel(); hostIn.Close(); hostOut.Close(); <-h.done })
	h.send(json.RawMessage(hello))
	if _, ok := h.next().(*p.Ready); !ok {
		t.Fatal("expected ready")
	}
	return h
}

func (h *testHost) send(f any) {
	h.t.Helper()
	if err := json.NewEncoder(h.in).Encode(f); err != nil {
		h.t.Fatal(err)
	}
}

func (h *testHost) next() any {
	h.t.Helper()
	line, err := jsonline.Read(h.out)
	if err != nil {
		h.t.Fatal(err)
	}
	frame, err := p.ParsePluginLineV2(line, p.KindChannel)
	if err != nil {
		h.t.Fatal(err)
	}
	return frame
}

func (h *testHost) finish() {
	h.t.Helper()
	h.in.Close()
	if s, ok := h.next().(*p.Status); !ok || s.State != p.StateStopped {
		h.t.Fatal("expected stopped", s)
	}
	if _, err := h.out.ReadByte(); err != io.EOF {
		h.t.Fatal("expected clean shutdown", err)
	}
}

type batchService struct {
	idleService
	batches [][]Inbound
	commits []string
}

func (s *batchService) Receive(ctx context.Context) ([]Inbound, error) {
	if len(s.batches) == 0 {
		return s.idleService.Receive(ctx)
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	return b, nil
}
func (s *batchService) Commit(_ context.Context, id string) error {
	s.commits = append(s.commits, id)
	return nil
}

func inbound(id string) Inbound {
	return Inbound{ID: id, Time: "2026-01-01T00:00:00Z", Sender: p.Sender{ID: "s"}, Chat: p.Chat{ID: "c"}, Text: "hello"}
}

func TestInboundRepairsAndOrderedCommit(t *testing.T) {
	long := strings.Repeat("i", 129)
	first := inbound(long)
	first.Text = ""
	first.Time = ""
	first.Sender.Name = "line\nbreak"
	second := inbound("second")
	second.Time = "2025-01-01T01:01:01+24:00"
	second.Sender.Name = strings.Repeat("x", 255) + "é"
	second.Text = strings.Repeat("界", p.MaxEventText/3+1)
	for range 18 {
		second.Attachments = append(second.Attachments, p.Attachment{Name: "photo", MIME: "image/jpeg", Ref: "image"})
	}
	invalid := inbound("invalid")
	invalid.Sender.ID = ""
	svc := &batchService{batches: [][]Inbound{{first, invalid, second, first}, {inbound("next")}}}
	start := time.Now()
	h := startHost(t, svc)
	a, ok := h.next().(*p.Inbound)
	if !ok || a.ID != fmt.Sprintf("%x", sha256.Sum256([]byte(long))) || a.Text != "[message with no text]" || a.Sender.Name != "line\ufffdbreak" {
		t.Fatal("unexpected first message", a)
	}
	b, ok := h.next().(*p.Inbound)
	if !ok || b.ID != "second" || b.Sender.Name != strings.Repeat("x", 255) || !strings.Contains(b.Text, "[message cut]") || !strings.Contains(b.Text, "[2 more attachments not shown]") || !utf8.ValidString(b.Text) || len(b.Attachments) != 16 {
		t.Fatal("unexpected second message")
	}
	for _, stamp := range []string{a.Time, b.Time} {
		got, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || got.Before(start) || got.After(time.Now()) {
			t.Fatal("receive time", stamp, err)
		}
	}
	h.send(p.Ack{Type: p.TypeAck, ID: b.ID})
	h.send(p.Ack{Type: p.TypeAck, ID: a.ID})
	next, ok := h.next().(*p.Inbound)
	if !ok || next.ID != "next" {
		t.Fatal("expected next batch", next)
	}
	h.send(p.Ack{Type: p.TypeAck, ID: next.ID})
	h.finish()
	if !slices.Equal(svc.commits, []string{long, "invalid", "second", "next"}) {
		t.Fatal(svc.commits)
	}
	for _, note := range []string{"invalid", "duplicate", "sender.name", "attachments", "time", "text", "hash"} {
		if !strings.Contains(h.log.String(), note) {
			t.Errorf("missing %q in %s", note, &h.log)
		}
	}
}

type deliveryFunc func(context.Context, p.Deliver) (string, error)
type deliveryService struct {
	idleService
	deliver deliveryFunc
}

func (s deliveryService) Deliver(ctx context.Context, d p.Deliver) (string, error) {
	return s.deliver(ctx, d)
}
func request(id string) p.Deliver {
	return p.Deliver{Type: p.TypeDeliver, ID: id, ChatID: "c", Text: "hello"}
}

func TestTransientDeliveryRetried(t *testing.T) {
	for _, code := range []string{p.CodeRateLimited, p.CodeUnauthorised, p.CodeInternal} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int32
			release := make(chan struct{})
			h := startHost(t, deliveryService{deliver: func(ctx context.Context, d p.Deliver) (string, error) {
				if calls.Add(1) == 1 {
					select {
					case <-release:
					case <-ctx.Done():
						return "", ctx.Err()
					}
					return "", &DeliveryError{Code: code}
				}
				return "sent", nil
			}})
			for range 2 {
				h.send(request("d"))
			}
			h.send(p.Call{Type: p.TypeCall, ID: "barrier", Capability: "echo"})
			if f, ok := h.next().(*p.Error); !ok || f.ID != "barrier" {
				t.Fatal(f)
			}
			close(release)
			for range 2 {
				f, ok := h.next().(*p.DeliveryFailed)
				if !ok || f.ID != "d" || f.Error.Code != code {
					t.Fatal(f)
				}
			}
			h.send(request("d"))
			if d, ok := h.next().(*p.Delivered); !ok || d.ID != "d" || calls.Load() != 2 {
				t.Fatal(d, calls.Load())
			}
		})
	}
}

func TestDeliveryEviction(t *testing.T) {
	var calls atomic.Int32
	h := startHost(t, deliveryService{deliver: func(_ context.Context, d p.Deliver) (string, error) { return fmt.Sprint(calls.Add(1)), nil }})
	send := func(id int, want int) {
		t.Helper()
		h.send(request(fmt.Sprint(id)))
		if d, ok := h.next().(*p.Delivered); !ok || d.MessageID != fmt.Sprint(want) {
			t.Fatalf("id %d: %v, want call %d", id, d, want)
		}
	}
	for i := 1; i <= 4096; i++ {
		send(i, i)
	}
	send(1, 1)
	send(4097, 4097)
	send(2, 2)
	send(1, 4098)
}

func TestMalformedDeliveryHasOutcome(t *testing.T) {
	h := startHost(t, deliveryService{deliver: func(context.Context, p.Deliver) (string, error) { return "sent", nil }})
	for _, line := range []string{
		`{"type":"deliver","id":"d","chat_id":"c"}`,
		`{"type":"deliver","id":"d","chat_id":"c","text":false}`,
		`{"type":"deliver","id":"d","chat_id":"c","text":"x","buttons":[{"id":"!"}]}`,
		`{"type":"deliver","id":"d","chat_id":"c","chat_id":"","text":"x"}`,
	} {
		h.send(json.RawMessage(line))
		if f, ok := h.next().(*p.DeliveryFailed); !ok || f.ID != "d" || f.Error.Code != p.CodeBadRequest {
			t.Fatal(f)
		}
	}
	h.send(request("d"))
	if f, ok := h.next().(*p.Delivered); !ok || f.ID != "d" {
		t.Fatal(f)
	}
	h.send(json.RawMessage(`{"type":"deliver","id":"d","chat_id":"","text":"x"}`))
	if f, ok := h.next().(*p.DeliveryFailed); !ok || f.ID != "d" || f.Error.Code != p.CodeBadRequest {
		t.Fatal("malformed repeat needs its own failure", f)
	}
	for _, line := range []string{
		`{"type":"deliver","id":"!","chat_id":"c"}`,
		`{"type":"deliver","id":"a","id":"b","chat_id":"c"}`,
		`{"type":"deliver","ID":"a","chat_id":"c"}`,
	} {
		h.send(json.RawMessage(line))
		if _, ok := h.next().(*p.Error); !ok {
			t.Fatal("expected uncorrelated error")
		}
	}
}

func TestDistinctConcurrentDeliveries(t *testing.T) {
	first := make(chan struct{})
	started := make(chan struct{})
	h := startHost(t, deliveryService{deliver: func(ctx context.Context, d p.Deliver) (string, error) {
		if d.ID == "first" {
			close(started)
			select {
			case <-first:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return d.ID, nil
	}})
	h.send(request("first"))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first delivery did not start")
	}
	h.send(request("second"))
	if d, ok := h.next().(*p.Delivered); !ok || d.ID != "second" {
		t.Fatal(d)
	}
	close(first)
	if d, ok := h.next().(*p.Delivered); !ok || d.ID != "first" {
		t.Fatal(d)
	}
}

func TestInvalidStatusReturnedToCaller(t *testing.T) {
	reported := make(chan error, 2)
	h := startHost(t, deliveryService{deliver: func(ctx context.Context, _ p.Deliver) (string, error) {
		reported <- Status(ctx, "unknown", "ok")
		reported <- Status(ctx, p.StateConnected, strings.Repeat("a", 1025))
		return "sent", nil
	}}, strings.Repeat("a", 32))
	h.send(request("d"))
	if _, ok := h.next().(*p.Delivered); !ok {
		t.Fatal("invalid status reached host")
	}
	for range 2 {
		if <-reported == nil {
			t.Error("caller got no error")
		}
	}
}

type drainingCommit struct {
	batchService
	release chan struct{}
}

func (s *drainingCommit) Commit(ctx context.Context, id string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return s.batchService.Commit(ctx, id)
	}
}
func TestEOFDrainsAckedCommit(t *testing.T) {
	s := &drainingCommit{batchService: batchService{batches: [][]Inbound{{inbound("a")}}}, release: make(chan struct{})}
	h := startHost(t, s)
	if _, ok := h.next().(*p.Inbound); !ok {
		t.Fatal("expected inbound")
	}
	h.send(p.Ack{Type: p.TypeAck, ID: "a"})
	h.in.Close()
	time.AfterFunc(400*time.Millisecond, func() { close(s.release) })
	h.finish()
	if !slices.Equal(s.commits, []string{"a"}) {
		t.Fatal("commit after EOF", s.commits)
	}
}

func TestEOFBlockedInboundAllowsAckedCommit(t *testing.T) {
	s := &drainingCommit{batchService: batchService{batches: [][]Inbound{{inbound("a"), inbound("b")}}}, release: make(chan struct{})}
	h := startHost(t, s)
	if f, ok := h.next().(*p.Inbound); !ok || f.ID != "a" {
		t.Fatal(f)
	}
	h.send(p.Ack{Type: p.TypeAck, ID: "a"})
	h.in.Close()
	time.Sleep(400 * time.Millisecond) // Leave the second inbound write blocked past its grace.
	close(s.release)
	if _, err := h.out.ReadByte(); err == nil {
		t.Fatal("blocked write was still open")
	}
	<-h.done
	if !slices.Equal(s.commits, []string{"a"}) {
		t.Fatal("lost commit after blocked write", s.commits)
	}
}

func TestRepairedIDsSharingAWireIDAreAllCommitted(t *testing.T) {
	svc := &batchService{batches: [][]Inbound{{inbound("a\x01"), inbound("a\x02")}, {inbound("next")}}}
	h := startHost(t, svc)
	got, ok := h.next().(*p.Inbound)
	if !ok || got.ID != "a\ufffd" {
		t.Fatal("unexpected message", got)
	}
	h.send(p.Ack{Type: p.TypeAck, ID: got.ID})
	next, ok := h.next().(*p.Inbound)
	if !ok || next.ID != "next" {
		t.Fatal("expected next batch", next)
	}
	h.send(p.Ack{Type: p.TypeAck, ID: next.ID})
	h.finish()
	if !slices.Equal(svc.commits, []string{"a\x01", "a\x02", "next"}) {
		t.Fatal(svc.commits)
	}
}
