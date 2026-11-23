package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type slowService struct {
	idleService
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *slowService) Receive(ctx context.Context) ([]Inbound, error) {
	return []Inbound{{ID: "a", Time: "2026-01-01T00:00:00Z", Sender: p.Sender{ID: "u"}, Chat: p.Chat{ID: "c"}, Text: "a"}}, nil
}
func (s *slowService) Commit(ctx context.Context, id string) error {
	close(s.entered)
	<-ctx.Done()
	return ctx.Err()
}
func (s *slowService) Deliver(ctx context.Context, d p.Deliver) (string, error) {
	s.calls.Add(1)
	select {
	case <-s.release:
		return "sent", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestEOFInterruptsCommit(t *testing.T) {
	svc := &slowService{entered: make(chan struct{})}
	settings := info
	settings.DrainGrace = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r, w := io.Pipe()
	defer w.Close()
	out, writer := io.Pipe()
	defer out.Close()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, settings, svc, r, writer, nil); writer.Close() }()
	io.WriteString(w, hello+"\n")
	dec := json.NewDecoder(out)
	var ready p.Ready
	var msg Inbound
	if err := dec.Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&msg); err != nil {
		t.Fatal(err)
	}
	json.NewEncoder(w).Encode(p.Ack{Type: p.TypeAck, ID: msg.ID})
	select {
	case <-svc.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	w.Close()
	var stopped p.Status
	if err := dec.Decode(&stopped); err != nil || stopped.State != p.StateStopped {
		t.Fatal(stopped, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("stdin closed while committing: %v", err)
	}
}

func TestConcurrentDuplicateDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	r, w := io.Pipe()
	defer w.Close()
	out, writer := io.Pipe()
	defer out.Close()
	svc := &slowService{release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, info, svc, r, writer, nil); writer.Close() }()
	io.WriteString(w, hello+"\n")
	dec := json.NewDecoder(out)
	var ready p.Ready
	var inbound Inbound
	if err := dec.Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&inbound); err != nil {
		t.Fatal(err)
	}
	d := p.Deliver{Type: p.TypeDeliver, ID: "d", ChatID: "c", Text: "x"}
	enc := json.NewEncoder(w)
	for range 2 {
		if err := enc.Encode(d); err != nil {
			t.Fatal(err)
		}
	}
	enc.Encode(p.Call{Type: p.TypeCall, ID: "barrier", Capability: "echo"})
	var barrier p.Error
	if err := dec.Decode(&barrier); err != nil || barrier.ID != "barrier" {
		t.Fatal(barrier, err)
	}
	close(svc.release)
	for range 2 {
		var delivered p.Delivered
		if err := dec.Decode(&delivered); err != nil || delivered.MessageID != "sent" {
			t.Fatal(delivered, err)
		}
	}
	if svc.calls.Load() != 1 {
		t.Fatal(svc.calls.Load())
	}
	w.Close()
	io.Copy(io.Discard, out)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDeliveryErrorCodes(t *testing.T) {
	s := session{log: &Logger{Writer: io.Discard, Redact: []string{"sample"}}}
	for _, code := range []string{p.CodeRateLimited, p.CodeBadRequest, p.CodeUnauthorised, p.CodeInternal} {
		got := s.outcome("d", "", &DeliveryError{Code: code, Message: "sample\nfailed"}).(*p.DeliveryFailed)
		if got.Error.Code != code || got.Error.Message != "[redacted]\\nfailed" {
			t.Fatal(got)
		}
	}
	for _, err := range []error{nil, errors.New("offline"), &DeliveryError{Code: "other"}} {
		got := s.outcome("d", "", err).(*p.DeliveryFailed)
		if got.Error.Code != p.CodeInternal {
			t.Fatal(got)
		}
	}
}
func TestMalformedHello(t *testing.T) {
	var out bytes.Buffer
	err := Run(t.Context(), info, idleService{}, bytes.NewBufferString("{}\n"), &out, nil)
	f, parseErr := p.ParsePluginLine(out.Bytes())
	if !errors.Is(err, ErrRefused) || parseErr != nil || f.(*p.Error).Code != p.CodeBadRequest {
		t.Fatal(f, err, parseErr)
	}
}

func TestEOFWithUndrainedStdout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	out, writer := io.Pipe()
	defer out.Close()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, info, idleService{}, bytes.NewBufferString(hello+"\n"), writer, nil) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stdin ended but ready is still blocked on stdout")
	}
}

func TestEOFDrainsAcceptedDelivery(t *testing.T) {
	svc := &slowService{release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	in, host := io.Pipe()
	defer host.Close()
	out, writer := io.Pipe()
	defer out.Close()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, info, svc, in, writer, nil); writer.Close() }()
	io.WriteString(host, hello+"\n")
	dec := json.NewDecoder(out)
	var ready p.Ready
	var inbound p.Inbound
	if err := dec.Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&inbound); err != nil {
		t.Fatal(err)
	}
	io.WriteString(host, `{"type":"deliver","id":"d","chat_id":"c","text":"x"}`+"\n")
	host.Close()
	time.AfterFunc(400*time.Millisecond, func() { close(svc.release) })
	var delivery p.Delivered
	if err := dec.Decode(&delivery); err != nil || delivery.Type != p.TypeDelivered {
		t.Fatal(delivery, err)
	}
	io.Copy(io.Discard, out)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
