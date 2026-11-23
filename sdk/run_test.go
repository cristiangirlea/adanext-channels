package sdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type idleService struct{}

func (idleService) Receive(ctx context.Context) ([]Inbound, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (idleService) Commit(context.Context, string) error { return errors.New("unexpected commit") }
func (idleService) Deliver(context.Context, p.Deliver) (string, error) {
	return "", errors.New("unexpected delivery")
}

const hello = `{"type":"hello","protocols":["v2"],"host":{"name":"ada","version":"1"}}`

var info = Info{Name: "echo", Label: "Echo", Version: "1"}

func TestInvalidHostLines(t *testing.T) {
	lines := []string{hello, `{"type":"call","id":"c","capability":"foo"}`, `{"type":"unknown"}`, "{", `{"type":"ack","id":"a","id":"b"}`, strings.Repeat("x", p.MaxLine+1), `{"type":"ack","id":"missing"}`}
	var out, logs bytes.Buffer
	err := Run(t.Context(), info, idleService{}, strings.NewReader(strings.Join(lines, "\n")+"\n"), &out, &Logger{Writer: &logs})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "missing") {
		t.Fatal(logs.String())
	}
	scanner := bufio.NewScanner(&out)
	var n int64
	for scanner.Scan() {
		n++
		f, err := p.ParsePluginLineV2(scanner.Bytes(), p.KindChannel)
		if err != nil {
			t.Fatal(err)
		}
		switch v := f.(type) {
		case *p.Ready:
			if n != 1 || v.Seq != n || v.Kind != p.KindChannel || v.Protocol != p.V2 {
				t.Fatal(v)
			}
		case *p.Error:
			if v.Code != p.CodeBadRequest || v.Seq != n || n == 2 && v.ID != "c" {
				t.Fatal(v)
			}
		case *p.Status:
			if v.State != p.StateStopped || v.Seq != 7 {
				t.Fatal(v)
			}
		default:
			t.Fatal(f)
		}
	}
	if n != 7 {
		t.Fatal(n)
	}
}

func TestRefusal(t *testing.T) {
	var out bytes.Buffer
	err := Run(t.Context(), info, idleService{}, strings.NewReader(strings.Replace(hello, "v2", "v1", 1)+"\n"), &out, nil)
	if !errors.Is(err, ErrRefused) {
		t.Fatal(err)
	}
	f, err := p.ParsePluginLine(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	v := f.(*p.Error)
	if v.Seq != 1 || v.Code != p.CodeUnsupportedProtocol || len(v.Protocols) != 1 || v.Protocols[0] != p.V2 {
		t.Fatal(v)
	}
}

type statusService struct{ idleService }

func TestNumericChannelName(t *testing.T) {
	var out bytes.Buffer
	err := Run(t.Context(), Info{Name: "7chat", Version: "1"}, idleService{}, strings.NewReader(hello+"\n"), &out, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ready p.Ready
	if err := json.NewDecoder(&out).Decode(&ready); err != nil || ready.Channel.Name != "7chat" || !p.ValidName(ready.Plugin.Name) {
		t.Fatal(ready, err)
	}
}

func (statusService) Receive(ctx context.Context) ([]Inbound, error) {
	if err := Status(ctx, p.StateReconnecting, "offline"); err != nil {
		return nil, err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (statusService) Deliver(ctx context.Context, d p.Deliver) (string, error) {
	return "", &DeliveryError{Code: p.CodeRateLimited, Message: "retry", RetryAfter: time.Second}
}
func TestServiceStatusAndFailure(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	out, writer := io.Pipe()
	defer out.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, info, statusService{}, r, writer, nil); writer.Close() }()
	io.WriteString(w, hello+"\n")
	dec := json.NewDecoder(out)
	var ready p.Ready
	var status p.Status
	if err := dec.Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&status); err != nil || status.State != p.StateReconnecting {
		t.Fatal(status, err)
	}
	io.WriteString(w, `{"type":"deliver","id":"d","chat_id":"c","text":"reply"}`+"\n")
	var failed p.DeliveryFailed
	if err := dec.Decode(&failed); err != nil || failed.ID != "d" || failed.Error.Code != p.CodeRateLimited || failed.RetryAfterMS == nil || *failed.RetryAfterMS != 1000 {
		t.Fatal(failed, err)
	}
	w.Close()
	if err := dec.Decode(&status); err != nil || status.State != p.StateStopped {
		t.Fatal(status, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestCancelBeforeHello(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	r, w := io.Pipe()
	defer w.Close()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, info, idleService{}, r, io.Discard, nil) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting on stdin")
	}
}

type shortWriter struct{ writes int }

func (w *shortWriter) Write(data []byte) (int, error) { w.writes++; return len(data) / 2, nil }

func TestPartialWriteEndsOutput(t *testing.T) {
	w := new(shortWriter)
	out := output{w: w}
	for range 2 {
		err := out.send(t.Context(), &p.Status{Type: p.TypeStatus, State: p.StateStopped})
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatal(err)
		}
	}
	if w.writes != 1 {
		t.Fatalf("appended another frame after a partial write: %d writes", w.writes)
	}
}
