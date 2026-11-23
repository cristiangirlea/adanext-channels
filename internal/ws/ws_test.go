package ws_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/internal/ws"
	"github.com/cristiangirlea/adanext-channels/internal/ws/wstest"
)

func server(t *testing.T, f func(*wstest.Peer)) string {
	t.Helper()
	done := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		p, err := wstest.Upgrade(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		defer p.Close()
		f(p)
	}))
	t.Cleanup(func() {
		s.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("peer did not stop")
		}
	})
	return "ws" + strings.TrimPrefix(s.URL, "http")
}

func TestMessagesAndControlFrames(t *testing.T) {
	for _, op := range []byte{1, 2} {
		data := bytes.Repeat([]byte("x"), 70000)
		addr := server(t, func(p *wstest.Peer) {
			code, msg, err := p.Frame()
			if code != 0x81 || err != nil || !bytes.Equal(msg, data) {
				t.Error("incorrect client message")
				return
			}
			p.Send(op, data)
			p.Send(0x89, []byte("ping"))
			p.Send(0x80, []byte("end"))
			code, msg, err = p.Frame()
			if code != 0x8a || string(msg) != "ping" || err != nil {
				t.Error("missing pong")
			}
			p.Send(0x88, []byte{15, 164})
			code, msg, err = p.Frame()
			if code != 0x88 || !bytes.Equal(msg, []byte{15, 164}) || err != nil {
				t.Error("missing close reply")
			}
		})
		c, err := ws.Dial(t.Context(), addr, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Write(t.Context(), data); err != nil {
			t.Fatal(err)
		}
		msg, err := c.Read(t.Context())
		if err != nil || !bytes.Equal(msg, append(data, []byte("end")...)) {
			t.Fatal("fragmented message", err)
		}
		_, err = c.Read(t.Context())
		var closed *ws.CloseError
		if !errors.As(err, &closed) || closed.Code != 4004 {
			t.Fatal(err)
		}
	}
}

func TestInvalidServerFrames(t *testing.T) {
	for _, frame := range [][]byte{{0x81, 0x80}, {0x80, 0}, {0xc1, 0}, {0x09, 0}, {0x88, 1, 0}, {0x81, 126, 0, 2}, {0x82, 127, 0, 0, 0, 0, 0, 0x40, 0, 1}} {
		addr := server(t, func(p *wstest.Peer) { p.Write(frame); p.Frame() })
		c, err := ws.Dial(t.Context(), addr, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err = c.Read(ctx)
		cancel()
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("accepted malformed frame", frame)
		}
		c.Close(1000, "")
	}
}

func TestBadUpgrade(t *testing.T) {
	for _, status := range []int{200, 101, 302} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Connection", "Upgrade")
			w.Header().Set("Upgrade", "websocket")
			w.Header().Set("Sec-WebSocket-Accept", "wrong")
			w.WriteHeader(status)
		}))
		_, err := ws.Dial(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
		srv.Close()
		if err == nil {
			t.Fatal(status)
		}
	}
}

func TestReadLimitIncludesAllFragments(t *testing.T) {
	addr := server(t, func(p *wstest.Peer) {
		p.Send(1, []byte("abc"))
		p.Send(0x80, []byte("def"))
		op, payload, err := p.Frame()
		if op != 0x88 || !bytes.Equal(payload, []byte{3, 241}) || err != nil {
			t.Error("expected close code 1009")
		}
	})
	c, err := ws.Dial(t.Context(), addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.SetReadLimit(5)
	if _, err := c.Read(t.Context()); err == nil {
		t.Fatal("accepted oversized fragmented message")
	}
}

func TestReadTimeoutClosesStream(t *testing.T) {
	addr := server(t, func(p *wstest.Peer) {
		if _, _, err := p.Frame(); err == nil {
			t.Error("expected the stream to close")
		}
	})
	c, err := ws.Dial(t.Context(), addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	var timeout net.Error
	if _, err := c.Read(ctx); !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("expected read timeout, got %v", err)
	}
	if _, err := c.Read(t.Context()); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}

func TestLocalClose(t *testing.T) {
	replied := make(chan struct{})
	addr := server(t, func(p *wstest.Peer) {
		code, body, err := p.Frame()
		if err != nil || code != 0x88 || string(body) != "\x03\xe8done" {
			t.Error("wrong close frame", err)
		}
		time.Sleep(20 * time.Millisecond)
		close(replied)
		p.Send(0x88, body)
	})
	c, err := ws.Dial(t.Context(), addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Close(1000, "done")
	select {
	case <-replied:
	default:
		t.Fatal("Close returned before the peer could answer")
	}
}
