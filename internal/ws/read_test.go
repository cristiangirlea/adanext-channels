package ws

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCancelMidFrameClosesStream(t *testing.T) {
	socket, peer := net.Pipe()
	defer peer.Close()
	defer socket.Close()
	c := &Conn{stream: socket, r: bufio.NewReader(socket), socket: socket,
		writeMu: make(chan struct{}, 1), closing: make(chan struct{})}
	c.SetReadLimit(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.Read(ctx); done <- err }()
	// net.Pipe.Write returns only once Read has consumed the partial frame.
	if _, err := peer.Write([]byte{0x82, 4, 'x'}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	go peer.Write([]byte{0x81, 1, 'y'})
	again, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if data, err := c.Read(again); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("second read returned %q, %v", data, err)
	}
}
