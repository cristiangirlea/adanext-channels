// Package fakecore drives a channel over pipes and keeps its host store across restarts.
package fakecore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/internal/jsonline"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

// Runner starts a fresh service for each session and returns when its context ends.
type Runner func(context.Context, io.Reader, io.Writer) error

// Command calls the factory again after a crash so the channel starts as a new process.
func Command(factory func(context.Context) *exec.Cmd) Runner {
	return func(ctx context.Context, in io.Reader, out io.Writer) error {
		cmd := factory(ctx)
		cmd.Stdin = in
		cmd.Stdout = out
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("channel process: %w", err)
		}
		return nil
	}
}

type Core struct {
	t       testing.TB
	run     Runner
	input   *io.PipeWriter
	output  *bufio.Reader
	cancel  context.CancelFunc
	done    chan struct{}
	seq     int64
	name    string
	pending []p.Inbound
	seen    map[[2]string]bool
	stored  []p.Inbound
}

func Start(t testing.TB, run Runner) *Core {
	t.Helper()
	core := &Core{t: t, run: run, seen: make(map[[2]string]bool)}
	core.start()
	t.Cleanup(core.stop)
	return core
}

func (c *Core) start() {
	ctx, cancel := context.WithCancel(c.t.Context())
	in, input := io.Pipe()
	output, out := io.Pipe()
	c.input = input
	c.output = bufio.NewReader(output)
	c.done = make(chan struct{})
	c.seq = 0
	c.name = ""
	c.pending = nil
	c.cancel = func() { cancel(); input.Close(); output.Close() }
	go func() {
		defer close(c.done)
		err := c.run(ctx, in, out)
		in.CloseWithError(err)
		out.CloseWithError(err)
	}()
}

func (c *Core) stop() {
	c.cancel()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		c.t.Fatal("channel failed to stop")
	}
}
func (c *Core) Restart() { c.t.Helper(); c.stop(); c.start() }

func (c *Core) send(frame any) {
	c.t.Helper()
	timer := time.AfterFunc(5*time.Second, c.cancel)
	defer timer.Stop()
	if err := json.NewEncoder(c.input).Encode(frame); err != nil {
		c.t.Fatalf("send host frame: %v", err)
	}
}

func (c *Core) read() any {
	c.t.Helper()
	timer := time.AfterFunc(5*time.Second, c.cancel)
	defer timer.Stop()
	line, err := jsonline.Read(c.output)
	if err != nil {
		c.t.Fatalf("read channel: %v", err)
	}
	f, err := p.ParsePluginLineV2(line, p.KindChannel)
	if err != nil {
		c.t.Fatalf("parse channel: %v", err)
	}
	var seq int64
	switch frame := f.(type) {
	case *p.Ready:
		seq = frame.Seq
	case *p.Inbound:
		seq = frame.Seq
	case *p.Status:
		seq = frame.Seq
	case *p.Delivered:
		seq = frame.Seq
	case *p.DeliveryFailed:
		seq = frame.Seq
	case *p.Error:
		seq = frame.Seq
	}
	c.seq++
	if seq != c.seq {
		c.t.Fatalf("sequence %d, want %d", seq, c.seq)
	}
	return f
}

func (c *Core) Handshake() *p.Ready {
	c.t.Helper()
	ready, refused := c.Offer(p.V2, p.V1)
	if refused != nil {
		c.t.Fatalf("channel refused handshake: %+v", refused)
	}
	return ready
}

// Offer allows tests to inspect a refusal when the host and channel have no common version.
func (c *Core) Offer(protocols ...string) (*p.Ready, *p.Error) {
	c.t.Helper()
	c.send(p.Hello{Type: p.TypeHello, Protocols: protocols, Host: p.Program{Name: "ada", Version: "1"}})
	frame := c.read()
	if refused, ok := frame.(*p.Error); ok {
		c.input.Close()
		return nil, refused
	}
	ready, ok := frame.(*p.Ready)
	if !ok || ready.Kind != p.KindChannel || ready.Protocol != p.V2 || !slices.Contains(protocols, ready.Protocol) {
		c.t.Fatalf("expected v2 channel ready, got %+v", frame)
	}
	c.name = ready.Channel.Name
	return ready, nil
}

func (c *Core) store(msg *p.Inbound) {
	key := [2]string{c.name, msg.ID}
	if !c.seen[key] {
		c.seen[key] = true
		c.stored = append(c.stored, *msg)
	}
}

func (c *Core) NextInbound() p.Inbound {
	c.t.Helper()
	if len(c.pending) > 0 {
		msg := c.pending[0]
		c.pending = c.pending[1:]
		return msg
	}
	for {
		frame := c.read()
		switch f := frame.(type) {
		case *p.Inbound:
			c.store(f)
			return *f
		case *p.Status:
			continue
		default:
			c.t.Fatalf("expected inbound, got %+v", frame)
		}
	}
}

func (c *Core) Ack(id string) { c.t.Helper(); c.send(p.Ack{Type: p.TypeAck, ID: id}) }

func (c *Core) Deliver(d p.Deliver) (*p.Delivered, *p.DeliveryFailed) {
	c.t.Helper()
	d.Type = p.TypeDeliver
	c.send(d)
	for {
		frame := c.read()
		switch f := frame.(type) {
		case *p.Inbound:
			c.store(f)
			c.pending = append(c.pending, *f)
			continue
		case *p.Status:
			continue
		case *p.Delivered:
			if f.ID == d.ID {
				return f, nil
			}
		case *p.DeliveryFailed:
			if f.ID == d.ID {
				return nil, f
			}
		}
		c.t.Fatalf("expected delivery outcome for %q, got %+v", d.ID, frame)
	}
}

func (c *Core) Stored() []p.Inbound { return slices.Clone(c.stored) }
