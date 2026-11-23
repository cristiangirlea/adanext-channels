package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	"github.com/cristiangirlea/adanext-channels/sdk/fakecore"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func TestEchoProcess(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-2] != "--" {
		return
	}
	os.Exit(serve(context.Background(), os.Args[len(os.Args)-1], os.Stdin, os.Stdout, os.Stderr))
}

func command(ctx context.Context, dir string) *exec.Cmd {
	return exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEchoProcess$", "--", dir)
}
func start(t *testing.T, dir string) *fakecore.Core {
	t.Helper()
	core := fakecore.Start(t, fakecore.Command(func(ctx context.Context) *exec.Cmd { return command(ctx, dir) }))
	ready := core.Handshake()
	if ready.Channel.Name != "echo" {
		t.Fatal(ready)
	}
	return core
}

func TestEchoReplay(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "messages.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"one", "two", "three"}
	var ends []int64
	for _, id := range ids {
		err = json.NewEncoder(file).Encode(sdk.Inbound{ID: id, Text: id, Time: "2026-01-01T00:00:00Z", Chat: p.Chat{ID: "room"}, Sender: p.Sender{ID: "reader"}})
		if err != nil {
			t.Fatal(err)
		}
		stat, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		ends = append(ends, stat.Size())
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	core := start(t, dir)
	for _, id := range ids {
		if msg := core.NextInbound(); msg.ID != id {
			t.Fatal(msg)
		}
	}
	core.Ack("three")
	core.Ack("one")
	waitForPosition(t, dir, ends[0])
	core.Restart()
	core.Handshake()
	for _, id := range ids[1:] {
		if msg := core.NextInbound(); msg.ID != id {
			t.Fatal(msg)
		}
		core.Ack(id)
	}
	waitForPosition(t, dir, ends[2])
	var stored []string
	for _, msg := range core.Stored() {
		stored = append(stored, msg.ID)
		if msg.Text != msg.ID {
			t.Fatal(msg)
		}
	}
	if !slices.Equal(stored, ids) {
		t.Fatal(stored)
	}
	core.Restart()
	core.Handshake()
	f, err := os.OpenFile(filepath.Join(dir, "messages.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewEncoder(f).Encode(sdk.Inbound{ID: "four", Text: "new", Time: "2026-01-01T00:00:00Z", Chat: p.Chat{ID: "room"}, Sender: p.Sender{ID: "reader"}})
	if err := errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	if msg := core.NextInbound(); msg.ID != "four" {
		t.Fatal(msg)
	}
}

func waitForPosition(t *testing.T, dir string, want int64) {
	t.Helper()
	d := directory{dir: dir}
	deadline := time.Now().Add(5 * time.Second)
	for {
		pos, err := d.position()
		if err != nil {
			t.Fatal(err)
		}
		if pos == want {
			return
		}
		if pos > want || time.Now().After(deadline) {
			t.Fatalf("position %d, want %d", pos, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestEchoDeliver(t *testing.T) {
	dir := t.TempDir()
	core := start(t, dir)
	d := p.Deliver{ID: "d", ChatID: "room", Text: "reply"}
	first, failed := core.Deliver(d)
	if failed != nil || first.ID != d.ID || first.MessageID == "" {
		t.Fatal(first, failed)
	}
	again, failed := core.Deliver(d)
	if failed != nil || again.MessageID != first.MessageID {
		t.Fatal(again, failed)
	}
	d.ID = "bad"
	d.ChatID = ""
	_, failed = core.Deliver(d)
	if failed == nil || failed.ID != "bad" || failed.Error.Code != p.CodeBadRequest {
		t.Fatal(failed)
	}
	d.ChatID = "room"
	_, againFailed := core.Deliver(d)
	if againFailed == nil || againFailed.Error != failed.Error {
		t.Fatal(againFailed)
	}
	data, err := os.ReadFile(filepath.Join(dir, "deliveries.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var record p.Deliver
	if err = json.Unmarshal(data, &record); err != nil || record.ID != "d" || record.Text != "reply" {
		t.Fatal(record, err)
	}
}

func TestEchoRefusesV1(t *testing.T) {
	dir := t.TempDir()
	exited := make(chan error, 1)
	run := fakecore.Command(func(ctx context.Context) *exec.Cmd { return command(ctx, dir) })
	core := fakecore.Start(t, func(ctx context.Context, in io.Reader, out io.Writer) error {
		err := run(ctx, in, out)
		exited <- err
		return err
	})
	ready, refusal := core.Offer(p.V1)
	if ready != nil || refusal == nil || refusal.Code != p.CodeUnsupportedProtocol || !slices.Equal(refusal.Protocols, []string{p.V2}) {
		t.Fatal(ready, refusal)
	}
	var exit *exec.ExitError
	select {
	case err := <-exited:
		if !errors.As(err, &exit) || exit.ExitCode() != p.ExitRefused {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refused channel did not exit")
	}
}

func TestEchoManifest(t *testing.T) {
	m, err := sdk.LoadManifest("channel.json")
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "echo" || m.SecretEnv != "ADA_ECHO_TOKEN" || !slices.Equal(m.Hosts, []string{"localhost"}) {
		t.Fatal(m)
	}
}
