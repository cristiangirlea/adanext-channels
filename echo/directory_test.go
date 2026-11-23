package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPositionCountsCRLFAndIgnoresIncompleteTail(t *testing.T) {
	d := directory{dir: t.TempDir()}
	path := filepath.Join(d.dir, "messages.jsonl")
	first := `{"id":"one"}` + "\r\n"
	if err := os.WriteFile(path, []byte(first+`{"id":"two"`), 0600); err != nil {
		t.Fatal(err)
	}
	msgs, err := d.Receive(t.Context())
	if err != nil || len(msgs) != 1 || msgs[0].ID != "one" {
		t.Fatal(msgs, err)
	}
	if err = d.Commit(t.Context(), "one"); err != nil {
		t.Fatal(err)
	}
	offset, err := d.position()
	if err != nil || offset != int64(len(first)) {
		t.Fatal(offset, err)
	}
	if err = d.Commit(t.Context(), "missing"); err == nil {
		t.Fatal("committed an unknown ID")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("}\n")
	if err = errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	msgs, err = d.Receive(t.Context())
	if err != nil || len(msgs) != 1 || msgs[0].ID != "two" {
		t.Fatal(msgs, err)
	}
}

func TestInvalidPositionStopsReceive(t *testing.T) {
	d := directory{dir: t.TempDir()}
	if err := os.WriteFile(filepath.Join(d.dir, "messages.jsonl"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, pos := range []string{"-1", "broken", "999"} {
		if err := os.WriteFile(filepath.Join(d.dir, "position"), []byte(pos), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := d.Receive(t.Context()); err == nil {
			t.Fatal("accepted position", pos)
		}
	}
}

func TestEmptyDirectoryStopsOnCancel(t *testing.T) {
	d := directory{dir: t.TempDir()}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := d.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDuplicateInBatchCommitsInOrder(t *testing.T) {
	d := directory{dir: t.TempDir()}
	a, b := `{"id":"a"}`+"\n", `{"id":"b"}`+"\n"
	records := a + b + a
	if err := os.WriteFile(filepath.Join(d.dir, "messages.jsonl"), []byte(records), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Receive(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		id     string
		offset int
	}{{"a", len(a)}, {"b", len(records)}} {
		if err := d.Commit(t.Context(), step.id); err != nil {
			t.Fatal(err)
		}
		if pos, err := d.position(); err != nil || pos != int64(step.offset) {
			t.Fatalf("commit %s: %d, %v; want %d", step.id, pos, err, step.offset)
		}
	}
}
