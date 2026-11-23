package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cristiangirlea/adanext-channels/internal/jsonline"
	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type directory struct {
	dir string
	// Only complete records receive a byte offset that can be committed.
	offsets    map[string]int64
	deliveryMu sync.Mutex
}

func (d *directory) position() (int64, error) {
	data, err := os.ReadFile(filepath.Join(d.dir, "position"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read position: %w", err)
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || offset < 0 {
		return 0, errors.New("invalid echo position")
	}
	return offset, nil
}

func (d *directory) Receive(ctx context.Context) ([]sdk.Inbound, error) {
	offset, err := d.position()
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		messages, err := d.read(offset)
		if err != nil || len(messages) > 0 {
			return messages, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (d *directory) read(offset int64) ([]sdk.Inbound, error) {
	f, err := os.Open(filepath.Join(d.dir, "messages.jsonl"))
	if errors.Is(err, os.ErrNotExist) && offset == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open messages: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat messages: %w", err)
	}
	if offset > st.Size() {
		return nil, errors.New("echo position exceeds message file")
	}
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek messages: %w", err)
	}
	r := bufio.NewReader(f)
	d.offsets = make(map[string]int64)
	var batch []sdk.Inbound
	var lastUnique string
	for len(batch) < 64 {
		line, err := jsonline.Read(r)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read message: %w", err)
		}
		var msg sdk.Inbound
		if err = json.Unmarshal(line, &msg); err != nil {
			return nil, fmt.Errorf("decode message: %w", err)
		}
		// The OS cursor includes buffered read-ahead and counts CRLF exactly as stored.
		next, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("locate message: %w", err)
		}
		// The SDK commits each distinct ID once. A trailing duplicate belongs to the
		// most recent distinct message's offset so an earlier ack cannot skip it.
		if _, seen := d.offsets[msg.ID]; !seen {
			lastUnique = msg.ID
		}
		d.offsets[lastUnique] = next - int64(r.Buffered())
		batch = append(batch, msg)
	}
	return batch, nil
}

func (d *directory) Commit(ctx context.Context, id string) error {
	offset, ok := d.offsets[id]
	if !ok {
		return fmt.Errorf("no offset for %q", id)
	}
	tmp, err := os.CreateTemp(d.dir, ".position-*")
	if err != nil {
		return fmt.Errorf("create position: %w", err)
	}
	defer os.Remove(tmp.Name())
	_, err = fmt.Fprintln(tmp, offset)
	if err = errors.Join(err, tmp.Sync(), tmp.Close()); err != nil {
		return fmt.Errorf("save position: %w", err)
	}
	if err = replaceFile(ctx, tmp.Name(), filepath.Join(d.dir, "position")); err != nil {
		return fmt.Errorf("replace position: %w", err)
	}
	delete(d.offsets, id)
	return nil
}

func (d *directory) Deliver(ctx context.Context, msg p.Deliver) (string, error) {
	d.deliveryMu.Lock()
	defer d.deliveryMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f, err := os.OpenFile(filepath.Join(d.dir, "deliveries.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return "", fmt.Errorf("open deliveries: %w", err)
	}
	err = json.NewEncoder(f).Encode(msg)
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return "", fmt.Errorf("append delivery: %w", err)
	}
	return msg.ID, nil
}

// Windows refuses a rename while another process holds the target open, so retry briefly.
func replaceFile(ctx context.Context, from, to string) error {
	for attempt := 1; attempt < 20; attempt++ {
		err := os.Rename(from, to)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(time.Duration(attempt) * 5 * time.Millisecond):
		}
	}
	return os.Rename(from, to)
}
