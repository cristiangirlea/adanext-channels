package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type prompt struct {
	Room     string          `json:"room"`
	Delivery string          `json:"delivery"`
	Buttons  []p.Button      `json:"buttons"`
	EventID  string          `json:"event_id,omitempty"`
	Armed    bool            `json:"armed,omitempty"`
	Used     map[string]bool `json:"used,omitempty"`
	Created  time.Time       `json:"created"`
}

type checkpoint struct {
	Encrypted map[string]bool     `json:"encrypted,omitempty"`
	Since     string              `json:"since"`
	Prompts   []prompt            `json:"prompts,omitempty"`
	OwnEvents map[string][]string `json:"own_events,omitempty"`
	Noticed   []string            `json:"noticed,omitempty"`
	Recent    []string            `json:"recent,omitempty"`
	Actions   map[string]p.Action `json:"uncommitted_actions,omitempty"`
}

func (s *service) load() error {
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return fmt.Errorf("create Matrix state directory: %w", err)
	}
	// A replacement token for the same account should resume that account's cursor.
	key := sha256.Sum256([]byte(s.home + "\x00" + s.userID))
	s.statePath = filepath.Join(s.dir, fmt.Sprintf("matrix-%x.json", key))
	data, err := os.ReadFile(s.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Matrix checkpoint: %w", err)
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return errors.New("invalid Matrix checkpoint JSON")
	}
	s.state.Noticed = s.state.Noticed[max(0, len(s.state.Noticed)-noticeLimit):]
	return nil
}

// The caller holds mu or is still starting the session.
func (s *service) save(next checkpoint) error {
	f, err := os.CreateTemp(s.dir, ".matrix-*")
	if err != nil {
		return fmt.Errorf("create Matrix checkpoint: %w", err)
	}
	defer os.Remove(f.Name())
	err = json.NewEncoder(f).Encode(next)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), s.statePath)
	}
	if err != nil {
		return fmt.Errorf("replace Matrix checkpoint: %w", err)
	}
	s.state = next
	return nil
}

func (s *service) advance(ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.state
	next.Since = s.next
	if len(s.ownEvents) > 0 {
		next.OwnEvents = make(map[string][]string, len(next.OwnEvents))
		maps.Copy(next.OwnEvents, s.state.OwnEvents)
		maps.Copy(next.OwnEvents, s.ownEvents)
	}
	// Publish newly seen prompt events with their cursor. A replay from the old
	// cursor must still treat numbers before the echo as plain text.
	if len(s.armed) > 0 {
		next.Prompts = slices.Clone(next.Prompts)
		for i, item := range next.Prompts {
			if s.armed[item.Delivery] {
				next.Prompts[i].Armed = true
			}
		}
	}
	if len(ids) > 0 {
		recent := append(slices.Clone(next.Recent), ids...)
		next.Recent = slices.Clone(recent[max(0, len(recent)-4096):])
	}
	next.Actions = nil
	if err := s.save(next); err != nil {
		return err
	}
	s.pending = nil
	s.batchIDs = nil
	s.ownEvents = nil
	s.used = nil
	return nil
}

func (s *service) Commit(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := s.pending[id]; !ok {
		return errors.New("unexpected Matrix commit ID")
	}
	if len(s.pending) == 1 {
		return s.advance(s.batchIDs)
	}
	delete(s.pending, id)
	return nil
}

// Preserve an action's meaning if Ada restarts after the prompt expires or changes.
func (s *service) stageActions() error {
	if len(s.actions) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.state
	next.Actions = s.actions
	next.Prompts = slices.Clone(next.Prompts)
	for i, item := range next.Prompts {
		if senders := s.used[item.Delivery]; len(senders) > 0 {
			used := maps.Clone(senders)
			maps.Copy(used, item.Used)
			next.Prompts[i].Used = used
		}
	}
	return s.save(next)
}
