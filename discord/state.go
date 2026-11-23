package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/cristiangirlea/adanext-channels/sdk"
)

const stateFile = "discord-state.json"

type cursor struct {
	Last  string `json:"last"`
	Guild string `json:"guild,omitempty"`
}

func snowflake(id string) uint64 {
	n, err := strconv.ParseUint(id, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != id {
		return 0
	}
	return n
}

func loadState(dir string) (map[string]cursor, error) {
	state := make(map[string]cursor)
	data, err := os.ReadFile(filepath.Join(dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Discord state: %w", err)
	}
	if err = json.Unmarshal(data, &state); err != nil || state == nil {
		return nil, errors.New("invalid Discord state file")
	}
	for chat, pos := range state {
		if snowflake(chat) == 0 || (pos.Last != "0" && snowflake(pos.Last) == 0) || (pos.Guild != "" && snowflake(pos.Guild) == 0) {
			return nil, errors.New("invalid Discord state cursor")
		}
	}
	return state, nil
}

func (s *service) save(chat string, pos cursor) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if current, ok := s.state[chat]; ok && snowflake(current.Last) >= snowflake(pos.Last) {
		return nil
	}
	next := maps.Clone(s.state)
	next[chat] = pos
	if err := os.MkdirAll(s.stateDir, 0700); err != nil {
		return fmt.Errorf("create Discord state directory: %w", err)
	}
	tmp, err := os.CreateTemp(s.stateDir, ".discord-state-*")
	if err != nil {
		return fmt.Errorf("create Discord state file: %w", err)
	}
	defer os.Remove(tmp.Name())
	err = json.NewEncoder(tmp).Encode(next)
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), filepath.Join(s.stateDir, stateFile))
	}
	if err == nil && runtime.GOOS != "windows" {
		var dir *os.File
		dir, err = os.Open(s.stateDir)
		if err == nil {
			err = dir.Sync()
			dir.Close()
		}
	}
	if err != nil {
		return fmt.Errorf("replace Discord state: %w", err)
	}
	s.state = next
	return nil
}

func (s *service) snapshot() map[string]cursor {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return maps.Clone(s.state)
}

func (s *service) Receive(ctx context.Context) ([]sdk.Inbound, error) {
	if s.cancel == nil {
		var runCtx context.Context
		runCtx, s.cancel = context.WithCancel(ctx)
		go func() { defer close(s.ended); s.runErr = s.gateway(runCtx) }()
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ended:
			return nil, s.runErr
		case batch := <-s.batches:
			s.stateMu.Lock()
			visible := batch[:0]
			for _, in := range batch {
				if _, ok := s.pending[in.ID]; ok {
					continue
				}
				if in.Action == nil && snowflake(in.ID) <= snowflake(s.state[in.Chat.ID].Last) {
					continue
				}
				// An interaction ID is never a message-history cursor.
				chat := ""
				if in.Action == nil {
					chat = in.Chat.ID
				}
				s.pending[in.ID] = chat
				visible = append(visible, in)
			}
			s.stateMu.Unlock()
			if len(visible) > 0 {
				return visible, nil
			}
		}
	}
}

func (s *service) Commit(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	chat, ok := s.pending[id]
	if !ok {
		return errors.New("unknown Discord commit")
	}
	if chat != "" {
		s.stateMu.Lock()
		pos := s.state[chat]
		s.stateMu.Unlock()
		pos.Last = id
		if err := s.save(chat, pos); err != nil {
			return err
		}
	}
	delete(s.pending, id)
	return nil
}
