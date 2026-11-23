package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
)

const stateFile = "slack-state.json"

func loadState(dir string) (map[string]string, error) {
	positions := make(map[string]string)
	data, err := os.ReadFile(filepath.Join(dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return positions, nil
	}
	if err != nil {
		return nil, stateError("read", err)
	}
	if json.Unmarshal(data, &positions) != nil || positions == nil {
		return nil, errors.New("invalid Slack state file")
	}
	for chat, ts := range positions {
		if !chatID.MatchString(chat) || instant(ts).IsZero() {
			return nil, errors.New("invalid Slack history cursor")
		}
	}
	return positions, nil
}

// File paths can contain credentials supplied in the process environment.
func stateError(op string, err error) error {
	var path *os.PathError
	if errors.As(err, &path) {
		err = path.Err
	}
	var link *os.LinkError
	if errors.As(err, &link) {
		err = link.Err
	}
	return fmt.Errorf("%s Slack state: %w", op, err)
}

func (s *service) save(chat, ts string) error {
	if !instant(ts).After(instant(s.positions[chat])) {
		return nil
	}
	next := maps.Clone(s.positions)
	next[chat] = ts
	if err := os.MkdirAll(s.stateDir, 0700); err != nil {
		return stateError("create directory for", err)
	}
	tmp, err := os.CreateTemp(s.stateDir, ".slack-state-*")
	if err != nil {
		return stateError("create", err)
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
		return stateError("replace", err)
	}
	s.positions = next
	return nil
}

var errHistoryUnavailable = errors.New("Slack history unavailable")

// Without the bot's own user ID no channel history can be read in this pass.
var errSelfUnavailable = fmt.Errorf("Slack bot identity unavailable: %w", errHistoryUnavailable)

// Retries ran out on a temporary failure, so the history may still be readable later.
var errHistoryRetriesExhausted = fmt.Errorf("Slack history retries exhausted: %w", errHistoryUnavailable)

// Catch-up runs inside Receive, so a server's long Retry-After must not stall live events.
const historyMaxWait = time.Minute

// A variable so tests can shorten one catch-up pass.
var catchUpBudget = 2 * time.Minute

// A conversation that uses up the budget this many passes in a row stops holding its cursor.
const maxOverruns = 3

var slackErrorCode = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// Retry the same cursor so rate limits on later pages cannot restart pagination.
func (s *service) historyCall(ctx context.Context, endpoint string, q url.Values) (apiResponse, error) {
	const maxAttempts = 4
	for attempt := 0; ; attempt++ {
		r, err := s.call(ctx, "GET", endpoint, q, nil)
		var de *sdk.DeliveryError
		if !errors.As(err, &de) {
			return r, err
		}
		code := de.Code
		if slackErrorCode.MatchString(r.Error) {
			code = r.Error
		}
		s.log.Printf("Slack history: %s", code)
		if !r.retryable {
			return r, errHistoryUnavailable
		}
		if attempt+1 == maxAttempts {
			return r, errHistoryRetriesExhausted
		}
		if err := s.backoff.Wait(ctx, attempt, min(de.RetryAfter, historyMaxWait)); err != nil {
			return r, err
		}
	}
}

func (s *service) history(ctx context.Context, chat, oldest string) ([]incoming, error) {
	group := chat[0] != 'D'
	if group && s.self == "" {
		// One attempt per pass: the caller skips channel history when the identity is unknown.
		r, err := s.call(ctx, "GET", "auth.test", nil, nil)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil || r.UserID == "" {
			return nil, errSelfUnavailable
		}
		s.self = r.UserID
	}
	q := url.Values{"channel": {chat}, "oldest": {oldest}, "limit": {"100"}}
	seen := make(map[string]bool)
	var found []incoming
	for {
		page, err := s.historyCall(ctx, "conversations.history", q)
		if err != nil {
			return nil, err
		}
		for _, m := range page.Messages {
			if !instant(m.TS).After(instant(oldest)) {
				continue
			}
			m.Channel = chat
			if group {
				// History represents app mentions as ordinary messages containing the bot user ID.
				if m.Type == "message" && strings.Contains(m.Text, "<@"+s.self+">") {
					m.Type = "app_mention"
				}
			} else {
				m.ChannelType = "im"
			}
			if in, ok := s.fromMessage(m); ok {
				found = append(found, in)
			}
		}
		cursor := page.Metadata.Next
		if cursor == "" && !page.HasMore {
			break
		}
		if cursor == "" || seen[cursor] {
			return nil, fmt.Errorf("Slack history pagination did not advance: %w", errHistoryUnavailable)
		}
		seen[cursor] = true
		q.Set("cursor", cursor)
	}
	slices.SortFunc(found, func(a, b incoming) int { return instant(a.ts).Compare(instant(b.ts)) })
	return found, nil
}

func (s *service) catchUp(ctx context.Context) ([]sdk.Inbound, error) {
	var batch []sdk.Inbound
	// Cursors are kept for whatever is not reached, so the next pass resumes there.
	pass, stop := context.WithTimeout(ctx, catchUpBudget)
	defer stop()
	skipChannels, stopped := false, false
	if s.incomplete == nil {
		s.incomplete = make(map[string]bool)
	}
	if s.overruns == nil {
		s.overruns = make(map[string]int)
	}
	if s.abandoned == nil {
		s.abandoned = make(map[string]bool)
	}
	if s.unreached == nil {
		s.unreached = make(map[string]bool)
	}
	chats := slices.Sorted(maps.Keys(s.positions))
	// Conversations the last pass never reached go first, so one slow conversation cannot starve the rest.
	slices.SortStableFunc(chats, func(a, b string) int {
		switch ia, ib := s.unreached[a], s.unreached[b]; {
		case ia == ib:
			return 0
		case ia:
			return -1
		default:
			return 1
		}
	})
	for _, chat := range chats {
		if chat[0] != 'D' && !s.allowed[chat] || s.abandoned[chat] {
			continue
		}
		if chat[0] != 'D' && skipChannels {
			s.incomplete[chat] = true
			s.unreached[chat] = true
			continue
		}
		if pass.Err() != nil && ctx.Err() == nil {
			if !stopped {
				s.log.Printf("Slack history: catch-up stopped after %s; the rest resumes on the next pass", catchUpBudget)
				stopped = true
			}
			s.incomplete[chat] = true
			s.unreached[chat] = true
			continue
		}
		delete(s.unreached, chat)
		found, err := s.history(pass, chat, s.positions[chat])
		if err != nil {
			switch {
			case errors.Is(err, errSelfUnavailable):
				skipChannels = true
				s.incomplete[chat] = true
				continue
			case errors.Is(err, errHistoryRetriesExhausted), pass.Err() != nil && ctx.Err() == nil:
				s.overran(chat)
				continue
			case errors.Is(err, errHistoryUnavailable):
				// A permanent error: this history cannot be read, so live commits may move the cursor.
				delete(s.incomplete, chat)
				delete(s.overruns, chat)
				continue
			}
			return nil, fmt.Errorf("read Slack history: %w", err)
		}
		delete(s.incomplete, chat)
		delete(s.overruns, chat)
		for _, in := range found {
			if _, ok := s.pending[in.in.ID]; ok || s.recent[in.in.ID] {
				continue
			}
			in.in.Sender.Name = s.name(pass, in.in.Sender.ID)
			s.pending[in.in.ID] = in
			batch = append(batch, in.in)
		}
	}
	return batch, nil
}

// Holding a cursor protects the gap, but a conversation that never finishes must not hold it forever.
func (s *service) overran(chat string) {
	s.overruns[chat]++
	if s.overruns[chat] < maxOverruns {
		s.incomplete[chat] = true
		return
	}
	s.log.Printf("Slack history: giving up the unread history of %s after %d passes; live messages move its cursor again", chat, maxOverruns)
	delete(s.incomplete, chat)
	delete(s.overruns, chat)
	s.abandoned[chat] = true
}
