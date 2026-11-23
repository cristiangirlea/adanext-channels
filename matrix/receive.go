package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const timelineFilter = `{"types":["m.room.message","m.room.member","m.room.encrypted","m.room.encryption"]}`
const syncFilter = `{"presence":{"types":[]},"account_data":{"types":[]},"room":{
	"timeline":` + timelineFilter + `,"state":{"types":["m.room.member","m.room.encryption"]},
	"ephemeral":{"types":[]},"account_data":{"types":[]}}}`

func (s *service) connect(ctx context.Context) error {
	var account struct {
		ID string `json:"user_id"`
	}
	if err := s.call(ctx, http.MethodGet, "account/whoami", nil, &account); err != nil {
		return fmt.Errorf("Matrix whoami: %w", err)
	}
	if account.ID == "" {
		return errors.New("Matrix whoami returned no user_id")
	}
	s.userID = account.ID
	return s.load()
}

func (s *service) report(ctx context.Context, state string) error {
	if s.status == state {
		return nil
	}
	if err := sdk.Status(ctx, state, ""); err != nil {
		return fmt.Errorf("report Matrix status: %w", err)
	}
	s.status = state
	return nil
}

func (s *service) Receive(ctx context.Context) ([]sdk.Inbound, error) {
	for attempt := 0; ctx.Err() == nil; attempt++ {
		var batch []sdk.Inbound
		var err error
		if s.userID == "" {
			err = s.connect(ctx)
		} else {
			batch, err = s.syncOnce(ctx)
		}
		if err == nil {
			if err := s.report(ctx, p.StateConnected); err != nil {
				return nil, err
			}
			if len(batch) > 0 {
				return batch, nil
			}
			attempt = -1
			continue
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var failure *sdk.DeliveryError
		if !errors.As(err, &failure) {
			return nil, err
		}
		if failure.Code == p.CodeUnauthorised {
			s.log.Printf("matrix: unauthorised, stopping sync")
			if err := s.report(ctx, p.StateUnauthorised); err != nil {
				return nil, err
			}
			return nil, err
		}
		s.log.Printf("matrix: %v", err)
		if err := s.report(ctx, p.StateReconnecting); err != nil {
			return nil, err
		}
		if err := s.backoff.Wait(ctx, attempt, failure.RetryAfter); err != nil {
			return nil, fmt.Errorf("wait for Matrix: %w", err)
		}
	}
	return nil, ctx.Err()
}

type events struct {
	Events []roomEvent `json:"events"`
}
type joinedRoom struct {
	State    events `json:"state"`
	Timeline struct {
		Events   []roomEvent `json:"events"`
		Limited  bool        `json:"limited"`
		Previous string      `json:"prev_batch"`
	} `json:"timeline"`
}
type syncResponse struct {
	Next  string `json:"next_batch"`
	Rooms struct {
		Invite map[string]struct {
			State events `json:"invite_state"`
		} `json:"invite"`
		Join  map[string]joinedRoom `json:"join"`
		Leave map[string]struct{}   `json:"leave"`
	} `json:"rooms"`
}

func (s *service) syncOnce(ctx context.Context) (batch []sdk.Inbound, err error) {
	s.mu.Lock()
	current := s.state
	s.mu.Unlock()
	query := url.Values{"filter": {syncFilter}, "timeout": {"30000"}}
	if current.Since != "" {
		query.Set("since", current.Since)
	}
	select {
	case <-s.ready:
	default:
		// Recover encryption state even from checkpoints made with an older filter.
		query.Set("full_state", "true")
		defer func() {
			if err == nil {
				close(s.ready)
			}
		}()
	}
	var sync syncResponse
	if err := s.call(ctx, http.MethodGet, "sync?"+query.Encode(), nil, &sync); err != nil {
		return nil, fmt.Errorf("Matrix sync: %w", err)
	}
	if sync.Next == "" {
		return nil, errors.New("Matrix sync returned no next_batch")
	}
	var encrypted []string
	for room, joined := range sync.Rooms.Join {
		if hasEncryption(joined.State.Events) || hasEncryption(joined.Timeline.Events) {
			encrypted = append(encrypted, room)
		}
	}
	if err := s.rememberEncryption(encrypted...); err != nil {
		return nil, err
	}
	for room, invite := range sync.Rooms.Invite {
		for _, e := range invite.State.Events {
			if e.Type == "m.room.member" && e.StateKey == s.userID && e.Content.Membership == "invite" && (e.Content.Direct || s.groups) {
				if err := s.call(ctx, http.MethodPost, "join/"+url.PathEscape(room), strings.NewReader("{}"), nil); err != nil {
					var failure *sdk.DeliveryError
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}
					if errors.Is(err, errNoResponse) || errors.As(err, &failure) && (failure.Code == p.CodeUnauthorised || failure.Code == p.CodeRateLimited) {
						return nil, fmt.Errorf("join Matrix invite: %w", err)
					}
					s.log.Printf("matrix: skipping join invite for %s: %v", room, err)
				}
				break
			}
		}
	}
	// A retry must interpret membership changes from the same pre-batch snapshot.
	if len(sync.Rooms.Join)+len(sync.Rooms.Leave) > 0 {
		previous := maps.Clone(s.members)
		defer func() {
			if err != nil {
				s.members = previous
			}
		}()
	}
	for room := range sync.Rooms.Leave {
		delete(s.members, room)
	}
	s.next = sync.Next
	s.mu.Lock()
	s.ownEvents = make(map[string][]string)
	s.armed = make(map[string]bool)
	s.mu.Unlock()
	if current.Since == "" {
		return nil, s.advance(nil)
	}
	s.used = make(map[string]map[string]bool)
	s.actions = make(map[string]p.Action)
	seen := make(map[string]bool, len(current.Recent))
	for _, id := range current.Recent {
		seen[id] = true
	}
	for _, room := range slices.Sorted(maps.Keys(sync.Rooms.Join)) {
		joined := sync.Rooms.Join[room]
		if joined.Timeline.Limited {
			history, err := s.gap(ctx, room, joined.Timeline.Previous, current.Since)
			if err != nil {
				if s.skipRoomError("read room history", err) {
					continue
				}
				return nil, err
			}
			if hasEncryption(history) {
				if err := s.rememberEncryption(room); err != nil {
					return nil, err
				}
			}
			joined.Timeline.Events = append(history, joined.Timeline.Events...)
		}
		msgs, err := s.roomMessages(ctx, room, joined, seen)
		if err != nil {
			if s.skipRoomError("read room", err) {
				continue
			}
			return nil, err
		}
		batch = append(batch, msgs...)
	}
	if len(batch) == 0 {
		return nil, s.advance(nil)
	}
	s.pending = make(map[string]bool, len(batch))
	s.batchIDs = make([]string, 0, len(batch))
	for _, msg := range batch {
		s.pending[msg.ID] = true
		s.batchIDs = append(s.batchIDs, msg.ID)
	}
	if err := s.stageActions(); err != nil {
		return nil, err
	}
	return batch, nil
}

// A limited incremental timeline has a gap between the old cursor and its first event.
func (s *service) gap(ctx context.Context, room, from, to string) ([]roomEvent, error) {
	if from == "" {
		return nil, errors.New("limited Matrix timeline has no prev_batch")
	}
	const limit = 1000
	var reverse []roomEvent
	pages := map[string]bool{}
	for from != "" && from != to {
		if pages[from] {
			return nil, errors.New("Matrix history pagination repeated a token")
		}
		pages[from] = true
		q := url.Values{"dir": {"b"}, "from": {from}, "to": {to}, "filter": {timelineFilter}, "limit": {strconv.Itoa(limit - len(reverse))}}
		var page struct {
			Chunk []roomEvent `json:"chunk"`
			End   string      `json:"end"`
		}
		if err := s.call(ctx, http.MethodGet, "rooms/"+url.PathEscape(room)+"/messages?"+q.Encode(), nil, &page); err != nil {
			return nil, fmt.Errorf("fill Matrix timeline gap: %w", err)
		}
		take := min(len(page.Chunk), limit-len(reverse))
		reverse = append(reverse, page.Chunk[:take]...)
		from = page.End
		if len(reverse) == limit {
			dropped := len(page.Chunk) - take
			if from != "" && from != to {
				s.log.Printf("matrix: room %s gap reached %d events; dropped %d returned events plus uncounted older history", room, limit, dropped)
			} else if dropped > 0 {
				s.log.Printf("matrix: room %s gap dropped %d events", room, dropped)
			}
			break
		}
	}
	slices.Reverse(reverse)
	return reverse, nil
}
