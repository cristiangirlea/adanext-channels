package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type profile struct {
	Name string `json:"display_name"`
}

type memberState struct {
	Membership  string  `json:"membership"`
	DisplayName *string `json:"displayname"`
}

type roomEvent struct {
	ID       string `json:"event_id"`
	Type     string `json:"type"`
	StateKey string `json:"state_key"`
	Sender   string `json:"sender"`
	Time     int64  `json:"origin_server_ts"`
	Unsigned struct {
		Previous *memberState `json:"prev_content"`
	} `json:"unsigned"`
	Content struct {
		memberState
		Direct bool   `json:"is_direct"`
		Type   string `json:"msgtype"`
		Body   string `json:"body"`
		URL    string `json:"url"`
		Info   struct {
			MIME string `json:"mimetype"`
			Size int64  `json:"size"`
		} `json:"info"`
		Relates relation `json:"m.relates_to"`
	} `json:"content"`
}

// A malformed user event must not strand other senders behind the same sync cursor.
func (e *roomEvent) UnmarshalJSON(data []byte) error {
	type wire roomEvent
	var parsed wire
	if err := json.Unmarshal(data, &parsed); err != nil {
		if parsed.Type == "m.room.member" {
			*e = roomEvent{Type: parsed.Type, StateKey: parsed.StateKey}
		}
		return nil
	}
	*e = roomEvent(parsed)
	return nil
}

type audience struct{ kind, name string }

func roomAudience(members map[string]profile, bot, sender string, uncertain bool) audience {
	person, hasSender := members[sender]
	_, hasBot := members[bot]
	if !uncertain && len(members) == 2 && hasBot && hasSender {
		return audience{"private", person.Name}
	}
	return audience{"group", person.Name}
}

func (s *service) audiences(ctx context.Context, room string, joined joinedRoom) ([]audience, error) {
	prior := s.members[room]
	refresh := prior == nil
	names := map[string]string{}
	known := map[string]memberState{}
	record := func(e roomEvent) {
		if e.Type != "m.room.member" {
			return
		}
		refresh = true
		known[e.StateKey] = e.Content.memberState
		if e.Content.DisplayName != nil {
			names[e.StateKey] = *e.Content.DisplayName
		}
	}
	for _, e := range joined.State.Events {
		record(e)
	}
	timeline := joined.Timeline.Events
	before := map[int]memberState{}
	for i, e := range timeline {
		if e.Type != "m.room.member" {
			continue
		}
		prev, ok := known[e.StateKey]
		if !ok && prior != nil {
			prev.Membership = "leave"
			if p, found := prior[e.StateKey]; found {
				prev = memberState{Membership: "join", DisplayName: &p.Name}
			}
		}
		if e.Unsigned.Previous != nil {
			prev = *e.Unsigned.Previous
		}
		before[i] = prev
		record(e)
	}
	if len(timeline) == 0 {
		if refresh {
			delete(s.members, room)
		}
		return nil, nil
	}
	members := prior
	if refresh {
		var result struct {
			Joined map[string]profile `json:"joined"`
		}
		if err := s.call(ctx, http.MethodGet, "rooms/"+url.PathEscape(room)+"/joined_members", nil, &result); err != nil {
			return nil, fmt.Errorf("read Matrix room members: %w", err)
		}
		if result.Joined == nil {
			return nil, errors.New("Matrix joined_members returned no joined map")
		}
		members = result.Joined
		for id, name := range names {
			if _, ok := members[id]; ok {
				members[id] = profile{Name: name}
			}
		}
		s.members[room] = members
	}
	// Walk backwards from the refreshed snapshot so a later leave cannot expose group history as a DM.
	if len(before) > 0 {
		members = maps.Clone(members)
	}
	unknown := map[string]bool{}
	result := make([]audience, len(timeline))
	for i := len(timeline) - 1; i >= 0; i-- {
		e := timeline[i]
		result[i] = roomAudience(members, s.userID, e.Sender, len(unknown) > 0)
		if prev, changed := before[i]; changed {
			delete(members, e.StateKey)
			delete(unknown, e.StateKey)
			switch prev.Membership {
			case "join":
				var p profile
				if prev.DisplayName != nil {
					p.Name = *prev.DisplayName
				}
				members[e.StateKey] = p
			case "invite", "leave", "ban", "knock":
			default:
				unknown[e.StateKey] = true
			}
		}
	}
	return result, nil
}

func (s *service) roomMessages(ctx context.Context, room string, joined joinedRoom, seen map[string]bool) ([]sdk.Inbound, error) {
	rooms, err := s.audiences(ctx, room, joined)
	if err != nil {
		return nil, err
	}
	var batch []sdk.Inbound
	for i, e := range joined.Timeline.Events {
		if e.Sender == s.userID && e.ID != "" {
			s.observePrompt(room, s.redact(e.ID))
			continue
		}
		if e.Sender == "" || e.ID == "" || e.Type != "m.room.message" && e.Type != "m.room.encrypted" {
			continue
		}
		id := s.redact(e.ID)
		if seen[id] {
			continue
		}
		seen[id] = true
		kind := rooms[i].kind
		if kind == "group" && !s.groups {
			continue
		}
		if e.Type == "m.room.encrypted" {
			if err := s.encryptedNotice(ctx, room); err != nil {
				if s.skipRoomError("encryption notice", err) {
					continue
				}
				return nil, err
			}
			continue
		}
		in := sdk.Inbound{ID: id, Chat: p.Chat{ID: s.redact(room), Type: &kind}, Sender: p.Sender{ID: s.redact(e.Sender), Name: s.redact(rooms[i].name)}}
		if e.Time != 0 {
			in.Time = time.UnixMilli(e.Time).UTC().Format(time.RFC3339Nano)
		}
		content := e.Content
		if reply := s.redact(content.Relates.Reply.ID); reply != "" {
			in.ReplyTo = &reply
		}
		in.Text = s.redact(content.Body)
		switch content.Type {
		case "m.text", "m.notice", "m.emote":
			if action := s.action(id, room, in.Sender.ID, content.Body); action != nil {
				in.Action = action
				in.Text = ""
			} else if content.Type == "m.emote" {
				in.Text = "* " + in.Text
			}
		case "m.image", "m.file", "m.audio", "m.video":
			ref, err := url.Parse(content.URL)
			if err != nil || ref.Scheme != "mxc" || ref.Host == "" || len(ref.Path) < 2 {
				continue
			}
			in.Attachments = []p.Attachment{{Name: in.Text, MIME: s.redact(content.Info.MIME), Size: content.Info.Size, Ref: s.redact(content.URL)}}
		default:
			continue
		}
		batch = append(batch, in)
	}
	return batch, nil
}

func hasEncryption(events []roomEvent) bool {
	return slices.ContainsFunc(events, func(e roomEvent) bool { return e.Type == "m.room.encryption" && e.StateKey == "" })
}

func (s *service) rememberEncryption(rooms ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rooms = slices.DeleteFunc(rooms, func(room string) bool { return s.state.Encrypted[room] })
	if len(rooms) == 0 {
		return nil
	}
	next := s.state
	next.Encrypted = make(map[string]bool, len(s.state.Encrypted)+len(rooms))
	maps.Copy(next.Encrypted, s.state.Encrypted)
	for _, room := range rooms {
		next.Encrypted[room] = true
	}
	return s.save(next)
}

const noticeLimit = 1024

func (s *service) encryptedNotice(ctx context.Context, room string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slices.Contains(s.state.Noticed, room) {
		return nil
	}
	// The reserved prefix keeps crash retries idempotent and separate from Ada delivery IDs.
	txn := fmt.Sprintf("~encrypted-%x", sha256.Sum256([]byte(room)))
	data, err := (outgoing{Type: "m.notice", Body: "This bot cannot read encrypted rooms."}).encode()
	if err != nil {
		return err
	}
	if _, err := s.send(ctx, room, txn, data); err != nil {
		return fmt.Errorf("send Matrix encryption notice: %w", err)
	}
	next := s.state
	next.Noticed = append(slices.Clone(next.Noticed[max(0, len(next.Noticed)-noticeLimit+1):]), room)
	return s.save(next)
}
