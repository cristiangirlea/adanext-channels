package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type relation struct {
	Reply struct {
		ID string `json:"event_id"`
	} `json:"m.in_reply_to"`
}

type outgoing struct {
	Type    string    `json:"msgtype"`
	Body    string    `json:"body"`
	Relates *relation `json:"m.relates_to,omitempty"`
}

func (msg outgoing) encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(msg)
	return buf.Bytes(), err
}

// Start at the text limit, then split only parts whose JSON escapes or reply
// reference put them over the wire limit. Each transaction sends the bytes measured here.
func (msg outgoing) parts(text string) ([][]byte, error) {
	var parts [][]byte
	var add func(string) error
	add = func(part string) error {
		msg.Body = part
		data, err := msg.encode()
		if err != nil {
			return err
		}
		if len(data) <= 60000 {
			parts = append(parts, data)
			return nil
		}
		smaller := sdk.Split(part, max(4, len(part)/2))
		if len(smaller) < 2 {
			return &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "Matrix message metadata exceeds event size limit"}
		}
		for _, piece := range smaller {
			if err := add(piece); err != nil {
				return err
			}
		}
		return nil
	}
	for _, part := range sdk.Split(text, 30000) {
		if strings.TrimSpace(part) != "" {
			if err := add(part); err != nil {
				return nil, err
			}
		}
	}
	if len(parts) == 0 {
		return nil, &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "nothing to send"}
	}
	return parts, nil
}

func (s *service) send(ctx context.Context, room, txn string, data []byte) (string, error) {
	var sent struct {
		ID string `json:"event_id"`
	}
	path := "rooms/" + url.PathEscape(room) + "/send/m.room.message/" + url.PathEscape(txn)
	if err := s.call(ctx, http.MethodPut, path, bytes.NewReader(data), &sent); err != nil {
		return "", err
	}
	if sent.ID == "" {
		return "", errors.New("Matrix send returned no event_id")
	}
	return s.redact(sent.ID), nil
}

const promptLifetime = 10 * time.Minute

func (s *service) Deliver(ctx context.Context, d p.Deliver) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-s.ready:
	}
	text := d.Text
	if len(d.Buttons) > 0 {
		choices := make([]string, len(d.Buttons))
		for i, b := range d.Buttons {
			choices[i] = fmt.Sprintf("%d to %s", i+1, b.Label)
		}
		text += "\n\nReply " + strings.Join(choices, ", ")
	}
	var msg outgoing
	msg.Type = "m.text"
	if d.ReplyTo != nil {
		msg.Relates = &relation{}
		msg.Relates.Reply.ID = *d.ReplyTo
	}
	parts, err := msg.parts(text)
	if err != nil {
		return "", err
	}
	var id string
	var started time.Time
	for i, part := range parts {
		s.mu.Lock()
		if s.state.Encrypted[d.ChatID] {
			s.mu.Unlock()
			return "", &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "Ada cannot write to an encrypted room"}
		}
		if i == len(parts)-1 && len(d.Buttons) > 0 {
			// A fast reply must see the prompt whose final part just appeared in the room.
			defer s.mu.Unlock()
			started = s.now()
		} else {
			s.mu.Unlock()
		}
		sent, err := s.send(ctx, d.ChatID, d.ID+"-"+strconv.Itoa(i), part)
		if err != nil {
			return "", err
		}
		id = sent
	}
	if len(d.Buttons) > 0 {
		next := s.state
		for _, item := range next.Prompts {
			if item.Room == d.ChatID && item.Delivery == d.ID {
				return id, nil
			}
		}
		active := slices.Clone(next.Prompts)
		for i, item := range active {
			if item.Room == d.ChatID || !started.Before(item.Created.Add(promptLifetime)) {
				active[i].Buttons = nil
			}
		}
		armed := slices.Contains(next.OwnEvents[d.ChatID], id)
		active = append(active, prompt{Room: d.ChatID, Delivery: d.ID, Buttons: d.Buttons, Created: started, EventID: id, Armed: armed})
		next.Prompts = slices.Clone(active[max(0, len(active)-32):])
		if err := s.save(next); err != nil {
			return "", err
		}
		// An echo in the current batch only becomes durable with its cursor.
		if !armed && slices.Contains(s.ownEvents[d.ChatID], id) {
			s.armed[d.ID] = true
		}
	}
	return id, nil
}

func (s *service) observePrompt(room, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids, ok := s.ownEvents[room]
	if !ok {
		ids = slices.Clone(s.state.OwnEvents[room])
	}
	const limit = 64
	if i := slices.Index(ids, id); i >= 0 {
		ids = slices.Delete(ids, i, i+1)
	} else if len(ids) == limit {
		ids = slices.Delete(ids, 0, 1)
	}
	s.ownEvents[room] = append(ids, id)
	for _, item := range s.state.Prompts {
		if item.Room == room && item.EventID == id && !item.Armed {
			s.armed[item.Delivery] = true
		}
	}
}

func (s *service) action(id, room, sender, text string) *p.Action {
	n, err := strconv.Atoi(text)
	if err != nil || n < 1 || strconv.Itoa(n) != text {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if action, ok := s.state.Actions[id]; ok {
		s.actions[id] = action
		return &action
	}
	for _, item := range s.state.Prompts {
		if item.Room != room || n > len(item.Buttons) || (!item.Armed && !s.armed[item.Delivery]) || item.Used[sender] || s.used[item.Delivery][sender] || !s.now().Before(item.Created.Add(promptLifetime)) {
			continue
		}
		if s.used[item.Delivery] == nil {
			s.used[item.Delivery] = make(map[string]bool)
		}
		s.used[item.Delivery][sender] = true
		action := p.Action{DeliveryID: s.redact(item.Delivery), ButtonID: s.redact(item.Buttons[n-1].ID)}
		s.actions[id] = action
		return &action
	}
	return nil
}
