package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type message struct {
	Type, Subtype, Channel, User, TS, Text string
	ChannelType                            string `json:"channel_type"`
	Thread                                 string `json:"thread_ts"`
	Bot                                    string `json:"bot_id"`
	Files                                  []struct {
		Name, Mimetype string
		Size           int64
		URL            string `json:"url_private"`
	}
}

var timestamp = regexp.MustCompile(`^([0-9]{1,11})\.([0-9]{1,6})$`)

func instant(ts string) time.Time {
	parts := timestamp.FindStringSubmatch(ts)
	if parts == nil {
		return time.Time{}
	}
	sec, _ := strconv.ParseInt(parts[1], 10, 64)
	micros, _ := strconv.Atoi(parts[2] + strings.Repeat("0", 6-len(parts[2])))
	return time.Unix(sec, int64(micros)*1000).UTC()
}

type incoming struct {
	in          sdk.Inbound
	eventID, ts string
	receipt     receipt
	history     bool
}

func (s *service) inbound(user, chat, id string) sdk.Inbound {
	kind := "group"
	if strings.HasPrefix(chat, "D") {
		kind = "private"
	}
	return sdk.Inbound{ID: s.redact.Replace(id), Chat: p.Chat{ID: chat, Type: &kind}, Sender: p.Sender{ID: s.redact.Replace(user)}}
}
func (s *service) fromMessage(m message) (incoming, bool) {
	if !chatID.MatchString(m.Channel) || m.User == "" || m.Bot != "" || m.Subtype != "" && m.Subtype != "file_share" {
		return incoming{}, false
	}
	switch m.Type {
	case "message":
		if m.ChannelType != "im" || m.Channel[0] != 'D' {
			return incoming{}, false
		}
	case "app_mention":
		if !s.allowed[m.Channel] {
			return incoming{}, false
		}
	default:
		return incoming{}, false
	}
	at := instant(m.TS)
	if at.IsZero() {
		return incoming{}, false
	}
	in := s.inbound(m.User, m.Channel, m.Channel+":"+m.TS)
	in.Time = at.Format(time.RFC3339Nano)
	in.Text = s.redact.Replace(m.Text)
	if m.Thread != m.TS && !instant(m.Thread).IsZero() {
		ref := m.Channel + ":" + m.Thread
		in.ReplyTo = &ref
		s.rememberThread(in.ID, m.Thread)
	}
	for _, f := range m.Files {
		in.Attachments = append(in.Attachments, p.Attachment{Name: s.redact.Replace(f.Name), MIME: s.redact.Replace(f.Mimetype), Ref: s.redact.Replace(f.URL), Size: f.Size})
	}
	return incoming{in: in, ts: m.TS}, true
}

func (s *service) rememberThread(id, parent string) {
	const capacity = 4096
	s.threadMu.Lock()
	defer s.threadMu.Unlock()
	if _, exists := s.threads[id]; !exists {
		if len(s.threadOrder) == capacity {
			delete(s.threads, s.threadOrder[0])
			s.threadOrder = s.threadOrder[1:]
		}
		s.threadOrder = append(s.threadOrder, id)
	}
	s.threads[id] = parent
}

func (s *service) decode(e envelope) (incoming, bool) {
	switch e.Type {
	case "events_api":
		var payload struct {
			EventID string `json:"event_id"`
			Event   message
		}
		if json.Unmarshal(e.Payload, &payload) != nil {
			return incoming{}, false
		}
		in, ok := s.fromMessage(payload.Event)
		in.eventID = payload.EventID
		return in, ok
	case "interactive":
		var q struct {
			Type          string
			User, Channel struct{ ID string }
			Actions       []struct {
				Type, Value string
				TS          string `json:"action_ts"`
			}
		}
		if json.Unmarshal(e.Payload, &q) != nil || q.Type != "block_actions" || len(q.Actions) != 1 || q.Actions[0].Type != "button" || q.User.ID == "" || !chatID.MatchString(q.Channel.ID) || q.Channel.ID[0] != 'D' && !s.allowed[q.Channel.ID] {
			return incoming{}, false
		}
		a := q.Actions[0]
		delivery, button, ok := decodeValue(a.Value)
		if !ok {
			return incoming{}, false
		}
		id := "action:" + e.ID
		at := instant(a.TS)
		if !at.IsZero() {
			id = "action:" + q.Channel.ID + ":" + q.User.ID + ":" + a.TS
		}
		in := s.inbound(q.User.ID, q.Channel.ID, id)
		in.Action = &p.Action{DeliveryID: s.redact.Replace(delivery), ButtonID: s.redact.Replace(button)}
		if !at.IsZero() {
			in.Time = at.Format(time.RFC3339Nano)
		}
		return incoming{in: in}, true
	}
	return incoming{}, false
}

func (s *service) name(ctx context.Context, id string) string {
	if name, ok := s.names[id]; ok {
		return name
	}
	r, err := s.call(ctx, "GET", "users.info", url.Values{"user": {id}}, nil)
	var user struct {
		Name     string
		RealName string `json:"real_name"`
		Profile  struct {
			DisplayName string `json:"display_name"`
			RealName    string `json:"real_name"`
		}
	}
	if err != nil {
		if !r.retryable && ctx.Err() == nil && (r.Error == "user_not_found" || r.Error == "user_not_visible") {
			s.names[id] = ""
		}
		return ""
	}
	if json.Unmarshal(r.User, &user) != nil {
		return ""
	}
	name := s.redact.Replace(cmp.Or(user.Profile.DisplayName, user.Profile.RealName, user.RealName, user.Name))
	s.names[id] = name
	return name
}

func (s *service) remember(key string) {
	if key == "" || s.recent[key] {
		return
	}
	const capacity = 8192
	if len(s.order) == capacity {
		delete(s.recent, s.order[0])
		s.order = s.order[1:]
	}
	s.order = append(s.order, key)
	s.recent[key] = true
}

func (s *service) Receive(ctx context.Context) ([]sdk.Inbound, error) {
	if s.cancel == nil {
		var ioCtx context.Context
		ioCtx, s.cancel = context.WithCancel(ctx)
		go s.gateway(ioCtx)
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case in, ok := <-s.events:
			if !ok {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			if in.history {
				batch, err := s.catchUp(ctx)
				if err != nil {
					return nil, err
				}
				if len(batch) > 0 {
					return batch, nil
				}
				continue
			}
			if s.recent[in.in.ID] || in.eventID != "" && s.recent["event:"+in.eventID] || in.ts != "" && s.positions[in.in.Chat.ID] == in.ts {
				s.rememberEvent(in.eventID)
				s.ackStored(ctx, in.receipt)
				continue
			}
			in.in.Sender.Name = s.name(ctx, in.in.Sender.ID)
			s.pending[in.in.ID] = in
			return []sdk.Inbound{in.in}, nil
		}
	}
}

func (s *service) rememberEvent(id string) {
	if id != "" {
		s.remember("event:" + id)
	}
}

func (s *service) ackStored(ctx context.Context, r receipt) {
	if err := r.ack(ctx); err != nil {
		// Ada already stored this inbound; Slack can retry the receipt on another socket.
		s.log.Printf("Slack receipt: %v", err)
		r.conn.Close(1000, "")
	}
}
func (s *service) Commit(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	in, ok := s.pending[id]
	if !ok {
		return errors.New("unknown Slack commit")
	}
	// A conversation with unread history keeps its cursor so the next pass reads the gap.
	if in.ts != "" && !s.incomplete[in.in.Chat.ID] {
		if err := s.save(in.in.Chat.ID, in.ts); err != nil {
			return err
		}
	}
	delete(s.pending, id)
	s.remember(id)
	s.rememberEvent(in.eventID)
	s.ackStored(ctx, in.receipt)
	return ctx.Err()
}
