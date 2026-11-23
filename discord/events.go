package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type user struct {
	ID, Username string
	Name         string `json:"global_name"`
	Bot          bool
}

type message struct {
	ID, Timestamp, Content string
	Chat                   string `json:"channel_id"`
	Guild                  string `json:"guild_id"`
	Author                 user
	Reference              reference `json:"message_reference"`
	Attachments            []struct {
		Filename, URL string
		MIME          string `json:"content_type"`
		Size          int64
	}
}

func (s *service) accepts(guild, chat string) bool {
	return snowflake(chat) != 0 && (guild == "" || guild == s.guild && s.allowed[chat])
}

func (s *service) inbound(id, chat, guild, self string, author user) (sdk.Inbound, bool) {
	if snowflake(id) == 0 || snowflake(author.ID) == 0 || author.Bot || author.ID == self || !s.accepts(guild, chat) {
		return sdk.Inbound{}, false
	}
	kind := "private"
	if guild != "" {
		kind = "group"
	}
	name := author.Name
	if name == "" {
		name = author.Username
	}
	return sdk.Inbound{ID: id, Chat: p.Chat{ID: chat, Type: &kind}, Sender: p.Sender{ID: author.ID, Name: s.redact(name)}}, true
}

func (s *service) mapMessage(m message, self string) (sdk.Inbound, bool) {
	in, ok := s.inbound(m.ID, m.Chat, m.Guild, self, m.Author)
	if !ok {
		return in, false
	}
	in.Text = s.redact(m.Content)
	in.Time = s.redact(m.Timestamp)
	if snowflake(m.Reference.ID) != 0 {
		in.ReplyTo = &m.Reference.ID
	}
	for _, a := range m.Attachments {
		in.Attachments = append(in.Attachments, p.Attachment{Name: s.redact(a.Filename), MIME: s.redact(a.MIME), Ref: s.redact(a.URL), Size: a.Size})
	}
	return in, true
}

type callback struct {
	ID, Token string
	Type      int
}

type interaction struct {
	callback
	Guild  string `json:"guild_id"`
	Chat   string `json:"channel_id"`
	User   user
	Member struct{ User user }
	Data   struct {
		Type     int    `json:"component_type"`
		CustomID string `json:"custom_id"`
	}
}

func (s *service) answer(ctx context.Context, q callback) {
	if q.Type != 3 || snowflake(q.ID) == 0 || q.Token == "" || q.Token == "." || q.Token == ".." {
		return
	}
	err := s.call(ctx, "POST", "/interactions/"+q.ID+"/"+url.PathEscape(q.Token)+"/callback", struct {
		Type int `json:"type"`
	}{6}, nil)
	if err != nil && ctx.Err() == nil {
		s.log.Printf("Discord interaction callback: %v", err)
	}
}

func (s *service) mapInteraction(q interaction, self string) (sdk.Inbound, bool) {
	author := q.User
	if q.Guild != "" {
		author = q.Member.User
	}
	in, ok := s.inbound(q.ID, q.Chat, q.Guild, self, author)
	if !ok || q.Type != 3 || q.Data.Type != 2 || !validCustom(q.Data.CustomID) {
		return in, false
	}
	delivery, button, _ := strings.Cut(q.Data.CustomID, separator)
	in.Action = &p.Action{DeliveryID: s.redact(delivery), ButtonID: s.redact(button)}
	in.Time = time.UnixMilli(int64(snowflake(q.ID)>>22) + 1420070400000).UTC().Format(time.RFC3339Nano)
	return in, true
}

func (s *service) emit(ctx context.Context, batch []sdk.Inbound) error {
	if len(batch) == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.batches <- batch:
		return nil
	}
}

func (s *service) live(ctx context.Context, e gatewayEvent, self string) error {
	var in sdk.Inbound
	var ok bool
	switch e.T {
	case "MESSAGE_CREATE":
		var m message
		if err := json.Unmarshal(e.D, &m); err != nil {
			return fmt.Errorf("decode Discord message: %w", err)
		}
		in, ok = s.mapMessage(m, self)
		if ok {
			s.stateMu.Lock()
			_, known := s.state[m.Chat]
			s.stateMu.Unlock()
			// Register the first message before handing it to Ada, so a crash before its ack can replay it.
			if !known {
				if err := s.save(m.Chat, cursor{Last: strconv.FormatUint(snowflake(m.ID)-1, 10), Guild: m.Guild}); err != nil {
					return err
				}
			}
		}
	case "INTERACTION_CREATE":
		var q interaction
		if err := json.Unmarshal(e.D, &q); err != nil {
			s.log.Printf("skipping malformed Discord interaction")
			return nil
		}
		in, ok = s.mapInteraction(q, self)
	}
	if ok {
		return s.emit(ctx, []sdk.Inbound{in})
	}
	return nil
}

func (s *service) historyUnavailable(chat string, err error) bool {
	var failure *httpFailure
	if errors.As(err, &failure) && (failure.status == 403 || failure.status == 404) {
		s.log.Printf("Discord history unavailable for channel %s: %v", chat, err)
		return true
	}
	return false
}

// Seed before opening a fresh gateway session so live messages cannot race the baseline.
func (s *service) seedChannels(ctx context.Context) error {
	if s.guild == "" || len(s.allowed) == 0 {
		return nil
	}
	known := s.snapshot()
	for chat := range s.allowed {
		if _, ok := known[chat]; ok {
			continue
		}
		var page []struct{ ID string }
		err := s.call(ctx, "GET", "/channels/"+chat+"/messages?limit=1", nil, &page)
		if s.historyUnavailable(chat, err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("initialise Discord history for channel %s: %w", chat, err)
		}
		last := "0"
		if len(page) > 0 {
			last = page[0].ID
			if snowflake(last) == 0 {
				return fmt.Errorf("invalid latest Discord message ID for channel %s", chat)
			}
		}
		if err := s.save(chat, cursor{Last: last, Guild: s.guild}); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) catchUp(ctx context.Context, self string) error {
	for chat, pos := range s.snapshot() {
		if !s.accepts(pos.Guild, chat) {
			continue
		}
		for {
			var page []message
			err := s.call(ctx, "GET", "/channels/"+chat+"/messages?limit=100&after="+pos.Last, nil, &page)
			if s.historyUnavailable(chat, err) {
				break
			}
			if err != nil {
				return fmt.Errorf("read Discord history: %w", err)
			}
			// Discord returns newest first, but Ada may only commit an oldest-first prefix.
			slices.SortFunc(page, func(a, b message) int { return cmp.Compare(snowflake(a.ID), snowflake(b.ID)) })
			after := snowflake(pos.Last)
			var batch []sdk.Inbound
			for _, m := range page {
				if m.Chat != chat || snowflake(m.ID) <= after {
					continue
				}
				after = snowflake(m.ID)
				if m.Guild == "" {
					m.Guild = pos.Guild
				}
				if m.Guild == pos.Guild {
					if in, ok := s.mapMessage(m, self); ok {
						batch = append(batch, in)
					}
				}
			}
			if err := s.emit(ctx, batch); err != nil {
				return err
			}
			if len(page) < 100 {
				break
			}
			if after <= snowflake(pos.Last) {
				return fmt.Errorf("Discord history page did not advance")
			}
			pos.Last = strconv.FormatUint(after, 10)
		}
	}
	return nil
}
