package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type user struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
	Bot       bool   `json:"is_bot"`
}

type media struct {
	FileID string `json:"file_id"`
	Name   string `json:"file_name"`
	MIME   string `json:"mime_type"`
	Size   int64  `json:"file_size"`
	Width  int64  `json:"width"`
	Height int64  `json:"height"`
}

type message struct {
	Date int64 `json:"date"`
	From user  `json:"from"`
	Chat struct {
		ID   int64  `json:"id"`
		Type string `json:"type"`
	} `json:"chat"`
	Text    string `json:"text"`
	Caption string `json:"caption"`
	Reply   *struct {
		ID int64 `json:"message_id"`
	} `json:"reply_to_message"`
	Photo    []media `json:"photo"`
	Document *media  `json:"document"`
	Audio    *media  `json:"audio"`
	Video    *media  `json:"video"`
	Voice    *media  `json:"voice"`
}

type update struct {
	ID      int64    `json:"update_id"`
	Message *message `json:"message"`
	Query   *struct {
		ID      string   `json:"id"`
		From    user     `json:"from"`
		Message *message `json:"message"`
		Data    string   `json:"data"`
	} `json:"callback_query"`
}

func (s *service) attachments(m *message) []p.Attachment {
	var items []p.Attachment
	add := func(file *media, name, mime string) {
		if file == nil {
			return
		}
		if file.Name != "" {
			name = file.Name
		}
		if file.MIME != "" {
			mime = file.MIME
		}
		items = append(items, p.Attachment{Name: s.redact(name), MIME: s.redact(mime), Size: file.Size, Ref: s.redact(file.FileID)})
	}
	var largest *media
	for i := range m.Photo {
		size := &m.Photo[i]
		if largest == nil || size.Width*size.Height > largest.Width*largest.Height ||
			(size.Width*size.Height == largest.Width*largest.Height && size.Size > largest.Size) {
			largest = size
		}
	}
	add(largest, "photo.jpg", "image/jpeg")
	add(m.Document, "document", "application/octet-stream")
	add(m.Audio, "audio.mp3", "audio/mpeg")
	add(m.Video, "video.mp4", "video/mp4")
	add(m.Voice, "voice.ogg", "audio/ogg")
	return items
}

func (s *service) inbound(ctx context.Context, u update) (sdk.Inbound, string) {
	var in sdk.Inbound
	m := u.Message
	if q := u.Query; q != nil {
		// Dismiss the spinner even when this update will be skipped.
		if q.ID != "" {
			s.call(ctx, "answerCallbackQuery", map[string]string{"callback_query_id": q.ID}, nil)
		}
		m = q.Message
	}
	if m == nil {
		return in, "unsupported update"
	}
	from := m.From
	if u.Query != nil {
		from = u.Query.From
	}
	if from.Bot {
		return in, "bot sender"
	}
	kind := m.Chat.Type
	switch kind {
	case "private":
	case "group", "supergroup":
		if !s.groups {
			return in, "groups disabled"
		}
		kind = "group"
	default:
		return in, "unsupported chat"
	}
	if from.ID == 0 || m.Chat.ID == 0 {
		return in, "missing sender or chat"
	}
	name := strings.TrimSpace(from.FirstName + " " + from.LastName)
	if name == "" {
		name = from.Username
	}
	in.ID = strconv.FormatInt(u.ID, 10)
	in.Sender = p.Sender{ID: strconv.FormatInt(from.ID, 10), Name: s.redact(name)}
	in.Chat = p.Chat{ID: strconv.FormatInt(m.Chat.ID, 10), Type: &kind}
	if q := u.Query; q != nil {
		delivery, button, ok := strings.Cut(q.Data, buttonSeparator)
		if !ok || delivery == "" || button == "" || strings.Contains(button, buttonSeparator) {
			return in, "invalid callback data"
		}
		in.Action = &p.Action{DeliveryID: s.redact(delivery), ButtonID: s.redact(button)}
		return in, ""
	}
	if m.Date != 0 {
		in.Time = time.Unix(m.Date, 0).UTC().Format(time.RFC3339)
	}
	in.Text = s.redact(m.Text)
	if m.Text == "" {
		in.Text = s.redact(m.Caption)
	}
	if m.Reply != nil {
		reply := fmt.Sprintf("%s:%d", in.Chat.ID, m.Reply.ID)
		in.ReplyTo = &reply
	}
	in.Attachments = s.attachments(m)
	return in, ""
}

func (s *service) status(ctx context.Context, state string) error {
	if state == s.state {
		return nil
	}
	if err := sdk.Status(ctx, state, ""); err != nil {
		return fmt.Errorf("report %s: %w", state, err)
	}
	s.state = state
	return nil
}

func (s *service) Receive(ctx context.Context) ([]sdk.Inbound, error) {
	attempt := 0
	for ctx.Err() == nil {
		var updates []update
		var failure *sdk.DeliveryError
		if !s.verified {
			failure = s.call(ctx, "getMe", struct{}{}, nil)
			s.verified = failure == nil
		} else {
			args := map[string]any{"timeout": 30, "allowed_updates": []string{"message", "callback_query"}}
			if s.offset != 0 {
				args["offset"] = s.offset
			}
			failure = s.call(ctx, "getUpdates", args, &updates)
		}
		if failure != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			state := p.StateReconnecting
			if failure.Code == p.CodeUnauthorised {
				state = p.StateUnauthorised
			}
			if err := s.status(ctx, state); err != nil {
				return nil, err
			}
			if err := s.backoff.Wait(ctx, attempt, failure.RetryAfter); err != nil {
				return nil, fmt.Errorf("wait for Telegram: %w", err)
			}
			attempt++
			continue
		}
		attempt = 0
		if err := s.status(ctx, p.StateConnected); err != nil {
			return nil, err
		}
		var batch []sdk.Inbound
		for _, u := range updates {
			in, skip := s.inbound(ctx, u)
			if skip != "" {
				s.log.Printf("skipping update %d: %s", u.ID, skip)
				if len(batch) == 0 {
					s.offset = max(s.offset, u.ID+1)
				} else {
					s.trailingOffset = max(s.trailingOffset, u.ID+1)
				}
				continue
			}
			batch = append(batch, in)
		}
		if len(batch) > 0 {
			s.lastInbound = batch[len(batch)-1].ID
			return batch, nil
		}
	}
	return nil, ctx.Err()
}

func (s *service) Commit(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("parse update id: %s", s.redact(err.Error()))
	}
	if n < 0 || n == math.MaxInt64 {
		return errors.New("invalid Telegram update id")
	}
	s.offset = max(s.offset, n+1)
	// A trailing skip becomes confirmable only after the last visible update's ack.
	if id == s.lastInbound {
		s.offset = max(s.offset, s.trailingOffset)
		s.trailingOffset = 0
	}
	return nil
}
