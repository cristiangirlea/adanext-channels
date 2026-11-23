package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const buttonSeparator = "\x1f"

type button struct {
	Text string `json:"text"`
	Data string `json:"callback_data"`
}

type keyboard struct {
	Rows [][]button `json:"inline_keyboard"`
}

type replyParameters struct {
	MessageID                int64 `json:"message_id"`
	AllowSendingWithoutReply bool  `json:"allow_sending_without_reply"`
}

type sendMessage struct {
	ChatID string           `json:"chat_id"`
	Text   string           `json:"text"`
	Reply  *replyParameters `json:"reply_parameters,omitempty"`
	Markup *keyboard        `json:"reply_markup,omitempty"`
}

func splitMessage(text string) []string {
	const limit = 4096
	parts := sdk.Split(text, limit)
	result := parts[:0]
	for len(parts) > 0 {
		part := parts[0]
		parts = parts[1:]
		text = text[len(part):]
		if strings.TrimSpace(part) != "" {
			result = append(result, part)
			continue
		}
		text = strings.TrimLeftFunc(text, unicode.IsSpace)
		if text == "" {
			break
		}
		parts = sdk.Split(text, limit)
	}
	return result
}

type partialSend struct {
	payload [sha256.Size]byte
	next    int
}

type multipartProgress struct {
	mu      sync.Mutex
	pending []partialSend
}

func (m *multipartProgress) take(payload [sha256.Size]byte) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.pending {
		if item.payload == payload {
			m.pending = slices.Delete(m.pending, i, i+1)
			return item.next
		}
	}
	return 0
}

func (m *multipartProgress) remember(item partialSend) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Match the SDK's replay window without retaining chat content.
	if len(m.pending) == 4096 {
		m.pending = m.pending[1:]
	}
	m.pending = append(m.pending, item)
}

func (s *service) Deliver(ctx context.Context, d p.Deliver) (string, error) {
	req := sendMessage{ChatID: d.ChatID}
	if d.ReplyTo != nil {
		chat, id, ok := strings.Cut(*d.ReplyTo, ":")
		n, err := strconv.ParseInt(id, 10, 64)
		if !ok || chat != d.ChatID || err != nil || n <= 0 {
			return "", &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "reply_to must identify a message in the destination chat"}
		}
		req.Reply = &replyParameters{MessageID: n, AllowSendingWithoutReply: true}
	}
	var markup keyboard
	if len(d.Buttons) > 100 {
		return "", &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "Telegram allows at most 100 buttons"}
	}
	for _, b := range d.Buttons {
		if strings.TrimSpace(b.Label) == "" {
			return "", &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "button label is blank"}
		}
		data := d.ID + buttonSeparator + b.ID
		if len(data) > 64 {
			return "", &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "ids are too long for Telegram buttons"}
		}
		markup.Rows = append(markup.Rows, []button{{Text: b.Label, Data: data}})
	}
	parts := splitMessage(d.Text)
	if len(parts) == 0 {
		return "", &sdk.DeliveryError{Code: p.CodeBadRequest, Message: "nothing to send"}
	}
	var progress partialSend
	if len(parts) > 1 {
		payload, err := json.Marshal(d)
		if err != nil {
			return "", fmt.Errorf("encode multipart delivery: %w", err)
		}
		progress.payload = sha256.Sum256(payload)
		progress.next = s.partial.take(progress.payload)
	}
	var lastID string
	for i := progress.next; i < len(parts); i++ {
		req.Text = parts[i]
		if i == len(parts)-1 && len(markup.Rows) > 0 {
			req.Markup = &markup
		}
		var sent struct {
			ID   int64 `json:"message_id"`
			Chat struct {
				ID int64 `json:"id"`
			} `json:"chat"`
		}
		failure := s.call(ctx, "sendMessage", req, &sent)
		if failure == nil && sent.ID <= 0 {
			failure = &sdk.DeliveryError{Code: p.CodeInternal, Message: "sendMessage returned no message id"}
		}
		if failure != nil {
			if i > 0 {
				progress.next = i
				s.partial.remember(progress)
			}
			return "", failure
		}
		chat := d.ChatID
		if sent.Chat.ID != 0 {
			chat = strconv.FormatInt(sent.Chat.ID, 10)
		}
		lastID = fmt.Sprintf("%s:%d", chat, sent.ID)
	}
	return lastID, nil
}
