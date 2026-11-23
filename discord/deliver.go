package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const separator = "\x1f"

type button struct {
	Type     int    `json:"type"`
	Style    int    `json:"style"`
	Label    string `json:"label"`
	CustomID string `json:"custom_id"`
}

type actionRow struct {
	Type       int      `json:"type"`
	Components []button `json:"components"`
}

type reference struct {
	ID   string `json:"message_id"`
	Fail bool   `json:"fail_if_not_exists"`
}

type outgoing struct {
	Nonce        string      `json:"nonce"`
	EnforceNonce bool        `json:"enforce_nonce"`
	Content      string      `json:"content"`
	Reply        *reference  `json:"message_reference,omitempty"`
	Components   []actionRow `json:"components,omitempty"`
	Mentions     struct {
		Parse []string `json:"parse"`
	} `json:"allowed_mentions"`
}

func messageParts(text string) []string {
	const limit = 2000
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

func (s *service) Deliver(ctx context.Context, d p.Deliver) (string, error) {
	var req outgoing
	req.EnforceNonce = true
	req.Mentions.Parse = []string{}
	if snowflake(d.ChatID) == 0 {
		return "", failure(p.CodeBadRequest, "invalid Discord channel ID")
	}
	if d.ReplyTo != nil {
		if snowflake(*d.ReplyTo) == 0 {
			return "", failure(p.CodeBadRequest, "invalid Discord reply ID")
		}
		req.Reply = &reference{ID: *d.ReplyTo}
	}
	if len(d.Buttons) > 25 {
		return "", failure(p.CodeBadRequest, "Discord allows at most 25 buttons")
	}
	var rows []actionRow
	for i, b := range d.Buttons {
		custom := d.ID + separator + b.ID
		if !validCustom(custom) || strings.TrimSpace(b.Label) == "" || utf8.RuneCountInString(b.Label) > 80 {
			return "", failure(p.CodeBadRequest, "invalid Discord button ID or label")
		}
		if i%5 == 0 {
			rows = append(rows, actionRow{Type: 1})
		}
		style := 2
		if i == 0 {
			style = 1
		}
		rows[i/5].Components = append(rows[i/5].Components, button{2, style, b.Label, custom})
	}
	parts := messageParts(d.Text)
	if len(parts) == 0 {
		return "", failure(p.CodeBadRequest, "nothing to send")
	}
	var last string
	for i, part := range parts {
		// Discord's 25-character nonce must distinguish parts while surviving a retry.
		digest := sha256.Sum256([]byte(d.ChatID + separator + d.ID + separator + strconv.Itoa(i)))
		req.Nonce = base64.RawURLEncoding.EncodeToString(digest[:16])
		req.Content = part
		if i == len(parts)-1 {
			req.Components = rows
		}
		var sent struct{ ID string }
		if err := s.call(ctx, "POST", "/channels/"+d.ChatID+"/messages", req, &sent); err != nil {
			return "", err
		}
		if snowflake(sent.ID) == 0 {
			return "", failure(p.CodeInternal, "Discord returned an invalid message ID")
		}
		last = sent.ID
	}
	return last, nil
}

func validCustom(v string) bool {
	delivery, button, ok := strings.Cut(v, separator)
	return ok && delivery != "" && button != "" && !strings.Contains(button, separator) && utf8.RuneCountInString(v) <= 100
}
