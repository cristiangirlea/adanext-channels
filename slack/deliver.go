package main

import (
	"cmp"
	"context"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const separator = "\x1f"

var escape = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
var unescape = strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">")

func messageParts(raw string) []string {
	const limit = 3000
	text := escape.Replace(raw)
	var parts []string
	for len(text) > 0 {
		part := text
		if len(text) > limit {
			// One extra rune lets Split choose its usual whitespace or rune boundary.
			end := limit + 1
			for end < len(text) && !utf8.RuneStart(text[end]) {
				end++
			}
			part = sdk.Split(text[:end], limit)[0]
			// An escape must travel in one message for Slack to render it literally.
			if i := strings.LastIndexByte(part, '&'); i >= 0 && !strings.Contains(part[i:], ";") {
				part = part[:i]
			}
		}
		text = text[len(part):]
		if strings.TrimSpace(part) != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func decodeValue(value string) (string, string, bool) {
	d, b, ok := strings.Cut(value, separator)
	return d, b, ok && d != "" && b != "" && !strings.Contains(b, separator) && len(value) <= 2000
}

type textObject struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type element struct {
	Type     string     `json:"type"`
	Text     textObject `json:"text"`
	ActionID string     `json:"action_id"`
	Value    string     `json:"value"`
	Style    string     `json:"style,omitempty"`
}
type block struct {
	Type     string      `json:"type"`
	Text     *textObject `json:"text,omitempty"`
	Elements []element   `json:"elements,omitempty"`
}
type outgoing struct {
	Channel     string  `json:"channel"`
	Text        string  `json:"text"`
	Thread      string  `json:"thread_ts,omitempty"`
	UnfurlLinks bool    `json:"unfurl_links"`
	UnfurlMedia bool    `json:"unfurl_media"`
	Markdown    bool    `json:"mrkdwn"`
	Parse       string  `json:"parse"`
	Blocks      []block `json:"blocks,omitempty"`
}

func (s *service) Deliver(ctx context.Context, d p.Deliver) (string, error) {
	if !chatID.MatchString(d.ChatID) {
		return "", failure(p.CodeBadRequest, "invalid Slack destination")
	}
	req := outgoing{Channel: d.ChatID, Parse: "none"}
	if d.ReplyTo != nil {
		chat, ts, ok := strings.Cut(*d.ReplyTo, ":")
		if !ok || chat != d.ChatID || instant(ts).IsZero() {
			return "", failure(p.CodeBadRequest, "invalid Slack thread reference")
		}
		s.threadMu.Lock()
		req.Thread = cmp.Or(s.threads[*d.ReplyTo], ts)
		s.threadMu.Unlock()
	}
	if len(d.Buttons) > 25 {
		return "", failure(p.CodeBadRequest, "Slack allows at most 25 buttons")
	}
	var rows []block
	for i, b := range d.Buttons {
		value := d.ID + separator + b.ID
		if _, _, ok := decodeValue(value); !ok || strings.TrimSpace(b.Label) == "" || utf8.RuneCountInString(b.Label) > 75 {
			return "", failure(p.CodeBadRequest, "invalid Slack button label or value")
		}
		if i%5 == 0 {
			rows = append(rows, block{Type: "actions"})
		}
		button := element{Type: "button", Text: textObject{"plain_text", b.Label}, Value: value, ActionID: "ada_" + strconv.Itoa(i)}
		if i == 0 {
			button.Style = "primary"
		}
		rows[i/5].Elements = append(rows[i/5].Elements, button)
	}
	parts := messageParts(d.Text)
	if len(parts) == 0 {
		return "", failure(p.CodeBadRequest, "nothing to send")
	}
	var last string
	for i, part := range parts {
		req.Text = part
		if i == len(parts)-1 && len(rows) > 0 {
			req.Blocks = append([]block{{Type: "section", Text: &textObject{"plain_text", unescape.Replace(part)}}}, rows...)
		}
		r, err := s.call(ctx, "POST", "chat.postMessage", nil, req)
		if err != nil {
			return "", err
		}
		if instant(r.TS).IsZero() {
			return "", failure(p.CodeInternal, "Slack returned an invalid message timestamp")
		}
		last = d.ChatID + ":" + r.TS
	}
	return last, nil
}
