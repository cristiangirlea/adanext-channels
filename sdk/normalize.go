package sdk

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

// Normalize fits service content to the channel protocol and describes each repair.
func Normalize(in Inbound) (Inbound, []string) {
	return normalize(in, time.Now())
}

func normalize(in Inbound, received time.Time) (Inbound, []string) {
	var fixes []string
	short := func(field, original string, limit int, id bool) string {
		// JSON would turn each bad byte into a three-byte U+FFFD and push the field past its limit.
		value := strings.ToValidUTF8(original, "�")
		if value != original {
			fixes = append(fixes, field+": replaced invalid UTF-8")
		}
		if strings.IndexFunc(value, unicode.IsControl) >= 0 {
			value = strings.Map(func(r rune) rune {
				if unicode.IsControl(r) {
					return utf8.RuneError
				}
				return r
			}, value)
			fixes = append(fixes, field+": replaced control characters")
		}
		if len(value) <= limit {
			return value
		}
		if id {
			fixes = append(fixes, field+": hashed oversized ID")
			return cutBytes(fmt.Sprintf("%x", sha256.Sum256([]byte(original))), limit)
		}
		fixes = append(fixes, field+": shortened to byte limit")
		return cutBytes(value, limit)
	}

	in.ID = short("id", in.ID, 128, true)
	in.Sender.ID = short("sender.id", in.Sender.ID, 128, true)
	in.Sender.Name = short("sender.name", in.Sender.Name, 256, false)
	in.Chat.ID = short("chat.id", in.Chat.ID, 128, true)
	if in.ReplyTo != nil {
		id := short("reply_to", *in.ReplyTo, 128, true)
		in.ReplyTo = &id
	}
	if in.Action != nil {
		action := *in.Action
		action.DeliveryID = short("action.delivery_id", action.DeliveryID, 128, true)
		action.ButtonID = short("action.button_id", action.ButtonID, 64, true)
		in.Action = &action
	}
	if !validInboundTime(in.Time) {
		in.Time = received.UTC().Format(time.RFC3339Nano)
		fixes = append(fixes, "time: used receive time")
	}
	if text := strings.ToValidUTF8(in.Text, "�"); text != in.Text {
		in.Text = text
		fixes = append(fixes, "text: replaced invalid UTF-8")
	}
	// Message text may keep line breaks and tabs; the protocol refuses every other control character.
	if strings.IndexFunc(in.Text, textControl) >= 0 {
		in.Text = strings.Map(func(r rune) rune {
			if textControl(r) {
				return utf8.RuneError
			}
			return r
		}, in.Text)
		fixes = append(fixes, "text: replaced control characters")
	}
	if in.Text == "" && in.Action == nil {
		in.Text = "[message with no text]"
		fixes = append(fixes, "text: supplied missing text")
	}
	var suffix string
	if extra := len(in.Attachments) - p.MaxAttachments; extra > 0 {
		suffix = fmt.Sprintf("\n[%d more attachments not shown]", extra)
		in.Attachments = in.Attachments[:p.MaxAttachments]
		fixes = append(fixes, fmt.Sprintf("attachments: omitted %d", extra))
	}
	in.Attachments = slices.Clone(in.Attachments)
	for i := range in.Attachments {
		a := &in.Attachments[i]
		field := fmt.Sprintf("attachments[%d].", i)
		a.Name = short(field+"name", a.Name, 256, false)
		a.MIME = short(field+"mime", a.MIME, 128, false)
		a.Ref = short(field+"ref", a.Ref, 128, true)
	}
	if len(in.Text) > p.MaxEventText-len(suffix) {
		suffix = "[message cut]" + suffix
		in.Text = cutBytes(in.Text, p.MaxEventText-len(suffix))
		fixes = append(fixes, "text: shortened to byte limit")
	}
	in.Text += suffix
	return in, fixes
}

func textControl(r rune) bool {
	return unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t'
}

func cutBytes(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}

// The protocol accepts RFC 3339 leap seconds and rejects some offsets Go parses.
// Isolate the timestamp so another invalid field cannot mask the result.
func validInboundTime(stamp string) bool {
	probe := Inbound{V: 1, Seq: 1, Type: p.TypeInbound, ID: "m", Time: stamp,
		Sender: p.Sender{ID: "s"}, Chat: p.Chat{ID: "c"}, Text: "x"}
	line, _ := json.Marshal(probe)
	_, err := p.ParsePluginLineV2(line, p.KindChannel)
	return err == nil
}
