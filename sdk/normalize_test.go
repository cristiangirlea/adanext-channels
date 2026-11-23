package sdk

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func TestNormalizeFields(t *testing.T) {
	long := strings.Repeat("a", 129) + "\n"
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(long)))
	button := strings.Repeat("b", 65)
	msg := inbound("m")
	msg.Time = "2016-12-31t23:59:60z"
	msg.Text = ""
	msg.Sender.ID, msg.Chat.ID, msg.ReplyTo = long, long, &long
	msg.Action = &p.Action{DeliveryID: long, ButtonID: button}
	msg.Attachments = []p.Attachment{{Ref: long, MIME: "x\ny", Name: strings.Repeat("a", 255) + "é"}}
	got, fixes := Normalize(msg)
	if got.Time != msg.Time || got.Text != "" || got.Sender.ID != hash || got.Chat.ID != hash || *got.ReplyTo != hash || got.Action.DeliveryID != hash || got.Action.ButtonID != fmt.Sprintf("%x", sha256.Sum256([]byte(button))) {
		t.Fatal(got)
	}
	if a := got.Attachments[0]; a.Ref != hash || a.MIME != "x\ufffdy" || a.Name != strings.Repeat("a", 255) {
		t.Fatal(a)
	}
	if msg.Action.ButtonID != button || *msg.ReplyTo != long || msg.Attachments[0].Ref != long || len(fixes) < 8 {
		t.Fatal("input changed or missing fixes", fixes)
	}
	again, notes := Normalize(got)
	if !reflect.DeepEqual(got, again) || len(notes) != 0 {
		t.Fatal("already valid message changed", notes)
	}
}

func TestNormalizeInvalidUTF8AndTextControls(t *testing.T) {
	msg := inbound("m")
	msg.Sender.Name = strings.Repeat("x", 254) + "\xff"
	msg.Text = "line one\nline\ttwo\r\n\x01bell\u0085next\xfe"
	got, fixes := Normalize(msg)
	if !utf8.ValidString(got.Sender.Name) || len(got.Sender.Name) > 256 {
		t.Fatalf("sender name %q (%d bytes)", got.Sender.Name, len(got.Sender.Name))
	}
	want := "line one\nline\ttwo\r\n\ufffdbell\ufffdnext\ufffd"
	if got.Text != want {
		t.Fatalf("text = %q, want %q", got.Text, want)
	}
	for _, note := range []string{"sender.name: replaced invalid UTF-8", "text: replaced invalid UTF-8", "text: replaced control characters"} {
		if !slices.Contains(fixes, note) {
			t.Errorf("missing fix %q in %q", note, fixes)
		}
	}
	line, err := json.Marshal(func() Inbound { got.V, got.Seq, got.Type = 1, 1, p.TypeInbound; return got }())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.ParsePluginLineV2(line, p.KindChannel); err != nil {
		t.Fatalf("normalised message still refused: %v", err)
	}
}
