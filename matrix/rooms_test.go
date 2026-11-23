package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func TestInvitesAndMembership(t *testing.T) {
	for _, groups := range []bool{false, true} {
		t.Run(fmt.Sprint("groups=", groups), func(t *testing.T) {
			var joined []string
			polls, members, notices := 0, 0, 0
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				switch {
				case path == "sync":
					polls++
					switch polls {
					case 1:
						invite := func(id string, direct bool) string {
							return fmt.Sprintf(`{"invite_state":{"events":[{"type":"m.room.member","state_key":%q,"sender":%q,"content":{"membership":"invite","is_direct":%t}}]}}`, id, user, direct)
						}
						fmt.Fprintf(w, `{"next_batch":"base","rooms":{"invite":{"!dm":%s,"!group":%s,"!other":%s}}}`, invite(bot, true), invite(bot, false), invite(user, true))
					case 2, 3:
						io.WriteString(w, batch(strconv.Itoa(polls), room, event("$enc", "m.room.encrypted", user, "{}"), message(fmt.Sprint("$", polls), "hi")))
					case 4:
						io.WriteString(w, batch("group", room, event("$member", "m.room.member", user, `{"membership":"join"}`), message("$group", "hi")))
					}
				case strings.HasSuffix(path, "/joined_members"):
					members++
					extra := ""
					if members > 1 {
						extra = `,"@third:test":{}`
					}
					fmt.Fprintf(w, `{"joined":{%q:{},%q:{}%s}}`, bot, user, extra)
				case strings.HasPrefix(path, "join/") || strings.HasSuffix(path, "/join"):
					if r.Method != "POST" {
						t.Error("join method")
					}
					joined = append(joined, path)
					io.WriteString(w, `{"room_id":"!joined"}`)
				case strings.Contains(path, "/send/"):
					var b struct{ Msgtype, Body string }
					json.NewDecoder(r.Body).Decode(&b)
					if b.Msgtype != "m.notice" || !strings.Contains(b.Body, "encrypted") {
						t.Error("incorrect encryption notice")
					}
					notices++
					io.WriteString(w, `{"event_id":"$notice"}`)
				default:
					return false
				}
				return true
			})
			f.groups = groups
			s := f.service(io.Discard)
			if err := s.connect(t.Context()); err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= 4; i++ {
				batch, err := s.syncOnce(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				want := i == 2 || i == 3 || i == 4 && groups
				if want != (len(batch) == 1) || !want && len(batch) != 0 {
					t.Fatalf("poll %d produced %d messages", i, len(batch))
				}
				if want {
					kind := "private"
					if i == 4 {
						kind = "group"
					}
					if batch[0].Chat.Type == nil || *batch[0].Chat.Type != kind {
						t.Error("incorrect chat type")
					}
					if err := s.Commit(t.Context(), batch[0].ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			paths := strings.Join(joined, " ")
			if members != 2 || notices != 1 || !strings.Contains(paths, "!dm") || strings.Contains(paths, "!group") != groups || strings.Contains(paths, "!other") {
				t.Fatalf("members %d notices %d joins %v", members, notices, joined)
			}
		})
	}
}

func TestInboundMediaAndNotice(t *testing.T) {
	var events []string
	for _, typ := range []string{"m.image", "m.file", "m.video", "m.audio"} {
		events = append(events, event("$"+typ, "m.room.message", user, fmt.Sprintf(`{"msgtype":%q,"body":"sample","url":"mxc://test/abc","info":{"mimetype":"application/test","size":23}}`, typ)))
	}
	events = append(events, event("$notice", "m.room.message", user, `{"msgtype":"m.notice","body":"notice"}`))
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		if path != "sync" {
			return false
		}
		if r.URL.Query().Get("since") == "" {
			io.WriteString(w, batch("initial", room))
			return true
		}
		io.WriteString(w, batch("next", room, events...))
		return true
	})
	s := f.service(io.Discard)
	if err := s.connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.syncOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	msgs, err := s.syncOnce(t.Context())
	if err != nil || len(msgs) != 5 {
		t.Fatal(len(msgs), err)
	}
	for _, m := range msgs[:4] {
		if !slices.Equal(m.Attachments, []p.Attachment{{Name: "sample", MIME: "application/test", Size: 23, Ref: "mxc://test/abc"}}) {
			t.Errorf("incorrect attachment for %s", m.ID)
		}
	}
	if msgs[4].Text != "notice" {
		t.Error("missing notice")
	}
}

func TestRoomChoices(t *testing.T) {
	target, body := room, "2"
	var seq int
	echoes := map[string][]string{}
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch {
		case path == "sync":
			seq++
			events := append(echoes[target], message(fmt.Sprint("$", seq), body))
			delete(echoes, target)
			io.WriteString(w, batch(strconv.Itoa(seq), target, events...))
			return true
		case strings.Contains(path, "/send/"):
			chat, txn, _ := strings.Cut(strings.TrimPrefix(path, "rooms/"), "/send/m.room.message/")
			echoes[chat] = append(echoes[chat], event("$"+txn, "m.room.message", bot, "{}"))
			fmt.Fprintf(w, `{"event_id":%q}`, "$"+txn)
			return true
		}
		return false
	})
	s := f.service(io.Discard)
	if err := s.connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.syncOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.now = func() time.Time { return now }
	d := p.Deliver{ID: "prompt", ChatID: room, Text: "OK?", Buttons: []p.Button{{ID: "ok", Label: "Approve"}, {ID: "no", Label: "Deny"}}}
	send := func() {
		t.Helper()
		if _, err := s.Deliver(t.Context(), d); err != nil {
			t.Fatal(err)
		}
	}
	check := func(chat, text, delivery string) {
		t.Helper()
		target, body = chat, text
		msgs, err := s.syncOnce(t.Context())
		if err != nil || len(msgs) != 1 {
			t.Fatal("missing reply", err)
		}
		m := msgs[0]
		if delivery == "" {
			if m.Action != nil || m.Text != text {
				t.Fatal("unexpected action")
			}
		} else if m.Text != "" || m.Action == nil || *m.Action != (p.Action{DeliveryID: delivery, ButtonID: "no"}) {
			t.Fatal("incorrect action")
		}
		if err := s.Commit(t.Context(), m.ID); err != nil {
			t.Fatal(err)
		}
	}
	send()
	check("!other:test", "2", "")
	check(room, " 2", "")
	check(room, "02", "")
	check(room, "3", "")
	d.ID = "new"
	send()
	d.ID = "prompt"
	send()
	check(room, "2", "new")
	d.ID = "new"
	send()
	check(room, "2", "")
	d.ID = "timed"
	send()
	now = now.Add(10 * time.Minute)
	check(room, "2", "")
	now = time.Now()
	for i := range 33 {
		d.ChatID = fmt.Sprint("!room", i)
		d.ID = fmt.Sprint("p", i)
		send()
	}
	check("!room0", "2", "")
	check("!room1", "2", "p1")
}

func TestGroupChoiceTimelineAndReplay(t *testing.T) {
	const other = "@other:test"
	reply := func(id, sender, ts string) string {
		return strings.Replace(event(id, "m.room.message", sender, `{"msgtype":"m.text","body":"1"}`), "1700000000123", ts, 1)
	}
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch {
		case path == "sync":
			switch r.URL.Query().Get("since") {
			case "":
				io.WriteString(w, batch("base", room))
			case "base":
				io.WriteString(w, batch("before", room, event("$first", "m.room.message", bot, "{}"), reply("$zero", user, "0")))
			case "before":
				io.WriteString(w, batch("armed", room, reply("$early", other, "9999999999999"), event("$last", "m.room.message", bot, "{}"), reply("$one", user, "1"), reply("$repeat", user, "1")))
			case "armed":
				io.WriteString(w, batch("done", room, reply("$two", other, "1"), reply("$again", user, "1")))
			}
		case strings.HasSuffix(path, "/joined_members"):
			fmt.Fprintf(w, `{"joined":{%q:{},%q:{},%q:{}}}`, bot, user, other)
		case strings.HasSuffix(path, "/prompt-0"):
			io.WriteString(w, `{"event_id":"$first"}`)
		case strings.HasSuffix(path, "/prompt-1"):
			io.WriteString(w, `{"event_id":"$last"}`)
		default:
			return false
		}
		return true
	})
	f.groups = true
	start := func() *service {
		t.Helper()
		s := f.service(io.Discard)
		if err := s.connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := start()
	if _, err := s.syncOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	d := p.Deliver{ID: "prompt", ChatID: room, Text: strings.Repeat("x", 30000), Buttons: []p.Button{{ID: "yes", Label: "Yes"}}}
	if _, err := s.Deliver(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	check := func(commit bool, ids ...string) {
		t.Helper()
		msgs, err := s.syncOnce(t.Context())
		if err != nil || len(msgs) != len(ids) {
			t.Fatalf("got %d replies: %v", len(msgs), err)
		}
		for i, m := range msgs {
			if m.ID != ids[i] {
				t.Fatalf("got %q want %q", m.ID, ids[i])
			}
			if m.ID == "$one" || m.ID == "$two" {
				if m.Action == nil || *m.Action != (p.Action{DeliveryID: "prompt", ButtonID: "yes"}) || m.Text != "" {
					t.Fatalf("%s lost action", m.ID)
				}
			} else if m.Action != nil || m.Text != "1" {
				t.Errorf("%s should be plain text", m.ID)
			}
			if commit {
				if err := s.Commit(t.Context(), m.ID); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	check(true, "$zero")
	check(false, "$early", "$one", "$repeat")
	s = start()
	check(true, "$early", "$one", "$repeat")
	s = start()
	check(true, "$two", "$again")
}

func TestEncryptedRoomDelivery(t *testing.T) {
	for _, location := range []string{"initial state", "timeline"} {
		t.Run(location, func(t *testing.T) {
			var sends int
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				switch {
				case path == "sync":
					if r.URL.Query().Get("since") == "next" || location == "initial state" && r.URL.Query().Get("since") == "base" {
						io.WriteString(w, batch("idle", room))
						return true
					}
					encryption := `{"type":"m.room.encryption","state_key":"","content":{"algorithm":"m.megolm.v1.aes-sha2"}}`
					if location == "initial state" {
						fmt.Fprintf(w, `{"next_batch":"base","rooms":{"join":{%q:{"state":{"events":[%s]}}}}}`, room, encryption)
					} else if r.URL.Query().Get("since") == "" {
						io.WriteString(w, batch("base", room))
					} else {
						io.WriteString(w, batch("next", room, encryption))
					}
				case strings.Contains(path, "/send/"):
					sends++
					io.WriteString(w, `{"event_id":"$leak"}`)
				default:
					return false
				}
				return true
			})
			s := f.service(io.Discard)
			if err := s.connect(t.Context()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			_, err := s.Deliver(ctx, p.Deliver{ID: "early", ChatID: room, Text: "private"})
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || sends != 0 {
				t.Fatalf("delivery before first sync: %v, sends %d", err, sends)
			}
			if _, err := s.syncOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			if location == "timeline" {
				if _, err := s.syncOnce(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			for i := range 2 {
				if i == 1 {
					s = f.service(io.Discard)
					if err := s.connect(t.Context()); err != nil {
						t.Fatal(err)
					}
					if _, err := s.syncOnce(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				_, err := s.Deliver(t.Context(), p.Deliver{ID: "d", ChatID: room, Text: "private"})
				var failure *sdk.DeliveryError
				if !errors.As(err, &failure) || failure.Code != p.CodeBadRequest || failure.Message != "Ada cannot write to an encrypted room" || sends != 0 {
					t.Fatalf("sends %d, error %v", sends, err)
				}
			}
		})
	}
}

func TestEncryptionNoticeWindow(t *testing.T) {
	var sends int
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		if !strings.Contains(path, "/send/") {
			return false
		}
		sends++
		io.WriteString(w, `{"event_id":"$notice"}`)
		return true
	})
	s := f.service(io.Discard)
	if err := s.connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range 1024 {
		s.state.Noticed = append(s.state.Noticed, fmt.Sprint("!", i))
	}
	for _, id := range []string{"!1024", "!1024", "!1"} {
		if err := s.encryptedNotice(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	if sends != 1 || len(s.state.Noticed) != 1024 || s.state.Noticed[0] != "!1" || s.state.Noticed[1023] != "!1024" {
		t.Fatal("incorrect notice eviction or duplicate send")
	}
}
