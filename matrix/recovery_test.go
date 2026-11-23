package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func TestRoomFailuresAndMalformedEvents(t *testing.T) {
	for _, fail := range []string{"invite", "invite502", "members", "notice", "malformed"} {
		t.Run(fail, func(t *testing.T) {
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				switch {
				case path == "sync":
					if r.URL.Query().Get("since") == "" {
						io.WriteString(w, batch("base", room))
						return true
					}
					invite := ""
					if strings.HasPrefix(fail, "invite") {
						invite = fmt.Sprintf(`"invite":{"!gone":{"invite_state":{"events":[{"type":"m.room.member","state_key":%q,"content":{"membership":"invite","is_direct":true}}]}}},`, bot)
					}
					bad := message("$gone", "gone")
					if fail == "notice" {
						bad = event("$enc", "m.room.encrypted", user, "{}")
					}
					if fail == "malformed" {
						bad = event("$bad", "m.room.message", user, `{"msgtype":"m.text","body":[]}`)
					}
					fmt.Fprintf(w, `{"next_batch":"next","rooms":{%s"join":{"!a":{"timeline":{"events":[%s]}},"!z":{"timeline":{"events":[%s]}}}}}`, invite, bad, message("$good", "good"))
				case fail == "invite502" && strings.HasPrefix(path, "join/"):
					w.WriteHeader(502)
				case strings.HasPrefix(path, "join/") || strings.Contains(path, "/send/") || fail == "members" && path == "rooms/!a/joined_members":
					w.WriteHeader(403)
					io.WriteString(w, `{"errcode":"M_FORBIDDEN"}`)
				default:
					return false
				}
				return true
			})
			var log bytes.Buffer
			s := f.service(&log)
			if err := s.connect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := s.syncOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			batch, err := s.syncOnce(t.Context())
			if err != nil || len(batch) == 0 || batch[len(batch)-1].ID != "$good" {
				t.Fatal("healthy room did not advance", err)
			}
			for _, msg := range batch {
				if err := s.Commit(t.Context(), msg.ID); err != nil {
					t.Fatal(err)
				}
			}
			if s.state.Since != "next" || fail == "invite502" && !strings.Contains(log.String(), "502") {
				t.Fatalf("cursor %q, log %q", s.state.Since, log.String())
			}
		})
	}
}

func TestInviteErrorsThatKeepCursor(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		code, want string
	}{
		{"unauthorised", 401, "M_USER_LOCKED", p.CodeUnauthorised},
		{"rate limit", 429, "M_LIMIT_EXCEEDED", p.CodeRateLimited},
		{"cancelled", 0, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				switch path {
				case "sync":
					fmt.Fprintf(w, `{"next_batch":"next","rooms":{"invite":{"!pending":{"invite_state":{"events":[{"type":"m.room.member","state_key":%q,"content":{"membership":"invite","is_direct":true}}]}}}}}`, bot)
				case "join/!pending":
					if tc.status == 0 {
						cancel()
						w.WriteHeader(502)
						return true
					}
					w.WriteHeader(tc.status)
					fmt.Fprintf(w, `{"errcode":%q}`, tc.code)
				default:
					return false
				}
				return true
			})
			s := f.service(io.Discard)
			if err := s.connect(ctx); err != nil {
				t.Fatal(err)
			}
			_, err := s.syncOnce(ctx)
			var failure *sdk.DeliveryError
			if tc.status == 0 {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
			} else if !errors.As(err, &failure) || failure.Code != tc.want {
				t.Fatalf("expected %s, got %v", tc.want, err)
			}
			if s.state.Since != "" {
				t.Fatal("advanced past failed invite")
			}
			select {
			case <-s.ready:
				t.Fatal("deliveries opened before the first sync completed")
			default:
			}
		})
	}
}

func TestInviteJoinRetriesAfterLostConnection(t *testing.T) {
	var joins atomic.Int32
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch path {
		case "sync":
			if r.URL.Query().Get("since") != "base" {
				return false
			}
			fmt.Fprintf(w, `{"next_batch":"joined","rooms":{"invite":{"!pending":{"invite_state":{"events":[{"type":"m.room.member","state_key":%q,"content":{"membership":"invite","is_direct":true}}]}}}}}`, bot)
		case "join/!pending":
			if joins.Add(1) == 1 {
				panic(http.ErrAbortHandler) // Close the connection without sending headers.
			}
			io.WriteString(w, `{"room_id":"!pending"}`)
		default:
			return false
		}
		return true
	})
	s := f.startDelivery()
	_, err := s.syncOnce(t.Context())
	var failure *sdk.DeliveryError
	if !errors.As(err, &failure) || failure.Code != p.CodeInternal || s.state.Since != "base" {
		t.Fatalf("lost join response: cursor %q, error %v", s.state.Since, err)
	}
	if _, err := s.syncOnce(t.Context()); err != nil || joins.Load() != 2 || s.state.Since != "joined" {
		t.Fatalf("retry: %d joins, cursor %q, error %v", joins.Load(), s.state.Since, err)
	}
}

func TestLimitedTimelineMembership(t *testing.T) {
	left := `{"type":"m.room.member","state_key":"@third:test","content":{"membership":"leave"},"unsigned":{"prev_content":{"membership":"join"}}}`
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch {
		case path == "sync":
			if r.URL.Query().Get("since") == "" {
				io.WriteString(w, batch("base", room))
				return true
			}
			fmt.Fprintf(w, `{"next_batch":"next","rooms":{"join":{%q:{"timeline":{"limited":true,"prev_batch":"edge","events":[%s,%s]}}}}}`, room, left, message("$private", "dm"))
		case strings.HasSuffix(path, "/messages"):
			q := r.URL.Query()
			if q.Get("from") != "edge" || q.Get("to") != "base" || q.Get("dir") != "b" {
				t.Error("wrong gap bounds")
			}
			fmt.Fprintf(w, `{"chunk":[%s],"end":"base"}`, message("$group", "before third member left"))
		default:
			return false
		}
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
	if err != nil || len(msgs) != 1 || msgs[0].ID != "$private" {
		t.Fatal("incorrect messages across membership boundary", len(msgs), err)
	}
}

func TestChoiceReplayAfterExpiry(t *testing.T) {
	start := time.Now().Truncate(time.Millisecond)
	var ticks int
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch {
		case path == "sync":
			if r.URL.Query().Get("since") == "" {
				io.WriteString(w, batch("base", room))
				return true
			}
			io.WriteString(w, batch("next", room, event("$ok", "m.room.message", bot, "{}"), strings.Replace(message("$fast", "1"), "1700000000123", strconv.FormatInt(start.Add(time.Second).UnixMilli(), 10), 1)))
		case strings.Contains(path, "/send/"):
			ticks = 2
			io.WriteString(w, `{"event_id":"$ok"}`)
		default:
			return false
		}
		return true
	})
	s := f.service(io.Discard)
	s.now = func() time.Time { return start.Add(time.Duration(ticks) * time.Second) }
	if err := s.connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.syncOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deliver(t.Context(), p.Deliver{ID: "d", ChatID: room, Text: "?", Buttons: []p.Button{{ID: "yes", Label: "Yes"}}}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if i == 1 {
			s = f.service(io.Discard)
			s.now = func() time.Time { return start.Add(time.Hour) }
			if err := s.connect(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		batch, err := s.syncOnce(t.Context())
		if err != nil || len(batch) != 1 || batch[0].Action == nil || *batch[0].Action != (p.Action{DeliveryID: "d", ButtonID: "yes"}) {
			t.Fatal("fast reply or replay lost its action", i, err)
		}
	}
}

func TestChoiceRetryAfterCheckpointedEcho(t *testing.T) {
	var sends atomic.Int32
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch path {
		case "rooms/" + room + "/send/m.room.message/prompt-0":
			sends.Add(1)
			io.WriteString(w, `{"event_id":"$prompt"}`)
		case "sync":
			switch r.URL.Query().Get("since") {
			case "base":
				echoes := []string{event("$older", "m.room.message", bot, "{}"), event("$prompt", "m.room.message", bot, "{}")}
				for i := range 63 {
					echoes = append(echoes, event(fmt.Sprintf("$later%d", i), "m.room.message", bot, "{}"))
				}
				io.WriteString(w, batch("echo", room, echoes...))
			case "echo":
				io.WriteString(w, batch("idle", room))
			case "idle":
				io.WriteString(w, batch("done", room, message("$choice", "1")))
			default:
				return false
			}
		default:
			return false
		}
		return true
	})
	s := f.startDelivery()
	d := p.Deliver{ID: "prompt", ChatID: room, Text: "OK?", Buttons: []p.Button{{ID: "yes", Label: "Yes"}}}
	path := s.statePath
	s.statePath = s.dir // Replacing a directory with the checkpoint file fails.
	if _, err := s.Deliver(t.Context(), d); err == nil || sends.Load() != 1 {
		t.Fatalf("expected checkpoint failure after first send, got %v", err)
	}
	s.statePath = path
	if _, err := s.syncOnce(t.Context()); err != nil || s.state.Since != "echo" {
		t.Fatalf("checkpoint echo: cursor %q, error %v", s.state.Since, err)
	}
	s = f.startDelivery() // Recover the echo from disk before retrying the same transaction.
	if ids := s.state.OwnEvents[room]; len(ids) != 64 || ids[0] != "$prompt" {
		t.Fatalf("unexpected echo history after restart: %v", ids)
	}
	if id, err := s.Deliver(t.Context(), d); err != nil || id != "$prompt" || sends.Load() != 2 {
		t.Fatalf("retry: event %q, sends %d, error %v", id, sends.Load(), err)
	}
	msgs, err := s.syncOnce(t.Context())
	want := p.Action{DeliveryID: d.ID, ButtonID: "yes"}
	if err != nil || len(msgs) != 1 || msgs[0].Action == nil || *msgs[0].Action != want {
		t.Fatalf("expected action after retry, got %+v, error %v", msgs, err)
	}
}

func TestLimitedTimelineOrder(t *testing.T) {
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch {
		case path == "sync":
			if r.URL.Query().Get("since") == "" {
				io.WriteString(w, batch("base", room))
				return true
			}
			fmt.Fprintf(w, `{"next_batch":"next","rooms":{"join":{%q:{"timeline":{"limited":true,"prev_batch":"edge","events":[%s,%s]}}}}}`, room, message("$3", "three"), message("$4", "four"))
		case strings.HasSuffix(path, "/messages"):
			if r.URL.Query().Get("from") == "edge" {
				fmt.Fprintf(w, `{"end":"older","chunk":[%s,%s]}`, message("$3", "three"), message("$2", "two"))
			} else {
				fmt.Fprintf(w, `{"end":"base","chunk":[%s]}`, message("$1", "one"))
			}
		default:
			return false
		}
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
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	if !slices.Equal(ids, []string{"$1", "$2", "$3", "$4"}) {
		t.Fatal("wrong gap order", ids)
	}
}

func TestMembershipOnSyncRetry(t *testing.T) {
	lookups, failures := 0, 0
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch path {
		case "sync":
			switch r.URL.Query().Get("since") {
			case "":
				io.WriteString(w, batch("initial", room))
			case "initial":
				io.WriteString(w, batch("before", "!a", message("$group0", "earlier group message")))
			default:
				fmt.Fprintf(w, `{"next_batch":"after","rooms":{"join":{"!a":{"timeline":{"events":[%s,{"type":"m.room.member","state_key":"@third:test","content":{"membership":"leave"}},%s]}},"!b":{"timeline":{"events":[%s]}}}}}`, message("$group", "group message before leave"), message("$dm", "private message after leave"), message("$other", "other room"))
			}
		case "rooms/!a/joined_members":
			extra := ""
			if lookups == 0 {
				extra = `,"@third:test":{}`
			}
			lookups++
			fmt.Fprintf(w, `{"joined":{%q:{},%q:{}%s}}`, bot, user, extra)
		case "rooms/!b/joined_members":
			if failures == 0 {
				failures++
				w.WriteHeader(502)
				return true
			}
			return false
		default:
			return false
		}
		return true
	})
	s := f.service(io.Discard)
	if err := s.connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := s.syncOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.syncOnce(t.Context()); err == nil {
		t.Fatal("missing transient failure")
	}
	msgs, err := s.syncOnce(t.Context())
	if err != nil || len(msgs) != 2 || msgs[0].ID != "$dm" || msgs[1].ID != "$other" {
		t.Fatal("retry exposed pre-leave group text", len(msgs), err)
	}
}

func TestGapLimit(t *testing.T) {
	var pages int
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		if !strings.HasSuffix(path, "/messages") {
			return false
		}
		pages++
		var chunk []string
		switch r.URL.Query().Get("from") {
		case "edge", "middle":
			start := 1100
			wantLimit := 1000
			if pages == 2 {
				wantLimit = 400
			}
			if r.URL.Query().Get("limit") != strconv.Itoa(wantLimit) {
				t.Errorf("page %d limit = %s", pages, r.URL.Query().Get("limit"))
			}
			if pages == 2 {
				start = 500
			}
			for i := range 600 {
				chunk = append(chunk, message(fmt.Sprintf("$%d", start-i), "hi"))
			}
			end := "middle"
			if pages == 2 {
				end = "older"
			}
			fmt.Fprintf(w, `{"end":%q,"chunk":[%s]}`, end, strings.Join(chunk, ","))
		default:
			io.WriteString(w, `{"end":"base","chunk":[]}`)
		}
		return true
	})
	var log bytes.Buffer
	s := f.service(&log)
	msgs, err := s.gap(t.Context(), room, "edge", "base")
	if err != nil || pages != 2 || len(msgs) != 1000 || msgs[0].ID != "$101" || msgs[999].ID != "$1100" {
		t.Fatalf("read %d events in %d pages: %v", len(msgs), pages, err)
	}
	if !strings.Contains(log.String(), "200") || !strings.Contains(log.String(), room) {
		t.Fatalf("missing dropped count and room in %q", log.String())
	}
}

func TestChoiceRetryWhileEchoBatchUncommitted(t *testing.T) {
	var sends atomic.Int32
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		switch path {
		case "rooms/" + room + "/send/m.room.message/prompt-0":
			sends.Add(1)
			io.WriteString(w, `{"event_id":"$prompt"}`)
		case "sync":
			switch r.URL.Query().Get("since") {
			case "base":
				// A "1" before the echo, the echo, then a message that keeps the batch uncommitted.
				io.WriteString(w, batch("echo", room, message("$early", "1"), event("$prompt", "m.room.message", bot, "{}"), message("$mid", "hello")))
			case "echo":
				io.WriteString(w, batch("done", room, message("$choice", "1")))
			default:
				return false
			}
		default:
			return false
		}
		return true
	})
	s := f.startDelivery()
	d := p.Deliver{ID: "prompt", ChatID: room, Text: "OK?", Buttons: []p.Button{{ID: "yes", Label: "Yes"}}}
	path := s.statePath
	s.statePath = s.dir // Replacing a directory with the checkpoint file fails.
	if _, err := s.Deliver(t.Context(), d); err == nil || sends.Load() != 1 {
		t.Fatalf("expected checkpoint failure after first send, got %v", err)
	}
	s.statePath = path
	msgs, err := s.syncOnce(t.Context())
	if err != nil || len(msgs) != 2 || msgs[0].Action != nil || msgs[0].Text != "1" || s.state.Since != "base" {
		t.Fatalf("echo batch: %+v, cursor %q, error %v", msgs, s.state.Since, err)
	}
	// The retry lands while the batch holding the echo is still uncommitted.
	if id, err := s.Deliver(t.Context(), d); err != nil || id != "$prompt" || sends.Load() != 2 {
		t.Fatalf("retry: event %q, sends %d, error %v", id, sends.Load(), err)
	}
	for _, m := range msgs {
		if err := s.Commit(t.Context(), m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if s.state.Since != "echo" {
		t.Fatalf("cursor after commits = %q", s.state.Since)
	}
	next, err := s.syncOnce(t.Context())
	want := p.Action{DeliveryID: d.ID, ButtonID: "yes"}
	if err != nil || len(next) != 1 || next[0].Action == nil || *next[0].Action != want {
		t.Fatalf("expected action after the batch committed, got %+v, error %v", next, err)
	}
}
