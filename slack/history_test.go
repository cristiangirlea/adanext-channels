package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func TestChannelHistoryFiltersMentions(t *testing.T) {
	var calls atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			fmt.Fprint(w, `{"ok":true,"user":"ada","user_id":"UADA","bot_id":"BADA"}`)
		case "/conversations.history":
			if r.URL.Query().Get("channel") != "C123" {
				t.Error("read an unlisted channel")
			}
			switch calls.Add(1) {
			case 1:
				w.WriteHeader(429)
				return
			case 2:
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			fmt.Fprint(w, `{"ok":true,"messages":[
 {"type":"message","ts":"1700000000.000005","thread_ts":"1700000000.000001","user":"U1","text":"<@UADA> please"},
 {"type":"message","ts":"1700000000.000005","thread_ts":"1700000000.000001","user":"U1","text":"<@UADA> please"},
 {"type":"message","ts":"1700000000.000004","user":"U1","text":"<@UOTHER> hey"},
 {"type":"message","ts":"1700000000.000003","user":"U2","bot_id":"B2","text":"<@UADA> hi"}
 ]}`)
		case "/users.info":
			fmt.Fprint(w, `{"ok":false,"error":"user_not_found"}`)
		case "/chat.postMessage":
			var post outgoing
			if json.NewDecoder(r.Body).Decode(&post) != nil || post.Thread != "1700000000.000001" {
				t.Error("reply to recovered mention missed its parent thread")
			}
			fmt.Fprint(w, `{"ok":true,"ts":"1700000001.000001"}`)
		default:
			t.Error("unexpected " + r.URL.Path)
		}
	})
	s := f.service()
	for _, chat := range []string{"C123", "C999"} {
		if err := s.save(chat, "1700000000.000001"); err != nil {
			t.Fatal(err)
		}
	}
	s.stop()
	core := f.start()
	peer := f.peer()
	event(t, peer, "dm", "E1", dm)
	msg := core.NextInbound()
	if msg.ID != "C123:1700000000.000005" || *msg.Chat.Type != "group" || msg.Text != "<@UADA> please" {
		t.Fatal("wrong recovered mention", msg.ID)
	}
	core.Ack(msg.ID)
	if _, failed := core.Deliver(p.Deliver{ID: "reply", ChatID: msg.Chat.ID, ReplyTo: &msg.ID, Text: "reply"}); failed != nil {
		t.Fatal(failed)
	}
	live := core.NextInbound()
	if live.Chat.ID != "D123" {
		t.Fatal("unexpected duplicate or non-mention")
	}
	core.Ack(live.ID)
	ack(t, peer, "dm")
	if calls.Load() != 3 {
		t.Errorf("history calls %d, want 3", calls.Load())
	}
}

func TestMissingHistoryScopeAllowsLiveDM(t *testing.T) {
	var pages atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			fmt.Fprint(w, `{"ok":true,"user_id":"UADA"}`)
		case "/conversations.history":
			if r.URL.Query().Get("channel") != "C123" {
				t.Error("unexpected history channel")
			}
			pages.Add(1)
			fmt.Fprint(w, `{"ok":false,"error":"missing_scope","needed":"private-scope-detail"}`)
		case "/users.info":
			fmt.Fprint(w, `{"ok":true,"user":{"name":"tester"}}`)
		default:
			t.Error("unexpected " + r.URL.Path)
		}
	})
	s := f.service()
	if err := s.save("C123", "1700000000.000001"); err != nil {
		t.Fatal(err)
	}
	s.stop()
	core := f.start()
	peer := f.peer()
	event(t, peer, "live", "E1", dm)
	in := core.NextInbound()
	if in.ID != "D123:1700000000.000002" {
		t.Fatalf("unexpected live DM %q", in.ID)
	}
	core.Ack(in.ID)
	ack(t, peer, "live")
	logs := f.logs.String()
	if pages.Load() != 1 || !strings.Contains(logs, "missing_scope") || strings.Contains(logs, "private-scope-detail") {
		t.Fatalf("history requests %d, logs %q", pages.Load(), logs)
	}
}

func TestHistoryRetryBudget(t *testing.T) {
	var calls atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			panic(http.ErrAbortHandler) // connection closed before a response
		case 2:
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		case 3:
			w.Header().Set("Content-Length", "100")
			fmt.Fprint(w, `{"ok":`) // connection closed during the response
		case 4:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			t.Error("retried beyond the history request budget")
			fmt.Fprint(w, `{"ok":true}`)
		}
	})
	s := f.service()
	defer s.stop()
	var waits []time.Duration
	s.backoff.Sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	s.positions["D123"] = "1700000000.000001"
	got, err := s.catchUp(t.Context())
	if err != nil || len(got) != 0 || calls.Load() != 4 || !slices.Equal(waits, []time.Duration{time.Millisecond, 7 * time.Second, 4 * time.Millisecond}) {
		t.Fatalf("catch-up got %v, %v after %d requests and waits %v", got, err, calls.Load(), waits)
	}
}

func TestHistoryRetryAfterIsCapped(t *testing.T) {
	var calls atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `{"ok":true,"messages":[],"has_more":false}`)
	})
	s := f.service()
	defer s.stop()
	var waits []time.Duration
	s.backoff.Sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	s.positions["D123"] = "1700000000.000001"
	if _, err := s.catchUp(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(waits, []time.Duration{historyMaxWait}) {
		t.Fatalf("waits = %v, want one capped wait of %s", waits, historyMaxWait)
	}
}

func TestFailedBotIdentitySkipsChannelsForThePass(t *testing.T) {
	var auth, history atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			auth.Add(1)
			fmt.Fprint(w, `{"ok":false,"error":"invalid_auth"}`)
		case "/conversations.history":
			history.Add(1)
			fmt.Fprint(w, `{"ok":true,"messages":[],"has_more":false}`)
		default:
			fmt.Fprint(w, `{"ok":true}`)
		}
	})
	s := f.service()
	defer s.stop()
	s.allowed = map[string]bool{"C111": true, "C222": true}
	s.positions["C111"] = "1700000000.000001"
	s.positions["C222"] = "1700000000.000001"
	s.positions["D123"] = "1700000000.000001"
	if _, err := s.catchUp(t.Context()); err != nil {
		t.Fatal(err)
	}
	if auth.Load() != 1 || history.Load() != 1 {
		t.Fatalf("auth.test calls = %d (want 1), history calls = %d (want 1, the direct message only)", auth.Load(), history.Load())
	}
}

func TestCatchUpBudgetKeepsCursors(t *testing.T) {
	old := catchUpBudget
	catchUpBudget = 50 * time.Millisecond
	defer func() { catchUpBudget = old }()
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	s := f.service()
	defer s.stop()
	s.positions["D111"] = "1700000000.000001"
	s.positions["D222"] = "1700000000.000002"
	start := time.Now()
	got, err := s.catchUp(t.Context())
	if err != nil || len(got) != 0 {
		t.Fatalf("catch-up = %v, %v", got, err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("catch-up took %s, past its budget", took)
	}
	if s.positions["D111"] != "1700000000.000001" || s.positions["D222"] != "1700000000.000002" {
		t.Fatalf("cursors changed: %v", s.positions)
	}
}

func TestInterruptedCatchUpHoldsCursor(t *testing.T) {
	old := catchUpBudget
	catchUpBudget = 100 * time.Millisecond
	defer func() { catchUpBudget = old }()
	var secondPage atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.history":
			if r.URL.Query().Get("cursor") == "" {
				fmt.Fprint(w, `{"ok":true,"has_more":true,"response_metadata":{"next_cursor":"c2"},"messages":[
 {"type":"message","ts":"1700000010.000000","user":"U1","text":"old one"}]}`)
				return
			}
			if secondPage.Add(1) == 1 {
				// The first pass runs out of budget on this page.
				<-r.Context().Done()
				return
			}
			fmt.Fprint(w, `{"ok":true,"has_more":false,"messages":[
 {"type":"message","ts":"1700000020.000000","user":"U1","text":"old two"}]}`)
		case "/users.info":
			fmt.Fprint(w, `{"ok":true,"user":{"name":"jo"}}`)
		default:
			fmt.Fprint(w, `{"ok":true}`)
		}
	})
	s := f.service()
	defer s.stop()
	s.positions["D123"] = "1700000000.000001"
	if got, err := s.catchUp(t.Context()); err != nil || len(got) != 0 || !s.incomplete["D123"] {
		t.Fatalf("first pass = %v, %v, incomplete %v", got, err, s.incomplete)
	}
	// A newer live message is committed while the history gap is still unread.
	s.pending["live"] = incoming{in: sdk.Inbound{ID: "D123:1700000100.000000", Chat: p.Chat{ID: "D123"}}, ts: "1700000100.000000"}
	if err := s.Commit(t.Context(), "live"); err != nil {
		t.Fatal(err)
	}
	if s.positions["D123"] != "1700000000.000001" {
		t.Fatalf("cursor moved past unread history: %s", s.positions["D123"])
	}
	got, err := s.catchUp(t.Context())
	if err != nil || len(got) != 2 || got[0].Text != "old one" || got[1].Text != "old two" || s.incomplete["D123"] {
		t.Fatalf("second pass = %+v, %v, incomplete %v", got, err, s.incomplete)
	}
}

// commitLive commits a newer live message for chat and returns the saved cursor.
func commitLive(t *testing.T, s *service, chat, ts string) string {
	t.Helper()
	id := chat + ":" + ts
	s.pending[id] = incoming{in: sdk.Inbound{ID: id, Chat: p.Chat{ID: chat}}, ts: ts}
	if err := s.Commit(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	return s.positions[chat]
}

func TestCursorHoldByOutcome(t *testing.T) {
	const start = "1700000000.000001"
	for _, tc := range []struct {
		name   string
		reply  func(http.ResponseWriter)
		chat   string
		held   bool
		before func(*service)
	}{
		{"retries exhausted", func(w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) }, "D123", true, nil},
		{"permanent error", func(w http.ResponseWriter) { fmt.Fprint(w, `{"ok":false,"error":"missing_scope"}`) }, "D123", false, nil},
		{"pagination stuck", func(w http.ResponseWriter) {
			fmt.Fprint(w, `{"ok":true,"has_more":true,"messages":[]}`)
		}, "D123", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup(t, func(w http.ResponseWriter, r *http.Request) { tc.reply(w) })
			s := f.service()
			defer s.stop()
			s.backoff.Sleep = func(context.Context, time.Duration) error { return nil }
			s.positions[tc.chat] = start
			if _, err := s.catchUp(t.Context()); err != nil {
				t.Fatalf("catch-up must not fail the channel: %v", err)
			}
			if s.incomplete[tc.chat] != tc.held {
				t.Fatalf("incomplete = %v, want %v", s.incomplete[tc.chat], tc.held)
			}
			got := commitLive(t, s, tc.chat, "1700000100.000000")
			if moved := got != start; moved == tc.held {
				t.Fatalf("cursor = %s (held %v)", got, tc.held)
			}
		})
	}
}

func TestFailedIdentityHoldsChannelsNotDMs(t *testing.T) {
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			fmt.Fprint(w, `{"ok":true,"has_more":false,"messages":[]}`)
		}
	})
	s := f.service()
	defer s.stop()
	s.allowed = map[string]bool{"C111": true}
	s.positions["C111"] = "1700000000.000001"
	s.positions["D123"] = "1700000000.000001"
	if _, err := s.catchUp(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !s.incomplete["C111"] || s.incomplete["D123"] {
		t.Fatalf("incomplete = %v", s.incomplete)
	}
	if commitLive(t, s, "C111", "1700000100.000000") != "1700000000.000001" {
		t.Fatal("channel cursor moved while its history is unread")
	}
	if commitLive(t, s, "D123", "1700000100.000000") != "1700000100.000000" {
		t.Fatal("direct message cursor held although its history was read")
	}
}

func TestSlowConversationDoesNotStarveOthers(t *testing.T) {
	old := catchUpBudget
	catchUpBudget = 50 * time.Millisecond
	defer func() { catchUpBudget = old }()
	var slow atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("channel") == "D111" {
			slow.Add(1)
			<-r.Context().Done() // always runs out of budget
			return
		}
		fmt.Fprint(w, `{"ok":true,"has_more":false,"messages":[]}`)
	})
	s := f.service()
	defer s.stop()
	s.backoff.Sleep = func(context.Context, time.Duration) error { return nil }
	s.positions["D111"] = "1700000000.000001"
	s.positions["D222"] = "1700000000.000001"
	// Pass 1: D111 overruns and D222 is not reached.
	if _, err := s.catchUp(t.Context()); err != nil || !s.incomplete["D111"] || !s.incomplete["D222"] {
		t.Fatalf("pass 1: %v, incomplete %v", err, s.incomplete)
	}
	// Pass 2: D222, left unread, goes first and completes.
	if _, err := s.catchUp(t.Context()); err != nil || s.incomplete["D222"] || !s.incomplete["D111"] {
		t.Fatalf("pass 2: %v, incomplete %v", err, s.incomplete)
	}
	// Pass 3: D111 overruns a third time and its gap is given up.
	if _, err := s.catchUp(t.Context()); err != nil || s.incomplete["D111"] || !s.abandoned["D111"] {
		t.Fatalf("pass 3: %v, incomplete %v, abandoned %v", err, s.incomplete, s.abandoned)
	}
	calls := slow.Load()
	if _, err := s.catchUp(t.Context()); err != nil || slow.Load() != calls {
		t.Fatalf("an abandoned conversation was read again: %v", err)
	}
	if commitLive(t, s, "D111", "1700000100.000000") != "1700000100.000000" {
		t.Fatal("an abandoned conversation still holds its cursor")
	}
}
