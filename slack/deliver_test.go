package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func TestDelivery(t *testing.T) {
	type element struct {
		Type, Style, Value string
		ActionID           string `json:"action_id"`
		Text               struct{ Type, Text string }
	}
	type block struct {
		Type     string
		Text     struct{ Type, Text string }
		Elements []element
	}
	var calls atomic.Int32
	var joined capture
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.postMessage" || r.Method != "POST" {
			t.Error("wrong delivery endpoint")
		}
		var msg struct {
			Channel, Text string
			Thread        string `json:"thread_ts"`
			UnfurlLinks   *bool  `json:"unfurl_links"`
			UnfurlMedia   *bool  `json:"unfurl_media"`
			Blocks        []block
		}
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Error(err)
		}
		if msg.Channel != "D123" || msg.Thread != "1700000000.000002" || msg.UnfurlMedia == nil || *msg.UnfurlMedia || msg.UnfurlLinks == nil || *msg.UnfurlLinks || len(msg.Text) > 3000 || strings.TrimSpace(msg.Text) == "" {
			t.Error("invalid post")
		}
		fmt.Fprint(&joined, msg.Text)
		n := calls.Add(1)
		if n == 1 && len(msg.Blocks) > 0 {
			t.Error("buttons on first part")
		}
		if n == 2 {
			if len(msg.Blocks) != 6 || msg.Blocks[0].Type != "section" || msg.Blocks[0].Text.Type != "plain_text" ||
				msg.Blocks[0].Text.Text != "&<!channel><@U123>&lt;" || msg.Text != "&amp;&lt;!channel&gt;&lt;@U123&gt;&amp;lt;" {
				t.Error("incorrect blocks")
			} else {
				for i, row := range msg.Blocks[1:] {
					if row.Type != "actions" || len(row.Elements) != 5 {
						t.Error("incorrect row")
					}
					for j, b := range row.Elements {
						idx := i*5 + j
						style := ""
						if idx == 0 {
							style = "primary"
						}
						if b.Type != "button" || b.Text.Type != "plain_text" || b.Text.Text != "choose" || b.ActionID != "ada_"+strconv.Itoa(idx) || b.Value != "delivery\x1f"+strconv.Itoa(idx) || b.Style != style {
							t.Error("incorrect button")
						}
					}
				}
			}
		}
		fmt.Fprintf(w, `{"ok":true,"ts":"1700000001.%06d"}`, n)
	})
	s := f.service()
	defer s.stop()
	reply := "D123:1700000000.000002"
	d := p.Deliver{ID: "delivery", ChatID: "D123", ReplyTo: &reply, Text: strings.Repeat(" ", 3000) + strings.Repeat("x", 2999) + "&<!channel><@U123>&lt;"}
	for i := range 25 {
		d.Buttons = append(d.Buttons, p.Button{Label: "choose", ID: strconv.Itoa(i)})
	}
	id, err := s.Deliver(t.Context(), d)
	if err != nil || id != "D123:1700000001.000002" || joined.String() != strings.Repeat("x", 2999)+"&amp;&lt;!channel&gt;&lt;@U123&gt;&amp;lt;" {
		t.Fatal("wrong delivery result", id, err)
	}
	for _, change := range []func(*p.Deliver){
		func(d *p.Deliver) { d.Buttons = append(d.Buttons, p.Button{}) },
		func(d *p.Deliver) { d.Text = " \n " },
		func(d *p.Deliver) { d.ID = strings.Repeat("é", 1000) },
		func(d *p.Deliver) { d.ID = "a\x1fb" },
		func(d *p.Deliver) { d.ChatID = "C321" },
	} {
		invalid := d
		change(&invalid)
		_, err := s.Deliver(t.Context(), invalid)
		var de *sdk.DeliveryError
		if !errors.As(err, &de) || de.Code != p.CodeBadRequest {
			t.Fatal("invalid delivery accepted", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("partial invalid delivery")
	}
}

func TestReplyToThreadAfterEviction(t *testing.T) {
	posts := make(chan string, 3)
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		var msg outgoing
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Error(err)
		}
		posts <- msg.Thread
		fmt.Fprint(w, `{"ok":true,"ts":"1700000001.000001"}`)
	})
	s := f.service()
	defer s.stop()
	m := message{Type: "message", ChannelType: "im", Channel: "D123", User: "U1", Thread: "1700000000.000000"}
	for i := 1; i <= 4097; i++ {
		m.TS = fmt.Sprintf("1700000000.%06d", i)
		s.fromMessage(m)
		s.fromMessage(m) // Slack can replay the same message.
	}
	for _, tc := range []struct{ ts, want string }{
		{"1700000000.000001", "1700000000.000001"},
		{"1700000000.000002", m.Thread},
		{"1700000000.004097", m.Thread},
	} {
		ref := m.Channel + ":" + tc.ts
		if _, err := s.Deliver(t.Context(), p.Deliver{ChatID: m.Channel, ReplyTo: &ref, Text: "reply"}); err != nil {
			t.Fatal(err)
		}
		if got := <-posts; got != tc.want {
			t.Errorf("reply to %s posted in %s, want %s", ref, got, tc.want)
		}
	}
}

func TestRESTFailures(t *testing.T) {
	for _, tc := range []struct {
		err, code string
		http      int
	}{
		{"", p.CodeRateLimited, 429},
		{"ratelimited", p.CodeRateLimited, 200},
		{"rate_limited", p.CodeRateLimited, 200},
		{"invalid_auth", p.CodeUnauthorised, 200},
		{"not_authed", p.CodeUnauthorised, 200},
		{"account_inactive", p.CodeUnauthorised, 200},
		{"token_revoked", p.CodeUnauthorised, 200},
		{"channel_not_found", p.CodeBadRequest, 200},
		{"not_in_channel", p.CodeBadRequest, 200},
		{"is_archived", p.CodeBadRequest, 200},
		{"msg_too_long", p.CodeBadRequest, 200},
		{"no_text", p.CodeBadRequest, 200},
		{appMarker + botMarker, p.CodeInternal, 200},
		{"", p.CodeInternal, 502},
	} {
		t.Run(tc.code+"/"+strconv.Itoa(tc.http), func(t *testing.T) {
			f := setup(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "9")
				w.WriteHeader(tc.http)
				fmt.Fprintf(w, `{"ok":false,"error":%q}`, tc.err)
			})
			s := f.service()
			defer s.stop()
			_, err := s.Deliver(t.Context(), p.Deliver{ID: "test", ChatID: "D1", Text: "hi"})
			var de *sdk.DeliveryError
			if !errors.As(err, &de) || de.Code != tc.code || tc.code == p.CodeRateLimited && de.RetryAfter != 9*time.Second {
				t.Fatal("wrong error mapping")
			}
			checkSecrets(t, err.Error())
		})
	}
}

func TestRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("followed redirect") }))
	defer target.Close()
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	})
	s := f.service()
	defer s.stop()
	if s.client.Transport.(*http.Transport).Proxy != nil {
		t.Error("proxy enabled")
	}
	if _, err := s.Deliver(t.Context(), p.Deliver{ChatID: "D1", Text: "hi"}); err == nil {
		t.Error("redirect reported success")
	}
}

func TestUnauthorised(t *testing.T) {
	for _, code := range []string{"invalid_auth", "not_authed"} {
		t.Run(code, func(t *testing.T) {
			var opens atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				opens.Add(1)
				fmt.Fprintf(w, `{"ok":false,"error":%q}`, code)
			}))
			defer api.Close()
			f := setup(t, nil)
			f.cfg.apiBase = api.URL
			s := f.service()
			defer s.stop()
			ctx, cancel := context.WithCancel(t.Context())
			input, host := io.Pipe()
			done := make(chan struct{})
			go func() { defer close(done); sdk.Run(ctx, info, s, input, &f.wire, s.log) }()
			json.NewEncoder(host).Encode(p.Hello{Type: p.TypeHello, Protocols: []string{p.V2}, Host: p.Program{Name: "ada", Version: "1"}})
			select {
			case <-s.done:
			case <-time.After(2 * time.Second):
				t.Error("socket worker did not stop")
			}
			cancel()
			host.Close()
			<-done
			if !strings.Contains(f.wire.String(), p.StateUnauthorised) || f.logs.String() == "" || opens.Load() != 1 {
				t.Fatalf("authentication lifecycle: %d calls, %s", opens.Load(), f.wire.String())
			}
		})
	}
}
