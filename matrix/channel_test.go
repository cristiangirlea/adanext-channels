package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	"github.com/cristiangirlea/adanext-channels/sdk/fakecore"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const marker = "fake-matrix-redaction-marker"
const room = "!dm:test"
const bot = "@ada:test"
const user = "@reader:test"

func TestSettings(t *testing.T) {
	t.Setenv("ADA_MATRIX_TOKEN", marker)
	t.Setenv("ADA_MATRIX_STATE_DIR", "")
	t.Setenv("ADA_MATRIX_HOMESERVER", "")
	t.Setenv("ADA_MATRIX_GROUPS", "")
	cfg, err := readSettings()
	if err != nil || cfg.home != "https://matrix.org" || cfg.dir != "." || cfg.groups {
		t.Fatalf("incorrect defaults: %v", err)
	}
	for _, home := range []string{"http://matrix.org", "matrix.org", "https://", "https://u:p@matrix.org", "https://matrix.org/path", "https://matrix.org/%2f", "https://matrix.org?", "https://matrix.org/#x", "https://matrix.org:99999", "https://matrix.org:"} {
		t.Setenv("ADA_MATRIX_HOMESERVER", home)
		if _, err := readSettings(); err == nil || !strings.Contains(err.Error(), "ADA_MATRIX_HOMESERVER") {
			t.Errorf("accepted invalid homeserver %q", home)
		}
	}
	t.Setenv("ADA_MATRIX_HOMESERVER", "https://MATRIX.org:8448/")
	t.Setenv("ADA_MATRIX_GROUPS", "1")
	cfg, err = readSettings()
	if err != nil || !cfg.groups {
		t.Fatal("valid settings rejected", err)
	}
	if _, err = newService(cfg, "channel.json", io.Discard); err != nil {
		t.Fatal(err)
	}
	cfg.home = "https://matrix.org.foreign.example"
	if _, err = newService(cfg, "channel.json", io.Discard); err == nil {
		t.Fatal("unlisted host accepted")
	}
	t.Setenv("ADA_MATRIX_TOKEN", "")
	if _, err = readSettings(); err == nil || !strings.Contains(err.Error(), "ADA_MATRIX_TOKEN") {
		t.Fatal("missing token accepted")
	}
}

type fixture struct {
	t          *testing.T
	server     *httptest.Server
	dir        string
	groups     bool
	identities atomic.Int32
}

func homeserver(t *testing.T, handle func(http.ResponseWriter, *http.Request, string) bool) *fixture {
	t.Helper()
	f := &fixture{t: t, dir: t.TempDir()}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, ok := strings.CutPrefix(r.URL.Path, "/_matrix/client/v3/")
		if !ok || r.Header.Get("Authorization") != "Bearer "+marker || strings.Contains(r.URL.String(), marker) {
			t.Error("incorrect Matrix endpoint or authentication")
			w.WriteHeader(400)
			return
		}
		if path == "sync" {
			var filter struct {
				Room struct{ Timeline, State struct{ Types []string } }
			}
			if err := json.Unmarshal([]byte(r.URL.Query().Get("filter")), &filter); err != nil {
				t.Errorf("invalid inline sync filter: %v", err)
			}
			for _, typ := range []string{"m.room.message", "m.room.encrypted", "m.room.member", "m.room.encryption"} {
				if !slices.Contains(filter.Room.Timeline.Types, typ) {
					t.Errorf("missing timeline filter for %s", typ)
				}
			}
			for _, typ := range []string{"m.room.member", "m.room.encryption"} {
				if !slices.Contains(filter.Room.State.Types, typ) {
					t.Errorf("missing state filter for %s", typ)
				}
			}
		}
		if handle != nil && handle(w, r, path) {
			return
		}
		switch {
		case path == "account/whoami" && r.Method == "GET":
			f.identities.Add(1)
			fmt.Fprintf(w, `{"user_id":%q}`, bot)
		case path == "sync" && r.Method == "GET":
			io.WriteString(w, `{"next_batch":"base"}`)
		case strings.HasSuffix(path, "/joined_members") && r.Method == "GET":
			fmt.Fprintf(w, `{"joined":{%q:{},%q:{"display_name":"Reader"}}}`, bot, user)
		default:
			t.Errorf("unexpected %s %s", r.Method, path)
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fixture) service(log io.Writer) *service {
	f.t.Helper()
	s, err := newService(settings{token: marker, home: f.server.URL, dir: f.dir, groups: f.groups}, "", log)
	if err != nil {
		f.t.Fatal(err)
	}
	s.client.Transport.(*http.Transport).TLSClientConfig = f.server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	f.t.Cleanup(s.client.CloseIdleConnections)
	return s
}

func (f *fixture) startDelivery() *service {
	f.t.Helper()
	s := f.service(io.Discard)
	if err := s.connect(f.t.Context()); err != nil {
		f.t.Fatal(err)
	}
	if _, err := s.syncOnce(f.t.Context()); err != nil {
		f.t.Fatal(err)
	}
	return s
}

func event(id, typ, sender, content string) string {
	return fmt.Sprintf(`{"event_id":%q,"type":%q,"sender":%q,"origin_server_ts":1700000000123,"content":%s}`, id, typ, sender, content)
}
func message(id, body string) string {
	return event(id, "m.room.message", user, fmt.Sprintf(`{"msgtype":"m.text","body":%q}`, body))
}
func batch(next, roomID string, events ...string) string {
	return fmt.Sprintf(`{"next_batch":%q,"rooms":{"join":{%q:{"timeline":{"events":[%s]}}}}}`, next, roomID, strings.Join(events, ","))
}

func TestSyncCheckpointAndReplay(t *testing.T) {
	var starts, replays atomic.Int32
	advanced := make(chan struct{}, 1)
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		if path != "sync" {
			return false
		}
		q := r.URL.Query()
		if r.Method != "GET" || q.Get("timeout") != "30000" {
			t.Error("incorrect sync request")
		}
		switch q.Get("since") {
		case "":
			starts.Add(1)
			io.WriteString(w, batch("first", room, message("$old", "history")))
		case "first":
			replays.Add(1)
			one := event("$1", "m.room.message", user, `{"msgtype":"m.emote","body":"waves `+marker+`","m.relates_to":{"m.in_reply_to":{"event_id":"$parent"}}}`)
			io.WriteString(w, batch("second", room, event("$self", "m.room.message", bot, `{"msgtype":"m.text","body":"mine"}`), one, one, message("$2", "next")))
		case "second":
			select {
			case advanced <- struct{}{}:
			default:
			}
			<-r.Context().Done()
		default:
			t.Error("unknown cursor")
			w.WriteHeader(400)
		}
		return true
	})
	var logs, wire bytes.Buffer
	c := fakecore.Start(t, func(ctx context.Context, in io.Reader, out io.Writer) error {
		s := f.service(&logs)
		return sdk.Run(ctx, info, s, in, io.MultiWriter(&wire, out), s.log)
	})
	for attempt := range 2 {
		if attempt > 0 {
			c.Restart()
		}
		c.Handshake()
		one := c.NextInbound()
		if one.ID != "$1" || one.Sender != (p.Sender{ID: user, Name: "Reader"}) || one.Chat.ID != room || one.Chat.Type == nil || *one.Chat.Type != "private" || one.Time != "2023-11-14T22:13:20.123Z" || one.Text != "* waves [redacted]" || one.ReplyTo == nil || *one.ReplyTo != "$parent" {
			t.Fatal("incorrect DM mapping")
		}
		c.Ack(one.ID)
		two := c.NextInbound()
		if two.ID != "$2" {
			t.Fatal("duplicate or missing event")
		}
		if attempt > 0 {
			c.Ack(two.ID)
		}
	}
	select {
	case <-advanced:
	case <-time.After(3 * time.Second):
		t.Fatal("complete batch did not advance")
	}
	c.Restart()
	if starts.Load() != 1 || replays.Load() != 2 || len(c.Stored()) != 2 || f.identities.Load() != 2 {
		t.Fatal("incorrect startup or replay")
	}
	if strings.Contains(wire.String()+logs.String(), marker) {
		t.Error("secret exposed")
	}
}

func TestResumeWithStaleFilter(t *testing.T) {
	for _, status := range []int{400, 404} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				if path != "sync" {
					return false
				}
				q := r.URL.Query()
				if q.Get("filter") == "expired" {
					w.WriteHeader(status)
					return true
				}
				if q.Get("since") != "stored" || q.Get("full_state") != "true" {
					t.Error("resume lost cursor or did not refresh encryption state")
				}
				io.WriteString(w, batch("resumed", room, message("$new", "hello")))
				return true
			})
			s := f.service(io.Discard)
			if err := s.connect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(s.statePath, []byte(`{"since":"stored","filter":"expired"}`), 0600); err != nil {
				t.Fatal(err)
			}
			s = f.service(io.Discard)
			if err := s.connect(t.Context()); err != nil {
				t.Fatal(err)
			}
			msgs, err := s.syncOnce(t.Context())
			if err != nil || len(msgs) != 1 || msgs[0].ID != "$new" {
				t.Fatalf("resume got %v, %v", msgs, err)
			}
			if err := s.Commit(t.Context(), msgs[0].ID); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(s.statePath)
			if err != nil || bytes.Contains(data, []byte(`"filter"`)) {
				t.Fatalf("stale filter retained, read error %v", err)
			}
		})
	}
}

func TestWhoamiUnauthorisedStops(t *testing.T) {
	var calls atomic.Int32
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		calls.Add(1)
		if path != "account/whoami" {
			t.Error("unexpected call")
		}
		w.WriteHeader(401)
		fmt.Fprintf(w, `{"errcode":"M_UNKNOWN_TOKEN","error":%q}`, marker)
		return true
	})
	var logs, wire bytes.Buffer
	s := f.service(&logs)
	in, host := io.Pipe()
	defer host.Close()
	go func() {
		_ = json.NewEncoder(host).Encode(p.Hello{Type: p.TypeHello, Protocols: []string{p.V2}, Host: p.Program{Name: "ada", Version: "1"}})
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := sdk.Run(ctx, info, s, in, &wire, s.log)
	if err == nil || calls.Load() != 1 || !strings.Contains(wire.String(), `"state":"unauthorised"`) || !strings.Contains(logs.String(), "unauthorised") || strings.Contains(wire.String()+logs.String()+err.Error(), marker) {
		t.Fatal("missing terminal unauthorised status or unsafe output", wire.String())
	}
}

func TestDelivery(t *testing.T) {
	type part struct {
		Msgtype, Body string
		Relates       struct {
			Reply struct {
				ID string `json:"event_id"`
			} `json:"m.in_reply_to"`
		} `json:"m.relates_to"`
	}
	var paths, bodies []string
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		if !strings.Contains(path, "/send/") {
			return false
		}
		if r.Method != "PUT" || !strings.HasPrefix(path, "rooms/"+room+"/send/m.room.message/") {
			t.Error("wrong send endpoint")
		}
		var part part
		if json.NewDecoder(r.Body).Decode(&part) != nil || part.Msgtype != "m.text" || part.Relates.Reply.ID != "$parent" || strings.TrimSpace(part.Body) == "" || len(part.Body) > 30000 {
			t.Error("invalid delivery part")
		}
		paths = append(paths, path)
		bodies = append(bodies, part.Body)
		fmt.Fprintf(w, `{"event_id":%q}`, "$"+strconv.Itoa((len(paths)-1)%2))
		return true
	})
	s := f.startDelivery()
	reply := "$parent"
	d := p.Deliver{ID: "delivery", ChatID: room, ReplyTo: &reply, Text: strings.Repeat(" ", 30000) + strings.Repeat("x", 29999) + "世", Buttons: []p.Button{{ID: "a", Label: "Approve"}, {ID: "d", Label: "Deny"}}}
	for range 2 {
		id, err := s.Deliver(t.Context(), d)
		if err != nil || id != "$1" {
			t.Fatalf("last event ID %q, %v", id, err)
		}
	}
	if len(paths) != 4 || !slices.Equal(paths[:2], paths[2:]) || paths[0] == paths[1] || !strings.Contains(paths[0], "delivery") || !strings.HasSuffix(strings.Join(bodies[:2], ""), "Reply 1 to Approve, 2 to Deny") {
		t.Fatal("bad choices or unstable transaction IDs")
	}
}

func TestDeliveryErrors(t *testing.T) {
	for _, tc := range []struct {
		status     int
		code, want string
	}{
		{413, "", p.CodeBadRequest}, {500, "M_TOO_LARGE", p.CodeBadRequest},
		{403, "M_USER_DEACTIVATED", p.CodeUnauthorised}, {401, "M_USER_LOCKED", p.CodeUnauthorised},
		{429, "", p.CodeRateLimited}, {403, "M_LIMIT_EXCEEDED", p.CodeRateLimited}, {401, "M_UNKNOWN_TOKEN", p.CodeUnauthorised}, {401, "M_MISSING_TOKEN", p.CodeUnauthorised}, {403, "M_FORBIDDEN", p.CodeBadRequest}, {500, "M_NOT_FOUND", p.CodeBadRequest}, {404, "", p.CodeBadRequest}, {400, "", p.CodeBadRequest}, {502, "", p.CodeInternal},
	} {
		t.Run(fmt.Sprint(tc.status, tc.code), func(t *testing.T) {
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				if !strings.Contains(path, "/send/") {
					return false
				}
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, `{"errcode":%q,"retry_after_ms":1700,"error":%q}`, tc.code, marker)
				return true
			})
			s := f.startDelivery()
			_, err := s.Deliver(t.Context(), p.Deliver{ID: "out", ChatID: room, Text: "hi"})
			var fail *sdk.DeliveryError
			if !errors.As(err, &fail) || fail.Code != tc.want || tc.want == p.CodeRateLimited && fail.RetryAfter != 1700*time.Millisecond || strings.Contains(err.Error(), marker) {
				t.Fatal("incorrect delivery failure")
			}
		})
	}
}

func TestTransport(t *testing.T) {
	var reached atomic.Bool
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Store(true) }))
	defer foreign.Close()
	f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
		http.Redirect(w, r, foreign.URL, 307)
		return true
	})
	s := f.service(io.Discard)
	if s.client.Transport.(*http.Transport).Proxy != nil || s.connect(t.Context()) == nil || reached.Load() {
		t.Fatal("unsafe transport")
	}
}

func TestManifest(t *testing.T) {
	m, err := sdk.LoadManifest("channel.json")
	if err != nil || m.Name != "matrix" || m.Label != "Matrix" || m.SecretEnv != "ADA_MATRIX_TOKEN" || !slices.Equal(m.Hosts, []string{"matrix.org"}) {
		t.Fatal("invalid shipped manifest", err)
	}
}

func TestEncodedDeliverySize(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		parts      int // Zero means the part must be split again.
	}{
		{"HTML", strings.Repeat("<&", 15000), 1},
		{"quotes", strings.Repeat("\"", 30000), 0},
		{"controls", strings.Repeat("\x01", 29996) + "last", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bodies []string
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				if !strings.Contains(path, "/send/") {
					return false
				}
				data, err := io.ReadAll(r.Body)
				var msg outgoing
				if err != nil || len(data) > 60000 || json.Unmarshal(data, &msg) != nil {
					t.Errorf("invalid encoded event (%d bytes)", len(data))
				}
				bodies = append(bodies, msg.Body)
				fmt.Fprintf(w, `{"event_id":"$part%d"}`, len(bodies))
				return true
			})
			s := f.startDelivery()
			if _, err := s.Deliver(t.Context(), p.Deliver{ID: "sized", ChatID: room, Text: tc.text}); err != nil {
				t.Fatal(err)
			}
			if strings.Join(bodies, "") != tc.text || tc.parts == 1 && len(bodies) != 1 || tc.parts == 0 && len(bodies) < 2 {
				t.Errorf("text did not round trip in the expected number of parts (got %d)", len(bodies))
			}
		})
	}
}

func TestRetryAfterHeader(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	for header, want := range map[string]time.Duration{"3": 3 * time.Second, now.Add(time.Minute).Format(http.TimeFormat): time.Minute, "invalid": 1700 * time.Millisecond, "0": 0} {
		t.Run(header, func(t *testing.T) {
			f := homeserver(t, func(w http.ResponseWriter, r *http.Request, path string) bool {
				w.Header().Set("Retry-After", header)
				w.WriteHeader(http.StatusTooManyRequests)
				io.WriteString(w, `{"retry_after_ms":1700}`)
				return true
			})
			s := f.service(io.Discard)
			s.now = func() time.Time { return now }
			err := s.connect(t.Context())
			var failure *sdk.DeliveryError
			if !errors.As(err, &failure) || failure.Code != p.CodeRateLimited || failure.RetryAfter != want {
				t.Fatalf("got %v, want retry after %v", failure, want)
			}
		})
	}
}
