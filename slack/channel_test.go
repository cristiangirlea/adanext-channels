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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristiangirlea/adanext-channels/internal/ws"
	"github.com/cristiangirlea/adanext-channels/internal/ws/wstest"
	"github.com/cristiangirlea/adanext-channels/sdk"
	"github.com/cristiangirlea/adanext-channels/sdk/fakecore"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const appMarker = "xapp-offline-marker"
const botMarker = "xoxb-offline-marker"

type capture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capture) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(b)
}
func (c *capture) String() string { c.mu.Lock(); defer c.mu.Unlock(); return c.buf.String() }

type fixture struct {
	t          *testing.T
	cfg        config
	socket     string
	peers      chan *wstest.Peer
	calls      atomic.Int32
	logs, wire capture
}

func setup(t *testing.T, api http.HandlerFunc) *fixture {
	f := &fixture{t: t, cfg: config{tokens: appMarker + "," + botMarker, channels: "C123", stateDir: t.TempDir()}, peers: make(chan *wstest.Peer, 8)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "ticket=temporary" || r.Header.Get("Authorization") != "" {
			t.Error("unexpected socket handshake")
		}
		peer, err := wstest.Upgrade(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		select {
		case f.peers <- peer:
		case <-t.Context().Done():
			peer.Close()
		}
	}))
	t.Cleanup(server.Close)
	f.socket = "ws" + strings.TrimPrefix(server.URL, "http")
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := botMarker
		if r.URL.Path == "/apps.connections.open" {
			token = appMarker
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("incorrect credential for %s", r.URL.Path)
		}
		if r.URL.Path == "/apps.connections.open" {
			f.calls.Add(1)
			if r.Method != "POST" {
				t.Error("connection request was not POST")
			}
			fmt.Fprint(w, `{"ok":true,"url":"wss://wss-primary.slack.com/link?ticket=temporary"}`)
			return
		}
		if api != nil {
			api(w, r)
			return
		}
		t.Error("unexpected " + r.URL.Path)
		w.WriteHeader(400)
	}))
	f.cfg.apiBase = rest.URL
	t.Cleanup(rest.Close)
	t.Cleanup(func() { checkSecrets(t, f.logs.String()+f.wire.String()) })
	return f
}

func checkSecrets(t *testing.T, text string) {
	t.Helper()
	for _, marker := range []string{appMarker, botMarker} {
		if strings.Contains(text, marker) {
			t.Error("credential in captured output")
		}
	}
}

func TestRESTTokenRouting(t *testing.T) {
	// The fake API checks each Authorization header before serving the method.
	f := setup(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"ok":true}`) })
	s := f.service()
	defer s.stop()
	for _, tc := range []struct{ method, endpoint string }{
		{"POST", "apps.connections.open"},
		{"GET", "auth.test"},
		{"GET", "conversations.history"},
		{"GET", "users.info"},
		{"POST", "chat.postMessage"},
	} {
		if _, err := s.call(t.Context(), tc.method, tc.endpoint, nil, nil); err != nil {
			t.Fatalf("%s: %v", tc.endpoint, err)
		}
	}
}

func (f *fixture) service() *service {
	f.t.Helper()
	s, err := newService(f.cfg, &f.logs)
	if err != nil {
		f.t.Fatal(err)
	}
	s.dial = func(ctx context.Context, addr string, h http.Header) (*ws.Conn, error) {
		if addr != "wss://wss-primary.slack.com/link?ticket=temporary" {
			return nil, errors.New("unexpected socket URL")
		}
		return ws.Dial(ctx, f.socket+"/link?ticket=temporary", h)
	}
	s.backoff = sdk.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond}
	return s
}
func (f *fixture) start() *fakecore.Core {
	c := fakecore.Start(f.t, func(ctx context.Context, in io.Reader, out io.Writer) error {
		s := f.service()
		defer s.stop()
		return sdk.Run(ctx, info, s, in, io.MultiWriter(&f.wire, out), s.log)
	})
	c.Handshake()
	return c
}
func (f *fixture) peer() *wstest.Peer {
	f.t.Helper()
	select {
	case peer := <-f.peers:
		f.t.Cleanup(func() { peer.Close() })
		send(f.t, peer, `{"type":"hello"}`)
		return peer
	case <-time.After(3 * time.Second):
		f.t.Fatal("no socket connection")
		return nil
	}
}
func send(t *testing.T, peer *wstest.Peer, data string) {
	t.Helper()
	if err := peer.Send(0x81, []byte(data)); err != nil {
		t.Fatal(err)
	}
}
func event(t *testing.T, peer *wstest.Peer, envelope, id, fields string) {
	t.Helper()
	send(t, peer, fmt.Sprintf(`{"envelope_id":%q,"type":"events_api","retry_attempt":1,"payload":{"event_id":%q,"event":{%s}}}`, envelope, id, fields))
}
func ack(t *testing.T, peer *wstest.Peer, id string) {
	t.Helper()
	peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, b, err := peer.Frame()
	var v map[string]string
	if err != nil || json.Unmarshal(b, &v) != nil || len(v) != 1 || v["envelope_id"] != id {
		t.Fatalf("expected acknowledgement of %s, got %s (%v)", id, b, err)
	}
}

const dm = `"type":"message","user":"U123","channel_type":"im","channel":"D123","ts":"1700000000.000002"`

func TestEnvelopes(t *testing.T) {
	var users atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/users.info" || r.URL.Query().Get("user") != "U123" {
			t.Error("unexpected lookup")
		}
		users.Add(1)
		fmt.Fprint(w, `{"ok":true,"user":{"profile":{"display_name":"Ada Tester"}}}`)
	})
	core := f.start()
	peer := f.peer()
	event(t, peer, "first", "Ev1", dm+`,"subtype":"file_share","thread_ts":"1700000000.000001","text":"hi `+botMarker+`","files":[{"name":"file","mimetype":"text/plain","size":7,"url_private":"https://files.slack.com/f"}]`)
	in := core.NextInbound()
	if in.ID != "D123:1700000000.000002" || in.Sender.ID != "U123" || in.Sender.Name != "Ada Tester" || in.Chat.ID != "D123" || *in.Chat.Type != "private" || in.Text != "hi [redacted]" || in.Time != "2023-11-14T22:13:20.000002Z" || in.ReplyTo == nil || *in.ReplyTo != "D123:1700000000.000001" || len(in.Attachments) != 1 || in.Attachments[0] != (p.Attachment{Name: "file", MIME: "text/plain", Size: 7, Ref: "https://files.slack.com/f"}) {
		t.Fatal("incorrect DM fields")
	}
	event(t, peer, "retry", "Ev1", dm)
	for i, fields := range []string{dm + `,"bot_id":"B12"`, dm + `,"subtype":"message_changed"`, `"type":"app_mention","channel":"C999","user":"U123","ts":"1700000000.000003"`, `"type":"reaction_added"`} {
		id := strconv.Itoa(i)
		event(t, peer, id, "ignored"+id, fields)
		ack(t, peer, id)
	}
	core.Ack(in.ID)
	ack(t, peer, "first")
	ack(t, peer, "retry")
	event(t, peer, "later", "Ev1", strings.Replace(dm, ".000002", ".000019", 1))
	ack(t, peer, "later")
	event(t, peer, "mention", "Ev2", `"type":"app_mention","channel":"C123","user":"U123","ts":"1700000000.000003","text":"hey"`)
	group := core.NextInbound()
	if group.Chat.ID != "C123" || *group.Chat.Type != "group" {
		t.Fatal("wrong mention")
	}
	core.Ack(group.ID)
	ack(t, peer, "mention")
	send(t, peer, `{"type":"interactive","envelope_id":"button","payload":{"type":"block_actions","user":{"id":"U123"},"channel":{"id":"C123"},"actions":[{"type":"button","value":"request\u001fyes","action_ts":"1700000001.000001"}]}}`)
	button := core.NextInbound()
	if button.Action == nil || button.Action.ButtonID != "yes" || button.Action.DeliveryID != "request" || button.Sender.ID != "U123" || button.Chat.ID != "C123" {
		t.Fatal("wrong button action")
	}
	send(t, peer, `{"type":"unsupported","envelope_id":"before-store-ack"}`)
	ack(t, peer, "before-store-ack")
	core.Ack(button.ID)
	ack(t, peer, "button")
	if users.Load() != 1 {
		t.Fatalf("user lookups %d", users.Load())
	}
}

func TestIgnoredButtonPresses(t *testing.T) {
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"ok":true,"user":{"name":"tester"}}`)
	})
	core := f.start()
	peer := f.peer()
	const button = `{"type":"button","value":"request\u001fyes"}`
	cases := []struct{ id, channel, actions string }{
		{"unlisted", "C999", button},
		{"two", "C123", button + "," + button},
		{"non-button", "C123", strings.Replace(button, "button", "static_select", 1)},
		{"oversized", "C123", strings.Replace(button, "yes", strings.Repeat("é", 997), 1)},
	}
	for _, tc := range cases {
		send(t, peer, fmt.Sprintf(`{"type":"interactive","envelope_id":%q,"payload":{"type":"block_actions","user":{"id":"U1"},"channel":{"id":%q},"actions":[%s]}}`, tc.id, tc.channel, tc.actions))
	}
	event(t, peer, "live", "E1", dm)
	in := core.NextInbound()
	if in.ID != "D123:1700000000.000002" {
		t.Fatalf("unexpected inbound %q before DM", in.ID)
	}
	for _, tc := range cases {
		ack(t, peer, tc.id)
	}
	core.Ack(in.ID)
	ack(t, peer, "live")
}

func TestNameCache(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		status     int
		cacheMiss  bool
	}{
		{"rate limit", "", 429, false},
		{"server failure", "user_not_found", 503, false},
		{"network", "", 0, false},
		{"cancelled", "", 0, false},
		{"missing user", "user_not_found", 200, true},
		{"hidden user", "user_not_visible", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			f := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 && tc.name != "cancelled" {
					if tc.name == "network" {
						panic(http.ErrAbortHandler)
					}
					w.WriteHeader(tc.status)
					fmt.Fprintf(w, `{"ok":false,"error":%q}`, tc.code)
					return
				}
				fmt.Fprint(w, `{"ok":true,"user":{"name":"tester"}}`)
			})
			s := f.service()
			defer s.stop()
			ctx := t.Context()
			if tc.name == "cancelled" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			if got := s.name(ctx, "U1"); got != "" {
				t.Fatalf("initial lookup returned %q", got)
			}
			want, wantCalls := "tester", int32(2)
			if tc.cacheMiss {
				want, wantCalls = "", 1
			} else if tc.name == "cancelled" {
				wantCalls = 1
			}
			for range 2 {
				if got := s.name(t.Context(), "U1"); got != want {
					t.Fatalf("later lookup returned %q, want %q", got, want)
				}
			}
			if calls.Load() != wantCalls {
				t.Errorf("lookups %d, want %d", calls.Load(), wantCalls)
			}
		})
	}
}

func TestLinkDisabledReconnectsWithBackoff(t *testing.T) {
	f := setup(t, nil)
	s := f.service()
	ctx, cancel := context.WithCancel(t.Context())
	input, host := io.Pipe()
	waited := make(chan time.Duration, 1)
	s.backoff.Sleep = func(ctx context.Context, d time.Duration) error {
		select {
		case waited <- d:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer s.stop()
		sdk.Run(ctx, info, s, input, &f.wire, s.log)
	}()
	defer func() { cancel(); host.Close(); <-done }()
	if err := json.NewEncoder(host).Encode(p.Hello{Type: p.TypeHello, Protocols: []string{p.V2}, Host: p.Program{Name: "ada", Version: "1"}}); err != nil {
		t.Fatal(err)
	}
	send(t, f.peer(), `{"type":"disconnect","reason":"link_disabled"}`)
	f.peer()
	select {
	case delay := <-waited:
		if delay != s.backoff.Base {
			t.Errorf("reconnect delay %v, want %v", delay, s.backoff.Base)
		}
	default:
		t.Error("opened replacement socket without backoff")
	}
}

func TestRestartAndRefresh(t *testing.T) {
	var pages atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/users.info":
			fmt.Fprint(w, `{"ok":false,"error":"unknown_user"}`)
		case "/conversations.history":
			n := pages.Add(1)
			q := r.URL.Query()
			if q.Get("channel") != "D123" {
				t.Error("wrong history channel")
			}
			switch n {
			case 1, 2:
				if q.Get("oldest") != "1700000000.000002" {
					t.Error("wrong restart cursor")
				}
				if n == 1 {
					fmt.Fprint(w, `{"ok":true,"messages":[{"type":"message","user":"U123","ts":"1700000000.000004","text":"four"}],"response_metadata":{"next_cursor":"p=2"}}`)
					return
				}
				if q.Get("cursor") != "p=2" {
					t.Error("missing next cursor")
				}
				fmt.Fprint(w, `{"ok":true,"messages":[{"type":"message","user":"U123","ts":"1700000000.000003","text":"three"}]}`)
			default:
				if q.Get("oldest") != "1700000000.000005" {
					t.Error("wrong refresh cursor")
				}
				fmt.Fprint(w, `{"ok":true,"messages":[]}`)
			}
		default:
			t.Error("unexpected " + r.URL.Path)
		}
	})
	c := f.start()
	first := f.peer()
	event(t, first, "one", "E1", dm)
	in := c.NextInbound()
	if in.Sender.Name != "" {
		t.Error("expected empty name")
	}
	c.Ack(in.ID)
	ack(t, first, "one")
	c.Restart()
	c.Handshake()
	second := f.peer()
	event(t, second, "overlap", "E4", strings.Replace(dm, ".000002", ".000004", 1))
	event(t, second, "live", "E5", strings.Replace(dm, ".000002", ".000005", 1))
	for _, suffix := range []string{"000003", "000004", "000005"} {
		m := c.NextInbound()
		if m.ID != "D123:1700000000."+suffix {
			t.Fatalf("unexpected order %s", m.ID)
		}
		c.Ack(m.ID)
	}
	ack(t, second, "overlap")
	ack(t, second, "live")
	send(t, second, `{"type":"disconnect","reason":"refresh_requested"}`)
	// Drive Slack independently while the fake core reads the connection status frames.
	ready := make(chan *wstest.Peer, 1)
	go func() {
		select {
		case next := <-f.peers:
			second.SetReadDeadline(time.Now().Add(time.Second))
			second.Send(0x81, []byte(`{"type":"unsupported","envelope_id":"old"}`))
			_, data, err := second.Frame()
			var receipt struct {
				ID string `json:"envelope_id"`
			}
			if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.ID != "old" {
				t.Error("old socket stopped processing before replacement hello")
			}
			next.Send(0x81, []byte(`{"type":"hello"}`))
			next.Send(0x81, []byte(`{"type":"events_api","envelope_id":"six","payload":{"event_id":"E6","event":{`+strings.Replace(dm, ".000002", ".000006", 1)+`}}}`))
			ready <- next
		case <-t.Context().Done():
		}
	}()
	m := c.NextInbound()
	if !strings.HasSuffix(m.ID, "000006") {
		t.Fatal(m.ID)
	}
	next := <-ready
	defer next.Close()
	c.Ack(m.ID)
	ack(t, next, "six")
	if pages.Load() != 3 {
		t.Errorf("history pages %d", pages.Load())
	}
}

func TestConfiguration(t *testing.T) {
	for _, tokens := range []string{"", appMarker, botMarker + "," + appMarker, "xapp-," + botMarker, appMarker + ",xoxb-", appMarker + "," + botMarker + ",more", appMarker + "," + botMarker + "\n"} {
		_, err := newService(config{tokens: tokens}, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "ADA_SLACK_TOKENS") {
			t.Fatal("missing configuration error")
		}
		checkSecrets(t, err.Error())
	}
	f := setup(t, nil)
	for _, channels := range []string{"D123", "C1,,C2", "C1,", "C1/foo", "c1", "C"} {
		cfg := f.cfg
		cfg.channels = channels
		if _, err := newService(cfg, io.Discard); err == nil || !strings.Contains(err.Error(), "ADA_SLACK_CHANNELS") {
			t.Error("accepted invalid channels")
		}
	}
	m, err := sdk.LoadManifest("channel.json")
	if err != nil || m.Name != "slack" || m.Label != "Slack" || m.SecretEnv != "ADA_SLACK_TOKENS" || !slices.Equal(m.Hosts, []string{"slack.com", "*.slack.com"}) {
		t.Fatal("wrong manifest", err)
	}
}

func TestUncommittedEnvelopeReplaysAfterRestart(t *testing.T) {
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/users.info" {
			t.Error("a history cursor was written before Ada's acknowledgement")
		}
		fmt.Fprint(w, `{"ok":true,"user":{"name":"tester"}}`)
	})
	core := f.start()
	first := f.peer()
	event(t, first, "before", "Ev1", dm)
	original := core.NextInbound()
	core.Restart()
	op, _, err := first.Frame()
	if err == nil && op == 0x81 {
		t.Fatal("acknowledged the envelope before restart")
	}
	core.Handshake()
	second := f.peer()
	event(t, second, "after", "Ev1", dm)
	replayed := core.NextInbound()
	if replayed.ID != original.ID || len(core.Stored()) != 1 {
		t.Fatal("replay changed the stored message")
	}
	core.Ack(replayed.ID)
	ack(t, second, "after")
}
func TestSocketHost(t *testing.T) {
	for _, raw := range []string{"ws://a.slack.com", "https://a.slack.com", "wss://slack.com", "wss://a.b.slack.com", "wss://a.slack.com.bad.com", "wss://user@a.slack.com", "wss://a.slack.com:444", "wss://K.slack.com", "wss://-bad.slack.com"} {
		if _, err := socketURL(raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"wss://wss-1.slack.com/t?k=v", "wss://A.SLACK.COM:443/"} {
		if _, err := socketURL(raw); err != nil {
			t.Error(err)
		}
	}
}
