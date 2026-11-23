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
	"net/url"
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

const marker = "test-only-discord-marker"

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
	peers      chan *wstest.Peer
	committed  chan string
	logs, wire capture
	gateway    string
	configure  func(*service)
	startLimit map[string]int
	elapsed    atomic.Int64
}

func setup(t *testing.T, rest http.HandlerFunc) *fixture {
	f := &fixture{t: t, peers: make(chan *wstest.Peer, 4), committed: make(chan string, 256)}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("encoding") != "json" || r.URL.Query().Get("v") != "10" {
			t.Error("gateway query")
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
	t.Cleanup(gateway.Close)
	f.cfg = config{token: marker, stateDir: t.TempDir()}
	f.gateway = "ws" + strings.TrimPrefix(gateway.URL, "http")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bot "+marker || r.Header.Get("User-Agent") != "DiscordBot (https://github.com/cristiangirlea/adanext-channels, 0.1)" {
			t.Error("incorrect REST headers")
		}
		if r.URL.Path == "/gateway/bot" && r.Method == "GET" {
			json.NewEncoder(w).Encode(map[string]any{"url": "wss://gateway.discord.gg", "session_start_limit": f.startLimit})
			return
		}
		if rest != nil {
			rest(w, r)
			return
		}
		t.Error("unexpected REST request")
		w.WriteHeader(400)
	}))
	t.Cleanup(api.Close)
	f.cfg.apiBase = api.URL
	t.Cleanup(func() {
		if strings.Contains(f.logs.String()+f.wire.String(), marker) {
			t.Error("credential in captured output")
		}
	})
	return f
}

func (f *fixture) service() *service {
	f.t.Helper()
	s, err := newService(f.cfg, &f.logs)
	if err != nil {
		f.t.Fatal(err)
	}
	s.dialGateway = func(ctx context.Context, addr string, h http.Header) (*ws.Conn, error) {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		switch u.Host {
		case "gateway.discord.gg", "gateway-us-east1-b.discord.gg":
		default:
			return nil, fmt.Errorf("unmapped fake gateway %s", u.Host)
		}
		return ws.Dial(ctx, f.gateway+u.RequestURI(), h)
	}
	s.now = func() time.Time { return time.Unix(100, f.elapsed.Load()) }
	s.wait = func(ctx context.Context, d time.Duration) error { f.elapsed.Add(int64(d)); return ctx.Err() }
	s.backoff = sdk.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond, Sleep: s.wait}
	if f.configure != nil {
		f.configure(s)
	}
	return s
}

type observedService struct {
	*service
	committed chan<- string
}

func (s observedService) Commit(ctx context.Context, id string) error {
	if err := s.service.Commit(ctx, id); err != nil {
		return err
	}
	s.committed <- id
	return nil
}

func (f *fixture) start() *fakecore.Core {
	core := fakecore.Start(f.t, func(ctx context.Context, in io.Reader, out io.Writer) error {
		s := f.service()
		defer s.stop()
		return sdk.Run(ctx, info, observedService{s, f.committed}, in, io.MultiWriter(&f.wire, out), s.log)
	})
	core.Handshake()
	return core
}

type packet struct {
	Op int
	D  json.RawMessage
}

func readPacket(t *testing.T, peer *wstest.Peer) packet {
	t.Helper()
	_, b, err := peer.Frame()
	if err != nil {
		t.Fatal(err)
	}
	var pkt packet
	if err = json.Unmarshal(b, &pkt); err != nil {
		t.Fatal(err)
	}
	return pkt
}

func sendPacket(t *testing.T, peer *wstest.Peer, body string) {
	t.Helper()
	if err := peer.Send(0x81, []byte(body)); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) hello(interval int) (*wstest.Peer, packet) {
	f.t.Helper()
	select {
	case peer := <-f.peers:
		f.t.Cleanup(func() { peer.Close() })
		sendPacket(f.t, peer, fmt.Sprintf(`{"op":10,"d":{"heartbeat_interval":%d}}`, interval))
		return peer, readPacket(f.t, peer)
	case <-time.After(4 * time.Second):
		f.t.Fatal("no gateway connection")
		return nil, packet{}
	}
}

func identify(t *testing.T, pkt packet, intents int) {
	t.Helper()
	var d struct {
		Token      string
		Intents    int
		Properties map[string]string
	}
	if pkt.Op != 2 || json.Unmarshal(pkt.D, &d) != nil || d.Intents != intents || d.Token != marker || d.Properties["os"] == "" {
		t.Fatal("incorrect Identify")
	}
}

func (f *fixture) ready(peer *wstest.Peer) {
	event(f.t, peer, 1, "READY", `{"session_id":"session","resume_gateway_url":"wss://gateway.discord.gg","user":{"id":"9"}}`)
}

func messageData(id int, fields string) string {
	return fmt.Sprintf(`{"id":"%d","channel_id":"42","author":{"id":"7","username":"user"},"content":"hello"%s}`, id, fields)
}

func event(t *testing.T, peer *wstest.Peer, seq int, name, data string) {
	sendPacket(t, peer, fmt.Sprintf(`{"op":0,"s":%d,"t":%q,"d":%s}`, seq, name, data))
}

func TestGatewayMessagesAndButtons(t *testing.T) {
	answers := make(chan struct{}, 4)
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "[]")
			return
		}
		var callback struct{ Type int }
		if r.Method != "POST" || !strings.HasPrefix(r.URL.Path, "/interactions/") || !strings.HasSuffix(r.URL.Path, "/press/callback") || json.NewDecoder(r.Body).Decode(&callback) != nil || callback.Type != 6 {
			t.Error("incorrect callback")
		}
		answers <- struct{}{}
		w.WriteHeader(204)
	})
	f.cfg.guild = "10"
	f.cfg.channels = " 42, 43 "
	c := f.start()
	peer, pkt := f.hello(60000)
	identify(t, pkt, 1<<12|1<<9|1<<15)
	f.ready(peer)
	for i, data := range []string{messageData(10, `,"guild_id":"11"`), strings.Replace(messageData(11, `,"guild_id":"10"`), "42", "44", 1), strings.Replace(messageData(12, ""), `"id":"7"`, `"id":"8","bot":true`, 1), strings.Replace(messageData(13, ""), `"id":"7"`, `"id":"9"`, 1)} {
		event(t, peer, i+2, "MESSAGE_CREATE", data)
	}
	data := strings.Replace(messageData(14, `,"timestamp":"2026-01-01T00:00:00Z","message_reference":{"message_id":"3"},"attachments":[{"filename":"a","content_type":"text/plain","size":4,"url":"https://cdn.example/`+marker+`"}]`), `"username":"user"`, `"username":"user","global_name":"Reader"`, 1)
	event(t, peer, 6, "MESSAGE_CREATE", data)
	m := c.NextInbound()
	if m.ID != "14" || m.Chat.ID != "42" || *m.Chat.Type != "private" || m.Sender != (p.Sender{ID: "7", Name: "Reader"}) || m.Text != "hello" || m.Time != "2026-01-01T00:00:00Z" || m.ReplyTo == nil || *m.ReplyTo != "3" || !slices.Equal(m.Attachments, []p.Attachment{{Name: "a", MIME: "text/plain", Size: 4, Ref: "https://cdn.example/[redacted]"}}) {
		t.Fatal("DM fields")
	}
	c.Ack(m.ID)
	event(t, peer, 7, "MESSAGE_CREATE", strings.Replace(messageData(15, `,"guild_id":"10"`), "42", "43", 1))
	m = c.NextInbound()
	if m.ID != "15" || *m.Chat.Type != "group" || m.Sender.Name != "user" {
		t.Fatal(m)
	}
	c.Ack(m.ID)
	event(t, peer, 8, "INTERACTION_CREATE", `{"type":3,"id":"19","token":"press","data":{"custom_id":12}}`)
	for i, custom := range []string{"invalid", "delivery\x1fbutton", "delivery\x1fbutton"} {
		data, _ := json.Marshal(map[string]any{"type": 3, "id": strconv.Itoa(16 + i), "guild_id": "10", "channel_id": "43", "member": map[string]any{"user": map[string]any{"id": "7", "bot": i == 1}}, "token": "press", "data": map[string]any{"component_type": 2, "custom_id": custom}})
		event(t, peer, 9+i, "INTERACTION_CREATE", string(data))
	}
	m = c.NextInbound()
	if m.ID != "18" || m.Action == nil || *m.Action != (p.Action{DeliveryID: "delivery", ButtonID: "button"}) {
		t.Fatal(m)
	}
	for range 4 {
		select {
		case <-answers:
		case <-time.After(time.Second):
			t.Fatal("missing callback")
		}
	}
}

func TestRestartHistoryAndContiguousAcks(t *testing.T) {
	var pages atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/channels/42/messages" || r.Method != "GET" || r.URL.Query().Get("limit") != "100" {
			t.Error("history request")
		}
		after, _ := strconv.Atoi(r.URL.Query().Get("after"))
		var msgs []json.RawMessage
		for i := min(202, after+100); i > after; i-- {
			msgs = append(msgs, json.RawMessage(messageData(i, "")))
		}
		pages.Add(1)
		json.NewEncoder(w).Encode(msgs)
	})
	c := f.start()
	peer, pkt := f.hello(60000)
	identify(t, pkt, 1<<12)
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(99, `,"guild_id":"10"`))
	event(t, peer, 3, "MESSAGE_CREATE", messageData(100, ""))
	m := c.NextInbound()
	if m.ID != "100" {
		t.Fatal(m)
	}
	c.Ack(m.ID)
	waitCursor(t, f, "100")
	c.Restart()
	c.Handshake()
	peer, _ = f.hello(60000)
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(202, ""))
	event(t, peer, 3, "MESSAGE_CREATE", messageData(203, ""))
	for i := 101; i <= 203; i++ {
		m = c.NextInbound()
		if m.ID != strconv.Itoa(i) {
			t.Fatalf("received %s, want %d", m.ID, i)
		}
		switch {
		case i < 200:
		case i == 200:
			c.Ack("103")
			c.Ack("101")
			waitCursor(t, f, "101")
			c.Ack("102")
			waitCursor(t, f, "103")
			for n := 104; n <= 200; n++ {
				c.Ack(strconv.Itoa(n))
			}
		default:
			c.Ack(m.ID)
		}
	}
	if pages.Load() != 2 || len(c.Stored()) != 104 {
		t.Fatal("history pages or replay", pages.Load(), len(c.Stored()))
	}
}

func waitCursor(t *testing.T, f *fixture, want string) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case id := <-f.committed:
			if id != want {
				continue
			}
			state, err := loadState(f.cfg.stateDir)
			if err != nil || state["42"].Last != want {
				t.Fatal("incorrect durable cursor", state, err)
			}
			return
		case <-timer.C:
			t.Fatal("missing commit", want)
		}
	}
}

func TestReconnectResumeInvalidSession(t *testing.T) {
	f := setup(t, nil)
	var invalidWait atomic.Int64
	var regionalDial atomic.Bool
	f.configure = func(s *service) {
		dial := s.dialGateway
		s.dialGateway = func(ctx context.Context, addr string, h http.Header) (*ws.Conn, error) {
			if strings.HasPrefix(addr, "wss://gateway-us-east1-b.discord.gg?") {
				regionalDial.Store(true)
			}
			return dial(ctx, addr, h)
		}
		s.invalidSessionDelay = func() time.Duration { return 3 * time.Second }
		s.backoff.Sleep = func(ctx context.Context, d time.Duration) error {
			invalidWait.Store(int64(d))
			f.elapsed.Add(int64(d))
			return ctx.Err()
		}
	}
	c := f.start()
	peer, _ := f.hello(60000)
	event(t, peer, 1, "READY", `{"session_id":"session","resume_gateway_url":"wss://gateway-us-east1-b.discord.gg","user":{"id":"9"}}`)
	event(t, peer, 12, "IGNORED", "null")
	peer.Close()
	// Drain SDK status frames while the test drives gateway connections.
	drained := make(chan struct{})
	go func() { defer close(drained); c.NextInbound() }()
	peer, pkt := f.hello(60000)
	var resume struct {
		Token, Session string
		Seq            int
		SessionID      string `json:"session_id"`
	}
	if !regionalDial.Load() || pkt.Op != 6 || json.Unmarshal(pkt.D, &resume) != nil || resume.Token != marker || resume.Seq != 12 || resume.SessionID != "session" {
		t.Fatal("incorrect Resume")
	}
	event(t, peer, 13, "RESUMED", "{}")
	sendPacket(t, peer, `{"op":9,"d":false}`)
	peer, pkt = f.hello(60000)
	identify(t, pkt, 1<<12)
	if got := time.Duration(invalidWait.Load()); got != 3*time.Second {
		t.Fatalf("invalid session delay %v", got)
	}
	if got := time.Duration(f.elapsed.Load()); got < 5*time.Second {
		t.Fatalf("two Identifies in %v", got)
	}
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(50, ""))
	<-drained
	wire := f.wire.String()
	for _, state := range []string{"connected", "reconnecting"} {
		if !strings.Contains(wire, `"state":"`+state+`"`) {
			t.Fatal("missing status", state)
		}
	}
}

func TestHeartbeatACKAndMissingACK(t *testing.T) {
	f := setup(t, nil)
	c := f.start()
	done := make(chan struct{})
	go func() { defer close(done); c.NextInbound() }()
	peer, pkt := f.hello(80)
	identify(t, pkt, 1<<12)
	f.ready(peer)
	for i := 0; i < 2; i++ {
		beat := readPacket(t, peer)
		if beat.Op != 1 {
			t.Fatal("missing heartbeat")
		}
		if i == 0 {
			sendPacket(t, peer, `{"op":11,"d":null}`)
		}
	}
	peer, pkt = f.hello(60000)
	if pkt.Op != 6 {
		t.Fatal("did not resume after missed ACK")
	}
	event(t, peer, 2, "RESUMED", "{}")
	event(t, peer, 3, "MESSAGE_CREATE", messageData(50, ""))
	<-done
}

func TestFailedRegionalResumeIdentifiesOnDefaultGateway(t *testing.T) {
	for _, failure := range []string{"dial refused", "disconnected before RESUMED", "invalid session d:true"} {
		t.Run(failure, func(t *testing.T) {
			const regional = "gateway-us-east1-b.discord.gg"
			f := setup(t, nil)
			hosts := make(chan string, 8)
			f.configure = func(s *service) {
				dial := s.dialGateway
				refused := 0
				s.dialGateway = func(ctx context.Context, addr string, h http.Header) (*ws.Conn, error) {
					u, err := url.Parse(addr)
					if err != nil {
						return nil, err
					}
					hosts <- u.Host
					if failure == "dial refused" && u.Host == regional && refused < 2 {
						refused++
						return nil, errors.New("regional gateway unavailable")
					}
					return dial(ctx, addr, h)
				}
			}
			core := f.start()
			peer, _ := f.hello(60000)
			event(t, peer, 1, "READY", `{"session_id":"session","resume_gateway_url":"wss://gateway-us-east1-b.discord.gg","user":{"id":"9"}}`)
			sendPacket(t, peer, `{"op":7}`)
			done := make(chan struct{})
			go func() { defer close(done); core.NextInbound() }()
			if failure != "dial refused" {
				// One unconfirmed Resume keeps the session; the second one gives it up.
				for range 2 {
					peer, pkt := f.hello(60000)
					if pkt.Op != 6 {
						t.Errorf("regional gateway received op %d, want Resume", pkt.Op)
					}
					if failure == "invalid session d:true" {
						sendPacket(t, peer, `{"op":9,"d":true}`)
					} else {
						peer.Close()
					}
				}
			}
			peer, pkt := f.hello(60000)
			f.ready(peer)
			event(t, peer, 2, "MESSAGE_CREATE", messageData(50, ""))
			<-done
			if len(hosts) != 4 {
				t.Fatalf("got %d gateway dials, want 4", len(hosts))
			}
			if got := []string{<-hosts, <-hosts, <-hosts, <-hosts}; !slices.Equal(got, []string{"gateway.discord.gg", regional, regional, "gateway.discord.gg"}) {
				t.Errorf("dialled %v", got)
			}
			identify(t, pkt, 1<<12)
		})
	}
}

func TestSingleFailedResumeKeepsSession(t *testing.T) {
	const regional = "gateway-us-east1-b.discord.gg"
	f := setup(t, nil)
	hosts := make(chan string, 8)
	var refuse atomic.Bool
	f.configure = func(s *service) {
		dial := s.dialGateway
		s.dialGateway = func(ctx context.Context, addr string, h http.Header) (*ws.Conn, error) {
			u, err := url.Parse(addr)
			if err != nil {
				return nil, err
			}
			hosts <- u.Host
			if u.Host == regional && refuse.CompareAndSwap(true, false) {
				return nil, errors.New("regional gateway unavailable")
			}
			return dial(ctx, addr, h)
		}
	}
	core := f.start()
	peer, _ := f.hello(60000)
	event(t, peer, 1, "READY", `{"session_id":"session","resume_gateway_url":"wss://gateway-us-east1-b.discord.gg","user":{"id":"9"}}`)
	done := make(chan struct{})
	go func() { defer close(done); core.NextInbound() }()
	// The second round shows that RESUMED reset the count: one more refusal still resumes.
	for i := range 2 {
		refuse.Store(true)
		sendPacket(t, peer, `{"op":7}`)
		var pkt packet
		peer, pkt = f.hello(60000)
		if pkt.Op != 6 {
			t.Fatalf("round %d: got op %d after one refused Resume, want Resume", i, pkt.Op)
		}
		event(t, peer, 2+i, "RESUMED", "{}")
	}
	event(t, peer, 4, "MESSAGE_CREATE", messageData(50, ""))
	<-done
	if len(hosts) != 5 {
		t.Fatalf("got %d gateway dials, want 5", len(hosts))
	}
	if got := []string{<-hosts, <-hosts, <-hosts, <-hosts, <-hosts}; !slices.Equal(got, []string{"gateway.discord.gg", regional, regional, regional, regional}) {
		t.Errorf("dialled %v", got)
	}
}

func TestFreshSessionRecoversServerHistory(t *testing.T) {
	var reads atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") == "1" {
			fmt.Fprint(w, "[]")
			return
		}
		if r.URL.Query().Get("after") == "0" {
			fmt.Fprint(w, "[]")
			return
		}
		if r.URL.Path != "/channels/42/messages" || r.URL.Query().Get("after") != "100" {
			t.Error("history cursor")
		}
		reads.Add(1)
		fmt.Fprintf(w, "[%s]", messageData(101, ""))
	})
	f.cfg.guild = "10"
	f.cfg.channels = "42"
	c := f.start()
	peer, _ := f.hello(60000)
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(100, `,"guild_id":"10"`))
	first := c.NextInbound()
	c.Ack(first.ID)
	waitCursor(t, f, "100")
	next := make(chan p.Inbound, 1)
	go func() { next <- c.NextInbound() }()
	sendPacket(t, peer, `{"op":7,"d":null}`)
	peer, pkt := f.hello(60000)
	if pkt.Op != 6 {
		t.Fatal("reconnect did not resume")
	}
	event(t, peer, 3, "RESUMED", "{}")
	sendPacket(t, peer, `{"op":9,"d":true}`)
	peer, pkt = f.hello(60000)
	if pkt.Op != 6 {
		t.Fatal("resumable session discarded")
	}
	sendPacket(t, peer, `{"op":9,"d":false}`)
	peer, pkt = f.hello(60000)
	identify(t, pkt, 1<<12|1<<9|1<<15)
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(102, `,"guild_id":"10"`))
	replayed := <-next
	if replayed.ID != "101" || *replayed.Chat.Type != "group" || reads.Load() != 1 {
		t.Fatal("incorrect server history", replayed)
	}
	c.Ack(replayed.ID)
	if live := c.NextInbound(); live.ID != "102" {
		t.Fatal(live)
	}
}

func TestUnacknowledgedFirstDMReplaysAfterRestart(t *testing.T) {
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") != "99" {
			t.Error("first unacknowledged message was skipped")
		}
		fmt.Fprintf(w, "[%s]", messageData(100, ""))
	})
	c := f.start()
	peer, _ := f.hello(60000)
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(100, ""))
	m := c.NextInbound()
	c.Restart()
	c.Handshake()
	peer, _ = f.hello(60000)
	f.ready(peer)
	replay := c.NextInbound()
	if replay.ID != m.ID || len(c.Stored()) != 1 {
		t.Fatal("first DM was lost or duplicated")
	}
}

func TestInaccessibleHistoryAllowsOtherChatsToContinue(t *testing.T) {
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/channels/42/messages" {
			t.Error("read a removed server channel")
		}
		w.WriteHeader(http.StatusForbidden)
	})
	s := f.service()
	if err := s.save("42", cursor{Last: "100"}); err != nil {
		t.Fatal(err)
	}
	if err := s.save("43", cursor{Last: "300", Guild: "10"}); err != nil {
		t.Fatal(err)
	}
	c := f.start()
	peer, _ := f.hello(60000)
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", strings.Replace(messageData(200, ""), "42", "44", 1))
	if m := c.NextInbound(); m.ID != "200" {
		t.Fatal(m)
	}
	state, err := loadState(f.cfg.stateDir)
	if err != nil || state["42"].Last != "100" || state["43"].Last != "300" {
		t.Fatal(state, err)
	}
}

func TestGatewayOrigin(t *testing.T) {
	f := setup(t, nil)
	s := f.service()
	for _, addr := range []string{"wss://discord.gg", "wss://one.two.discord.gg", "wss://bad_name.discord.gg", "wss://\u212A.discord.gg", "wss://-bad.discord.gg", "wss://bad-.discord.gg", "wss://[gateway.discord.gg]", "wss://regional.discord.gg:8080", "https://regional.discord.gg", "ws://gateway.discord.gg", "wss://gateway.discord.gg.example", "wss://gateway.discord.gg:444", "wss://x.discord.gg.", "wss://evil.com/.discord.gg", "wss://" + marker + "@gateway.discord.gg"} {
		if _, err := gatewayURL(addr); err == nil || strings.Contains(err.Error(), marker) {
			t.Fatal("unsafe gateway accepted or exposed")
		}
	}
	for _, addr := range []string{"wss://gateway.discord.gg/", "wss://gateway-us-east1-b.discord.gg", "wss://Gateway-US.discord.gg", "wss://regional.discord.gg:443/"} {
		if _, err := gatewayURL(addr); err != nil {
			t.Errorf("%s: %v", addr, err)
		}
	}
	if s.client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("proxy enabled")
	}
}

func TestParseConversationIDs(t *testing.T) {
	for _, tc := range []struct{ guild, channels, bad string }{
		{"", "", ""}, {"10", " 42, 43 ", ""}, {"10", "", ""},
		{"invalid", "42", "ADA_DISCORD_GUILD"}, {"0", "", "ADA_DISCORD_GUILD"},
		{"10", "42,nope", "ADA_DISCORD_CHANNELS"}, {"10", "42,", "ADA_DISCORD_CHANNELS"},
		{"10", "01", "ADA_DISCORD_CHANNELS"}, {"", "0", "ADA_DISCORD_CHANNELS"},
	} {
		ids, err := parseConversationIDs(tc.guild, tc.channels)
		if tc.bad == "" {
			if err != nil || tc.channels != "" && (!ids["42"] || !ids["43"]) {
				t.Fatal(tc, ids, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), tc.bad) {
			t.Fatal(tc, err)
		}
	}
}

func TestSessionStartLimitWaitsBeforeIdentify(t *testing.T) {
	f := setup(t, nil)
	f.startLimit = map[string]int{"remaining": 0, "reset_after": 12345}
	core := f.start()
	done := make(chan struct{})
	go func() { defer close(done); core.NextInbound() }()
	peer, pkt := f.hello(60000)
	identify(t, pkt, 1<<12)
	if time.Duration(f.elapsed.Load()) != 12345*time.Millisecond || !strings.Contains(f.wire.String(), `"state":"reconnecting"`) || !strings.Contains(f.logs.String(), "session start limit") {
		t.Errorf("unexpected quota wait %v, logs %s", time.Duration(f.elapsed.Load()), f.logs.String())
	}
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(100, ""))
	<-done
}

func TestSessionStartLimitRejectsNegativeResetBeforeDial(t *testing.T) {
	f := setup(t, nil)
	f.startLimit = map[string]int{"remaining": 0, "reset_after": -1}
	s := f.service()
	s.dialGateway = func(context.Context, string, http.Header) (*ws.Conn, error) {
		t.Fatal("dialled with a negative reset interval")
		return nil, nil
	}
	if _, err := s.connect(t.Context(), &session{}); err == nil || !strings.Contains(err.Error(), "invalid Discord session start limit") {
		t.Fatalf("unexpected connect error: %v", err)
	}
}

func TestReconnectBackoffAfterCatchUp(t *testing.T) {
	for _, uptime := range []time.Duration{29 * time.Second, 30 * time.Second} {
		t.Run(uptime.String(), func(t *testing.T) {
			var reads atomic.Int32
			f := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if reads.Add(1) <= 2 {
					w.WriteHeader(500)
					return
				}
				fmt.Fprint(w, "[]")
			})
			waits := make(chan time.Duration, 8)
			expectWait := func(want time.Duration) {
				t.Helper()
				select {
				case d := <-waits:
					if d != want {
						t.Fatalf("reconnect waited %v, want %v", d, want)
					}
				case <-time.After(time.Second):
					t.Fatal("missing reconnect wait")
				}
			}
			f.configure = func(s *service) {
				s.backoff = sdk.Backoff{Base: time.Second, Max: time.Minute, Sleep: func(ctx context.Context, d time.Duration) error {
					waits <- d
					f.elapsed.Add(int64(d))
					return ctx.Err()
				}}
			}
			if err := f.service().save("42", cursor{Last: "10"}); err != nil {
				t.Fatal(err)
			}
			c := f.start()
			nextInbound := func() <-chan string {
				in := make(chan string, 1)
				go func() {
					defer close(in)
					in <- c.NextInbound().ID
				}()
				return in
			}
			in := nextInbound()
			for _, want := range []time.Duration{time.Second, 2 * time.Second} {
				peer, pkt := f.hello(60000)
				identify(t, pkt, 1<<12)
				f.ready(peer)
				expectWait(want)
			}
			peer, pkt := f.hello(60000)
			identify(t, pkt, 1<<12)
			f.ready(peer)
			event(t, peer, 2, "MESSAGE_CREATE", messageData(101, ""))
			if got := <-in; got != "101" {
				t.Fatal(got)
			}
			c.Ack("101")
			waitCursor(t, f, "101")
			in = nextInbound()
			f.elapsed.Add(int64(uptime))
			peer.Close()
			peer, pkt = f.hello(60000)
			want := 4 * time.Second
			if uptime >= 30*time.Second {
				want = time.Second
			}
			expectWait(want)
			if pkt.Op != 6 {
				t.Fatalf("after %v expected Resume, got op %d", uptime, pkt.Op)
			}
			event(t, peer, 3, "RESUMED", "{}")
			sendPacket(t, peer, `{"op":7}`)
			peer, pkt = f.hello(60000)
			expectWait(time.Second)
			if pkt.Op != 6 {
				t.Fatalf("expected another Resume, got op %d", pkt.Op)
			}
			event(t, peer, 4, "RESUMED", "{}")
			event(t, peer, 5, "MESSAGE_CREATE", messageData(102, ""))
			if got := <-in; got != "102" {
				t.Fatalf("after Resume received %q", got)
			}
		})
	}
}

func TestTerminalGatewayCloseLeavesDeliveriesRunning(t *testing.T) {
	for _, tc := range []struct {
		code         int
		state, cause string
	}{
		{4004, "unauthorised", "authentication failed"},
		{4010, "stopped", "invalid shard"},
		{4011, "stopped", "sharding required"},
		{4012, "stopped", "invalid API version"},
		{4013, "stopped", "invalid intents"},
		{4014, "stopped", "enable the Message Content intent in the developer portal or remove ADA_DISCORD_GUILD/ADA_DISCORD_CHANNELS"},
	} {
		t.Run(strconv.Itoa(tc.code), func(t *testing.T) {
			f := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/channels/42/messages" {
					t.Error("unexpected delivery request")
				}
				fmt.Fprint(w, `{"id":"71"}`)
			})
			core := f.start()
			peer, _ := f.hello(60000)
			if err := peer.Send(0x88, []byte{byte(tc.code >> 8), byte(tc.code)}); err != nil {
				t.Fatal(err)
			}
			until := time.Now().Add(time.Second)
			for !strings.Contains(f.wire.String(), `"state":"`+tc.state+`"`) {
				if time.Now().After(until) {
					t.Fatalf("missing status %s, log %s", tc.state, f.logs.String())
				}
				time.Sleep(time.Millisecond)
			}
			ok, failed := core.Deliver(p.Deliver{ID: "deliver", ChatID: "42", Text: "hello"})
			if ok == nil || ok.MessageID != "71" || failed != nil {
				t.Fatal(ok, failed)
			}
			if !strings.Contains(f.logs.String(), tc.cause) {
				t.Fatal(f.logs.String())
			}
			select {
			case peer := <-f.peers:
				peer.Close()
				t.Fatal("reconnected after fatal close")
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func TestConfiguredChannelStartingPositions(t *testing.T) {
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error(r.Method)
		}
		if r.URL.Query().Get("limit") == "1" {
			switch r.URL.Path {
			case "/channels/42/messages":
				fmt.Fprint(w, `[{"id":"100"}]`)
			case "/channels/43/messages":
				fmt.Fprint(w, "[]")
			default:
				t.Error("reseeded known channel", r.URL.Path)
			}
			return
		}
		if r.URL.Path == "/channels/42/messages" && r.URL.Query().Get("after") == "100" {
			fmt.Fprintf(w, "[%s]", messageData(101, ""))
			return
		}
		fmt.Fprint(w, "[]")
	})
	f.cfg.guild, f.cfg.channels = "10", "42,43,44"
	if err := f.service().save("44", cursor{Last: "55", Guild: "10"}); err != nil {
		t.Fatal(err)
	}
	c := f.start()
	peer, _ := f.hello(60000)
	f.ready(peer)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(102, `,"guild_id":"10"`))
	if m := c.NextInbound(); m.ID != "101" {
		t.Fatalf("missed first server message during gap: %s", m.ID)
	}
	state, err := loadState(f.cfg.stateDir)
	if err != nil || state["42"] != (cursor{"100", "10"}) || state["43"] != (cursor{"0", "10"}) || state["44"] != (cursor{"55", "10"}) {
		t.Fatal("incorrect durable starting positions", state, err)
	}
}

func TestSlowInteractionCallbackDoesNotBlockHeartbeat(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(204)
	})
	f.start()
	defer close(release)
	peer, _ := f.hello(60000)
	event(t, peer, 1, "INTERACTION_CREATE", `{"type":3,"id":"19","token":"press"}`)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("missing callback")
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	for range 2 {
		sendPacket(t, peer, `{"op":1}`)
		if pkt := readPacket(t, peer); pkt.Op != 1 {
			t.Fatal("missing heartbeat")
		}
	}
}

func TestFullCallbackQueueStillDispatchesButton(t *testing.T) {
	f := setup(t, nil)
	s := f.service()
	conn, err := s.dialGateway(t.Context(), "wss://gateway.discord.gg?v=10&encoding=json", nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := <-f.peers
	defer peer.Close()
	defer conn.Close(1000, "")
	callbacks := make(chan callback, 1)
	callbacks <- callback{ID: "18"}
	events := make(chan gatewayEvent, 2)
	event(t, peer, 1, "INTERACTION_CREATE", `{"type":3,"id":"19","token":"press","channel_id":"42","user":{"id":"7"},"data":{"component_type":2,"custom_id":"delivery\u001fbutton"}}`)
	event(t, peer, 2, "MESSAGE_CREATE", messageData(20, ""))
	sendPacket(t, peer, `{"op":7}`)
	err = s.pump(t.Context(), conn, &heartbeat{}, events, callbacks)
	if len(events) != 2 || (<-events).T != "INTERACTION_CREATE" || (<-events).T != "MESSAGE_CREATE" {
		t.Fatalf("gateway failed to dispatch the press and the following message: %v", err)
	}
	if len(callbacks) != 1 || (<-callbacks).ID != "18" || !strings.Contains(f.logs.String(), "dropping acknowledgement") {
		t.Fatalf("expected to drop and log the new acknowledgement: %s", f.logs.String())
	}
}

func TestRedirectAndFailedCallbackKeepCredentialsPrivate(t *testing.T) {
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }))
	defer target.Close()
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/"+marker, http.StatusTemporaryRedirect)
	})
	s := f.service()
	s.answer(t.Context(), callback{ID: "100", Type: 3, Token: marker})
	if reached.Load() || !strings.Contains(f.logs.String(), "307") {
		t.Fatal("callback redirect was followed")
	}
	s.apiBase = target.URL
	target.Close()
	err := s.call(t.Context(), "POST", "/interactions/100/"+marker+"/callback", nil, nil)
	if err == nil || strings.Contains(err.Error(), marker) {
		t.Fatal("credential-bearing transport error")
	}
}

func TestDeliver(t *testing.T) {
	var count atomic.Int32
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		var body struct {
			Content  string
			Mentions map[string][]string `json:"allowed_mentions"`
			Reply    map[string]any      `json:"message_reference"`
			Rows     []struct {
				Type       int
				Components []struct {
					Type, Style int
					Label       string
					ID          string `json:"custom_id"`
				}
			} `json:"components"`
		}
		if r.Method != "POST" || r.URL.Path != "/channels/42/messages" || json.NewDecoder(r.Body).Decode(&body) != nil || body.Mentions["parse"] == nil || len(body.Mentions["parse"]) != 0 || body.Reply["message_id"] != "5" || body.Reply["fail_if_not_exists"] != false {
			t.Error("incorrect delivery envelope")
		}
		if n == 1 {
			if body.Content != strings.Repeat("x", 2000) || len(body.Rows) != 0 {
				t.Error("first part")
			}
		} else {
			if body.Content != "end" || len(body.Rows) != 5 {
				t.Error("last part")
				return
			}
			for i, row := range body.Rows {
				if row.Type != 1 || len(row.Components) != 5 {
					t.Error("row")
				}
				for j, b := range row.Components {
					idx := 5*i + j
					style := 2
					if idx == 0 {
						style = 1
					}
					if b.Type != 2 || b.Style != style || b.Label != "label" || b.ID != "delivery\x1f"+strconv.Itoa(idx) {
						t.Error("button", idx)
					}
				}
			}
		}
		fmt.Fprintf(w, `{"id":"%d"}`, n)
	})
	s := f.service()
	reply := "5"
	d := p.Deliver{ID: "delivery", ChatID: "42", ReplyTo: &reply, Text: strings.Repeat("x", 2000) + strings.Repeat(" ", 2000) + "end"}
	for i := range 25 {
		d.Buttons = append(d.Buttons, p.Button{Label: "label", ID: strconv.Itoa(i)})
	}
	id, err := s.Deliver(t.Context(), d)
	if err != nil || id != "2" {
		t.Fatal(id, err)
	}
	for _, change := range []func(*p.Deliver){func(d *p.Deliver) { d.Buttons = append(d.Buttons, p.Button{}) }, func(d *p.Deliver) { d.ID = strings.Repeat("x", 100) }, func(d *p.Deliver) { d.Text = " \n " }} {
		invalid := d
		change(&invalid)
		_, err = s.Deliver(t.Context(), invalid)
		var de *sdk.DeliveryError
		if !errors.As(err, &de) || de.Code != p.CodeBadRequest || count.Load() != 2 {
			t.Fatal("preflight", err)
		}
	}
}

func TestRESTFailures(t *testing.T) {
	for _, tc := range []struct {
		status     int
		code, body string
		delay      time.Duration
	}{{429, p.CodeRateLimited, `{"retry_after":1.25}`, 1250 * time.Millisecond}, {429, p.CodeRateLimited, "{}", 2500 * time.Millisecond}, {400, p.CodeBadRequest, "", 0}, {401, p.CodeUnauthorised, "", 0}, {403, p.CodeBadRequest, "", 0}, {404, p.CodeBadRequest, "", 0}, {500, p.CodeInternal, "", 0}, {307, p.CodeInternal, "", 0}} {
		f := setup(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "2.5")
			w.Header().Set("Location", "/"+marker)
			w.WriteHeader(tc.status)
			fmt.Fprint(w, tc.body+marker)
		})
		_, err := f.service().Deliver(t.Context(), p.Deliver{ChatID: "42", Text: "text"})
		var de *sdk.DeliveryError
		if !errors.As(err, &de) || de.Code != tc.code || de.RetryAfter != tc.delay || strings.Contains(err.Error(), marker) {
			t.Fatal(tc.status, err)
		}
	}
}

func TestMultipartRetryUsesStableNonces(t *testing.T) {
	var attempts atomic.Int32
	sent := make(chan string, 4)
	f := setup(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Nonce   string
			Enforce bool `json:"enforce_nonce"`
		}
		i := int(attempts.Add(1)) - 1
		if json.NewDecoder(r.Body).Decode(&body) != nil || !body.Enforce || body.Nonce == "" || len(body.Nonce) > 25 {
			t.Error("missing valid Discord nonce")
		}
		sent <- body.Nonce
		if i == 1 {
			w.WriteHeader(429)
			return
		}
		fmt.Fprint(w, `{"id":"123"}`)
	})
	s := f.service()
	d := p.Deliver{ID: strings.Repeat("a", 128), ChatID: "42", Text: strings.Repeat("b", 2001)}
	if _, err := s.Deliver(t.Context(), d); err == nil {
		t.Fatal("expected rate limit")
	}
	if _, err := s.Deliver(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	first, second, retryFirst, retrySecond := <-sent, <-sent, <-sent, <-sent
	if first == second || first != retryFirst || second != retrySecond {
		t.Fatal("retry used incorrect nonces")
	}
}

func TestManifest(t *testing.T) {
	m, err := sdk.LoadManifest("channel.json")
	if err != nil || m.Name != "discord" || m.Label != "Discord" || m.SecretEnv != "ADA_DISCORD_TOKEN" || !slices.Equal(m.Hosts, []string{"discord.com", "gateway.discord.gg", "*.discord.gg"}) {
		t.Fatal(m, err)
	}
}
