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
	"unicode/utf8"

	"github.com/cristiangirlea/adanext-channels/sdk"
	"github.com/cristiangirlea/adanext-channels/sdk/fakecore"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const marker = "test-only-redaction-marker"

type capture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capture) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(b)
}

func (c *capture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

type handler func(http.ResponseWriter, *http.Request, map[string]any)

func api(t *testing.T, methods map[string]handler) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, ok := strings.CutPrefix(r.URL.Path, "/bot"+marker+"/")
		var args map[string]any
		if !ok || r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&args) != nil {
			t.Error("unexpected Telegram API request")
			http.Error(w, "bad request", 400)
			return
		}
		if method == "getUpdates" {
			allowed, _ := json.Marshal(args["allowed_updates"])
			if args["timeout"] != float64(30) || string(allowed) != `["message","callback_query"]` {
				t.Error("incorrect long poll parameters")
			}
		}
		switch {
		case methods[method] != nil:
			methods[method](w, r, args)
		case method == "getMe":
			fmt.Fprint(w, `{"ok":true,"result":{"id":1}}`)
		case method == "getUpdates":
			<-r.Context().Done()
		default:
			t.Errorf("unexpected API method %q", method)
			http.Error(w, "unexpected", 400)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func start(t *testing.T, server *httptest.Server, groups bool) (*fakecore.Core, *capture, *capture) {
	t.Helper()
	logs, wire := new(capture), new(capture)
	t.Cleanup(func() {
		if strings.Contains(logs.String()+wire.String(), marker) {
			t.Error("channel exposed its token")
		}
	})
	core := fakecore.Start(t, func(ctx context.Context, in io.Reader, out io.Writer) error {
		s := newService(server.URL, marker, groups, logs)
		s.backoff.Sleep = func(ctx context.Context, d time.Duration) error {
			if d <= 0 || d > 5*time.Minute {
				t.Errorf("unexpected backoff %v", d)
			}
			return ctx.Err()
		}
		return sdk.Run(ctx, info, s, in, io.MultiWriter(wire, out), s.log)
	})
	if ready := core.Handshake(); ready.Channel.Name != "telegram" {
		t.Fatal(ready)
	}
	return core, logs, wire
}

func msg(id int, chatType string, bot bool, fields string) string {
	return fmt.Sprintf(`{"update_id":%d,"message":{"date":1700000000,"chat":{"id":-42,"type":%q},"from":{"id":7,"first_name":"Ada","last_name":"Lovelace","is_bot":%t},%s}}`, id, chatType, bot, fields)
}

func batch(updates ...string) handler {
	return func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		if _, ok := args["offset"]; ok {
			<-r.Context().Done()
			return
		}
		fmt.Fprintf(w, `{"ok":true,"result":[%s]}`, strings.Join(updates, ","))
	}
}

func TestInboundMediaAndReply(t *testing.T) {
	fields := []string{
		`"photo":[{"file_id":"big","file_size":90,"width":10,"height":10},{"file_id":"small","width":2,"height":2}]`,
		`"document":{"file_id":"doc","file_name":"x.pdf","file_size":12,"mime_type":"application/pdf"}`,
		`"audio":{"file_id":"audio","file_name":"x.mp3","file_size":3,"mime_type":"audio/mpeg"}`,
		`"video":{"file_id":"video","file_name":"x.mp4","file_size":4,"mime_type":"video/mp4"}`,
		`"voice":{"file_id":"voice","file_size":5,"mime_type":"audio/ogg"}`,
	}
	want := []p.Attachment{
		{Name: "photo.jpg", MIME: "image/jpeg", Size: 90, Ref: "big"},
		{Name: "x.pdf", MIME: "application/pdf", Size: 12, Ref: "doc"},
		{Name: "x.mp3", MIME: "audio/mpeg", Size: 3, Ref: "audio"},
		{Name: "x.mp4", MIME: "video/mp4", Size: 4, Ref: "video"},
		{Name: "voice.ogg", MIME: "audio/ogg", Size: 5, Ref: "voice"},
	}
	var updates []string
	for i, f := range fields {
		updates = append(updates, msg(i+1, "supergroup", false, f+`,"caption":"hello `+marker+`","reply_to_message":{"message_id":123}`))
	}
	t.Setenv("ADA_TELEGRAM_GROUPS", "1")
	c, _, _ := start(t, api(t, map[string]handler{"getUpdates": batch(updates...)}), groupsEnabled())
	for i, att := range want {
		in := c.NextInbound()
		if in.ID != strconv.Itoa(i+1) || in.Text != "hello [redacted]" || in.Time != "2023-11-14T22:13:20Z" ||
			in.Chat.ID != "-42" || in.Chat.Type == nil || *in.Chat.Type != "group" || in.Sender != (p.Sender{ID: "7", Name: "Ada Lovelace"}) ||
			in.ReplyTo == nil || *in.ReplyTo != "-42:123" || !slices.Equal(in.Attachments, []p.Attachment{att}) {
			t.Fatalf("incorrect mapping for %s", att.Ref)
		}
		c.Ack(in.ID)
	}
}

func TestReplayOffsetsAndSkips(t *testing.T) {
	updates := []string{
		msg(10, "group", false, `"text":"skip"`), msg(11, "private", false, `"text":"one"`),
		msg(12, "private", true, `"text":"skip"`), msg(13, "private", false, `"text":"two"`),
		msg(14, "private", false, `"text":"three"`), `{"update_id":15,"channel_post":{}}`,
	}
	polls := make(chan int, 10)
	var confirmed atomic.Int64
	server := api(t, map[string]handler{"getUpdates": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		offset := -1
		if v, ok := args["offset"]; ok {
			offset = int(v.(float64))
			confirmed.Store(int64(offset))
		}
		polls <- offset
		first := max(0, int(confirmed.Load())-10)
		if first >= len(updates) {
			<-r.Context().Done()
			return
		}
		fmt.Fprintf(w, `{"ok":true,"result":[%s]}`, strings.Join(updates[first:], ","))
	}})
	c, logs, _ := start(t, server, false)
	ids := []string{"11", "13", "14"}
	for session := range 2 {
		if session == 1 {
			c.Restart()
			c.Handshake()
		}
		for _, id := range ids {
			if in := c.NextInbound(); in.ID != id || in.Chat.Type == nil || *in.Chat.Type != "private" {
				t.Fatal("incorrect private update", in.ID)
			}
		}
		if got := <-polls; got != -1 {
			t.Fatalf("initial offset %d", got)
		}
		c.Ack("14")
		c.Ack("11")
	}
	c.Ack("13")
	select {
	case offset := <-polls:
		if offset != 16 {
			t.Fatalf("confirmed offset %d", offset)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("missing confirmation")
	}
	var stored []string
	for _, in := range c.Stored() {
		stored = append(stored, in.ID)
	}
	if !slices.Equal(stored, ids) {
		t.Fatal(stored)
	}
	for _, id := range []string{"10", "12"} {
		if strings.Count(logs.String(), id) != 2 {
			t.Errorf("expected one skip log per appearance of %s", id)
		}
	}
}

func TestOnlySkippedUpdatesAdvance(t *testing.T) {
	server := api(t, map[string]handler{"getUpdates": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		switch args["offset"] {
		case nil:
			batch(msg(51, "supergroup", false, `"text":"skip"`))(w, r, args)
		case float64(52):
			fmt.Fprintf(w, `{"ok":true,"result":[%s]}`, msg(52, "private", false, `"text":"keep"`))
		default:
			t.Error("incorrect offset")
			<-r.Context().Done()
		}
	}})
	c, _, _ := start(t, server, false)
	if in := c.NextInbound(); in.ID != "52" || in.Text != "keep" {
		t.Fatal("wrong update")
	}
}

func TestCallbackAnswerFailureStillDelivers(t *testing.T) {
	callback := `{"update_id":9,"callback_query":{"id":"q","from":{"id":31,"username":"reader"},"message":{"chat":{"id":42,"type":"private"}},"data":"delivery\u001fyes"}}`
	var answered atomic.Bool
	server := api(t, map[string]handler{"getUpdates": batch(callback), "answerCallbackQuery": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		answered.Store(args["callback_query_id"] == "q")
		w.WriteHeader(400)
		fmt.Fprintf(w, `{"ok":false,"description":%q}`, marker)
	}})
	c, logs, _ := start(t, server, false)
	in := c.NextInbound()
	if !answered.Load() || !strings.Contains(logs.String(), "answerCallbackQuery") || in.ID != "9" || in.Chat.ID != "42" || in.Sender != (p.Sender{ID: "31", Name: "reader"}) || in.Text != "" || in.Action == nil || *in.Action != (p.Action{DeliveryID: "delivery", ButtonID: "yes"}) {
		t.Fatal("incorrect callback handling")
	}
}

func TestSkippedCallbacksAreAnswered(t *testing.T) {
	var updates []string
	for i, tc := range []struct {
		id, chat, data string
		bot            bool
	}{
		{"group", "group", "delivery" + buttonSeparator + "ok", false},
		{"bad-data", "private", "invalid", false},
		{"bot", "private", "delivery" + buttonSeparator + "ok", true},
	} {
		data, _ := json.Marshal(tc.data)
		updates = append(updates, fmt.Sprintf(`{"update_id":%d,"callback_query":{"id":%q,"from":{"id":7,"is_bot":%t},"message":{"chat":{"id":42,"type":%q}},"data":%s}}`, i+1, tc.id, tc.bot, tc.chat, data))
	}
	updates = append(updates, `{"update_id":4,"callback_query":{"id":"inline"}}`, msg(5, "private", false, `"text":"done"`))
	var answers capture
	server := api(t, map[string]handler{"getUpdates": batch(updates...), "answerCallbackQuery": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		fmt.Fprintln(&answers, args["callback_query_id"])
		fmt.Fprint(w, `{"ok":true,"result":true}`)
	}})
	c, _, _ := start(t, server, false)
	if in := c.NextInbound(); in.ID != "5" || answers.String() != "group\nbad-data\nbot\ninline\n" {
		t.Fatalf("first inbound %s, callback answers %q", in.ID, answers.String())
	}
}

func TestDeliveryWhitespaceAtSplit(t *testing.T) {
	first := strings.Repeat("a", 4095) + "\n"
	run := strings.Repeat("\U00020000", 1200)
	word := strings.Repeat("b", 3000) + " "
	for _, tc := range []struct {
		name, input string
		want        []string
	}{
		{"blank line before long run", first + "\n" + run, []string{first, run[:4096], run[4096:]}},
		{"split at space after blank line", first + "\n  " + word + word, []string{first, word, word}},
		{"trailing whitespace part", first + "\n\t", []string{first}},
		{"long leading whitespace", strings.Repeat("\u3000", 1400) + "end", []string{"end"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var accepted []string
			server := api(t, map[string]handler{"sendMessage": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
				part := args["text"].(string)
				if strings.TrimSpace(part) == "" || len(part) > 4096 {
					t.Errorf("invalid part of %d bytes", len(part))
				}
				mu.Lock()
				accepted = append(accepted, part)
				mu.Unlock()
				fmt.Fprint(w, `{"ok":true,"result":{"message_id":1}}`)
			}})
			c, _, _ := start(t, server, false)
			if _, failed := c.Deliver(p.Deliver{ID: "parts", ChatID: "42", Text: tc.input}); failed != nil {
				t.Fatal(failed)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(accepted) != len(tc.want) {
				t.Fatalf("sent %d parts, want %d", len(accepted), len(tc.want))
			}
			for i, want := range tc.want {
				if got := accepted[i]; got != want {
					t.Errorf("part %d differs: got %d bytes, want %d", i+1, len(got), len(want))
				}
			}
		})
	}
}

func TestDeliveryValidationBeforeSend(t *testing.T) {
	buttons := make([]p.Button, 101)
	for i := range buttons {
		buttons[i] = p.Button{ID: strconv.Itoa(i), Label: "OK"}
	}
	for _, tc := range []struct {
		name, text string
		buttons    []p.Button
	}{
		{"empty", "", nil}, {"whitespace", " \t\n", nil},
		{"too many buttons", strings.Repeat("x", 4097), buttons},
		{"blank button label", strings.Repeat("x", 4097), []p.Button{{ID: "ok", Label: "OK"}, {ID: "blank", Label: "  "}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := api(t, nil) // Any service call fails the test.
			s := newService(server.URL, marker, false, io.Discard)
			_, err := s.Deliver(t.Context(), p.Deliver{ID: "d", ChatID: "42", Text: tc.text, Buttons: tc.buttons})
			var failure *sdk.DeliveryError
			if !errors.As(err, &failure) || failure.Code != p.CodeBadRequest || tc.buttons == nil && failure.Message != "nothing to send" {
				t.Fatalf("unexpected failure: %v", err)
			}
		})
	}
}

func TestSplitReplyAndButtonLimit(t *testing.T) {
	var sends []map[string]any
	var mu sync.Mutex
	server := api(t, map[string]handler{"sendMessage": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		sends = append(sends, args)
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"chat":{"id":42}}}`, len(sends))
	}})
	c, _, _ := start(t, server, false)
	reply := "42:9"
	id := strings.Repeat("d", 32)
	d := p.Deliver{ID: id, ChatID: "42", ReplyTo: &reply, Text: strings.Repeat("a", 4095) + "界!", Buttons: []p.Button{{ID: strings.Repeat("b", 31), Label: "OK"}, {ID: "no", Label: "No"}}}
	sent, failed := c.Deliver(d)
	if failed != nil || sent.MessageID != "42:2" {
		t.Fatal(sent, failed)
	}
	d.ID += "d"
	_, failed = c.Deliver(d)
	if failed == nil || failed.Error.Code != p.CodeBadRequest || !strings.Contains(failed.Error.Message, "too long") {
		t.Fatal(failed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sends) != 2 || sends[0]["reply_markup"] != nil {
		t.Fatal("unexpected partial send or markup")
	}
	var joined string
	for _, req := range sends {
		part := req["text"].(string)
		params, _ := req["reply_parameters"].(map[string]any)
		if len(part) > 4096 || !utf8.ValidString(part) || req["parse_mode"] != nil || fmt.Sprint(req["chat_id"]) != "42" || params["message_id"] != float64(9) || params["allow_sending_without_reply"] != true {
			t.Fatal("invalid sendMessage")
		}
		joined += part
	}
	markup, _ := json.Marshal(sends[1]["reply_markup"])
	want := `{"inline_keyboard":[[{"callback_data":"` + id + `\u001f` + d.Buttons[0].ID + `","text":"OK"}],[{"callback_data":"` + id + `\u001fno","text":"No"}]]}`
	if joined != d.Text || string(markup) != want {
		t.Fatal("incorrect text or keyboard")
	}
}

func TestDeliveryErrors(t *testing.T) {
	for _, tc := range []struct {
		http, code int
		want       string
	}{
		{429, 0, p.CodeRateLimited}, {200, 429, p.CodeRateLimited},
		{401, 0, p.CodeUnauthorised}, {200, 401, p.CodeUnauthorised},
		{403, 0, p.CodeBadRequest}, {200, 403, p.CodeBadRequest},
		{400, 0, p.CodeBadRequest}, {404, 0, p.CodeInternal}, {502, 0, p.CodeInternal},
	} {
		t.Run(fmt.Sprintf("%d-%d", tc.http, tc.code), func(t *testing.T) {
			server := api(t, map[string]handler{"sendMessage": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
				w.WriteHeader(tc.http)
				fmt.Fprintf(w, `{"ok":false,"error_code":%d,"description":%q,"parameters":{"retry_after":3}}`, tc.code, marker)
			}})
			c, _, _ := start(t, server, false)
			_, failed := c.Deliver(p.Deliver{ID: "out", ChatID: "42", Text: "hi"})
			if failed == nil || failed.Error.Code != tc.want || tc.want == p.CodeRateLimited && (failed.RetryAfterMS == nil || *failed.RetryAfterMS != 3000) {
				t.Fatal("incorrect delivery failure")
			}
		})
	}
}

func TestReconnectStatus(t *testing.T) {
	var me, polls atomic.Int32
	server := api(t, map[string]handler{
		"getMe": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
			if me.Add(1) == 1 {
				http.Error(w, "unauthorized", 401)
				return
			}
			fmt.Fprint(w, `{"ok":true,"result":{"id":1}}`)
		},
		"getUpdates": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
			if polls.Add(1) == 1 {
				panic(http.ErrAbortHandler)
			}
			batch(msg(1, "private", false, `"text":"ok"`))(w, r, args)
		},
	})
	c, _, wire := start(t, server, false)
	c.NextInbound()
	if states := statusStates(t, wire); !slices.Equal(states, []string{"unauthorised", "connected", "reconnecting", "connected"}) {
		t.Fatal(states)
	}
}

func statusStates(t *testing.T, wire *capture) []string {
	t.Helper()
	var states []string
	for _, line := range strings.Split(strings.TrimSpace(wire.String()), "\n") {
		var f p.Status
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Fatal(err)
		}
		if f.Type == p.TypeStatus {
			states = append(states, f.State)
		}
	}
	return states
}

func TestPollingErrorsBackOffAndRecover(t *testing.T) {
	for _, tc := range []struct {
		method string
		code   int
		state  string
	}{
		{"getMe", 401, p.StateUnauthorised}, {"getMe", 404, p.StateUnauthorised},
		{"getUpdates", 404, p.StateUnauthorised}, {"getUpdates", 409, p.StateReconnecting},
	} {
		t.Run(fmt.Sprintf("%s-%d", tc.method, tc.code), func(t *testing.T) {
			updates := batch(msg(1, "private", false, `"text":"recovered"`))
			methods := map[string]handler{"getUpdates": updates}
			var calls atomic.Int32
			methods[tc.method] = func(w http.ResponseWriter, r *http.Request, args map[string]any) {
				if calls.Add(1) == 1 {
					http.Error(w, http.StatusText(tc.code), tc.code)
				} else if tc.method == "getMe" {
					fmt.Fprint(w, `{"ok":true,"result":{"id":1}}`)
				} else {
					updates(w, r, args)
				}
			}
			server := api(t, methods)
			var logs, wire capture
			var waits atomic.Int32
			c := fakecore.Start(t, func(ctx context.Context, in io.Reader, out io.Writer) error {
				s := newService(server.URL, marker, false, &logs)
				s.backoff.Sleep = func(ctx context.Context, delay time.Duration) error {
					if delay <= 0 {
						t.Errorf("retry delay %v", delay)
					}
					waits.Add(1)
					return ctx.Err()
				}
				return sdk.Run(ctx, info, s, in, io.MultiWriter(&wire, out), s.log)
			})
			c.Handshake()
			c.NextInbound()
			want := []string{tc.state, p.StateConnected}
			if tc.method == "getUpdates" {
				want = append([]string{p.StateConnected}, want...)
			}
			if states := statusStates(t, &wire); !slices.Equal(states, want) || waits.Load() != 1 {
				t.Errorf("states %v, retry waits %d", states, waits.Load())
			}
			if tc.code == 409 && (!strings.Contains(logs.String(), "409") || !strings.Contains(logs.String(), "deleteWebhook")) {
				t.Errorf("missing webhook diagnostic in %q", logs.String())
			}
		})
	}
}

func TestNetworkErrorSafeBeforeSDK(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	var logs bytes.Buffer
	s := newService(server.URL, marker, false, &logs)
	_, err := s.Deliver(t.Context(), p.Deliver{ID: "d", ChatID: "42", Text: "hello"})
	var failure *sdk.DeliveryError
	if !errors.As(err, &failure) || failure.Code != p.CodeInternal || strings.Contains(err.Error()+logs.String(), marker) {
		t.Fatal("unsafe or incorrect network error")
	}
}

func TestNoRedirect(t *testing.T) {
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }))
	defer target.Close()
	server := api(t, map[string]handler{"sendMessage": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		http.Redirect(w, r, target.URL, 307)
	}})
	c, _, _ := start(t, server, false)
	_, failed := c.Deliver(p.Deliver{ID: "d", ChatID: "42", Text: "private"})
	if reached.Load() || failed == nil || failed.Error.Code != p.CodeInternal {
		t.Fatal("redirect handling")
	}
}

func TestTransportIgnoresEnvironmentProxy(t *testing.T) {
	s := newService(apiBase, marker, false, io.Discard)
	transport, ok := s.client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil {
		t.Fatal("Telegram requests can be routed by an environment proxy")
	}
}

func TestMultipartRetryResumesAfterLastSuccessfulPart(t *testing.T) {
	var attempts atomic.Int32
	var accepted capture
	server := api(t, map[string]handler{"sendMessage": func(w http.ResponseWriter, r *http.Request, args map[string]any) {
		if attempts.Add(1) == 2 {
			w.WriteHeader(429)
			fmt.Fprint(w, `{"ok":false,"parameters":{"retry_after":1}}`)
			return
		}
		io.WriteString(&accepted, args["text"].(string))
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d}}`, attempts.Load())
	}})
	c, _, _ := start(t, server, false)
	d := p.Deliver{ID: "parts", ChatID: "42", Text: strings.Repeat("x", 4096) + "end"}
	if _, failed := c.Deliver(d); failed == nil || failed.Error.Code != p.CodeRateLimited {
		t.Fatal("expected rate limit on second part")
	}
	sent, failed := c.Deliver(d)
	if failed != nil || sent.MessageID != "42:3" || accepted.String() != d.Text {
		t.Fatal("retry did not resume at the failed part")
	}
}

func TestManifestAndMissingToken(t *testing.T) {
	m, err := sdk.LoadManifest("channel.json")
	if err != nil || m.Name != "telegram" || m.Label != "Telegram" || m.SecretEnv != "ADA_TELEGRAM_TOKEN" || !slices.Equal(m.Hosts, []string{"api.telegram.org"}) {
		t.Fatal(m, err)
	}
	t.Setenv("ADA_TELEGRAM_TOKEN", "")
	var stderr bytes.Buffer
	if serve(t.Context(), strings.NewReader(""), io.Discard, &stderr) != p.ExitFailed || !strings.Contains(stderr.String(), m.SecretEnv) {
		t.Fatal("missing token diagnostic")
	}
}

func TestCommitMonotonic(t *testing.T) {
	s := newService("", marker, false, io.Discard)
	for _, id := range []string{"300", "100", "300"} {
		if err := s.Commit(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	if s.offset != 301 {
		t.Fatal(s.offset)
	}
}
