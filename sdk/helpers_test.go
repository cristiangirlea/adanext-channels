package sdk

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSplit(t *testing.T) {
	for _, tc := range []struct {
		text  string
		limit int
		want  []string
	}{
		{"", 1, []string{""}}, {"abcd", 4, []string{"abcd"}}, {"ab\ncd ef", 6, []string{"ab\n", "cd ef"}},
		{"ab cd ef", 6, []string{"ab cd ", "ef"}}, {"ab界cd", 4, []string{"ab", "界c", "d"}},
		{"abcdef", 2, []string{"ab", "cd", "ef"}}, {"x", 0, nil}, {"abc界", 2, nil},
	} {
		if got := Split(tc.text, tc.limit); !slices.Equal(got, tc.want) {
			t.Errorf("Split(%q,%d) = %q, want %q", tc.text, tc.limit, got, tc.want)
		}
	}
}

func TestFormatting(t *testing.T) {
	const special = "_*[]()~`>#+-=|{}.!\\"
	var want strings.Builder
	for _, r := range special {
		want.WriteByte('\\')
		want.WriteRune(r)
	}
	if got := EscapeMarkdownV2("Hi " + special); got != "Hi "+want.String() {
		t.Fatal(got)
	}
	if got := PlainText(special); got != special {
		t.Fatal(got)
	}
}

func TestPosition(t *testing.T) {
	var p Position
	for _, id := range []string{"a", "b", "c", "d"} {
		p.Sent(id)
	}
	for _, ack := range []struct {
		id   string
		want []string
	}{
		{"unknown", nil}, {"c", nil}, {"c", nil}, {"a", []string{"a"}}, {"a", nil}, {"d", nil}, {"b", []string{"b", "c", "d"}},
	} {
		if got := p.Ack(ack.id); !slices.Equal(got, ack.want) {
			t.Fatalf("Ack(%s) = %v, want %v", ack.id, got, ack.want)
		}
	}
}

func TestBackoff(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 5 * time.Second}
	for i, w := range []time.Duration{1, 2, 4, 5, 5} {
		if got := b.Next(i); got != w*time.Second {
			t.Fatal(i, got)
		}
	}
	if b.Next(1<<30) != b.Max || b.Next(-1) != b.Base {
		t.Fatal("attempt bounds")
	}
	var slept time.Duration
	b.Sleep = func(_ context.Context, d time.Duration) error { slept = d; return nil }
	for _, tc := range []struct{ retry, want time.Duration }{{0, 2}, {4, 4}, {7, 7}} {
		if err := b.Wait(t.Context(), 1, tc.retry*time.Second); err != nil || slept != tc.want*time.Second {
			t.Fatal(err, slept)
		}
	}
	zero := Backoff{Sleep: b.Sleep}
	if err := zero.Wait(t.Context(), 0, time.Second); err != nil || slept != time.Second {
		t.Fatal(err, slept)
	}
	for _, cap := range []time.Duration{0, -1} {
		uncapped := Backoff{Base: time.Second, Max: cap}
		if got := uncapped.Next(3); got != 8*time.Second {
			t.Fatal(got)
		}
	}
	b.Sleep = nil
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := b.Wait(ctx, 0, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	b.Jitter = 0.5
	for range 100 {
		if delay := b.Next(1); delay < time.Second || delay > 3*time.Second {
			t.Fatal(delay)
		}
	}
}

func TestLogger(t *testing.T) {
	var out bytes.Buffer
	log := Logger{Writer: &out, Redact: []string{"sample", "sample-long", ""}}
	log.Printf("id=%s/%s\n\x1b\u0085\u202e\u2028\u2029", "sample-long", "sample")
	got := out.String()
	if !strings.Contains(got, `\u202e\u2028\u2029`) {
		t.Fatalf("%q", got)
	}
	if strings.Count(got, "[redacted]") != 2 || strings.ContainsAny(strings.TrimSuffix(got, "\n"), "\n\x1b\u0085") || strings.Contains(got, "long") || !strings.Contains(got, `\n`) {
		t.Fatalf("%q", got)
	}
}

func TestManifest(t *testing.T) {
	base := Manifest{Name: "0-echo", Label: "Echo", SecretEnv: "ADA_ECHO_TOKEN", Hosts: []string{"localhost", "api.chat.example"}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*Manifest){
		func(m *Manifest) { m.Name = "Echo" }, func(m *Manifest) { m.Name = strings.Repeat("a", 65) }, func(m *Manifest) { m.SecretEnv = "_TOKEN" },
		func(m *Manifest) { m.Label = "bad\n" }, func(m *Manifest) { m.Hosts = nil },
	} {
		m := base
		edit(&m)
		if m.Validate() == nil {
			t.Fatalf("accepted %+v", m)
		}
	}
	for _, host := range []string{"", "*", "-abc", "a-.b", "https://chat", "a/b", "abc:80", "abc..d", strings.Repeat("a", 64)} {
		m := base
		m.Hosts = []string{host}
		if m.Validate() == nil {
			t.Fatal(host)
		}
	}
	path := filepath.Join(t.TempDir(), "channel.json")
	const valid = `{"name":"echo","label":"Echo","secret_env":"ADA_ECHO_TOKEN","hosts":["localhost"]}`
	for _, s := range []string{valid, valid + "{}", strings.Replace(valid, "}", `,"extra":1}`, 1), strings.Replace(valid, `"hosts"`, `"Hosts"`, 1), strings.Replace(valid, `"name":"echo"`, `"name":"bad","name":"echo"`, 1)} {
		if err := os.WriteFile(path, []byte(s), 0600); err != nil {
			t.Fatal(err)
		}
		m, err := LoadManifest(path)
		if (err == nil) != (s == valid) {
			t.Fatal(s, err)
		}
		if err == nil && m.Name != "echo" {
			t.Fatal(m)
		}
	}
}
