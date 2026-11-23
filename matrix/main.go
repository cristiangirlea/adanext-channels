package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

var info = sdk.Info{Name: "matrix", Label: "Matrix", Version: "1"}

type settings struct {
	home, token, dir string
	groups           bool
}

func readSettings() (settings, error) {
	cfg := settings{home: os.Getenv("ADA_MATRIX_HOMESERVER"), token: os.Getenv("ADA_MATRIX_TOKEN"), dir: os.Getenv("ADA_MATRIX_STATE_DIR"), groups: os.Getenv("ADA_MATRIX_GROUPS") == "1"}
	if strings.TrimSpace(cfg.token) == "" || strings.ContainsAny(cfg.token, "\r\n") {
		return cfg, errors.New("ADA_MATRIX_TOKEN must contain a bot access token")
	}
	if cfg.dir == "" {
		cfg.dir = "."
	}
	if cfg.home == "" {
		cfg.home = "https://matrix.org"
	}
	return cfg, validateHome(cfg.home)
}

func validateHome(home string) error {
	invalid := errors.New("ADA_MATRIX_HOMESERVER must be an HTTPS URL with only a hostname, optional port and optional trailing slash")
	u, err := url.Parse(home)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || strings.ContainsAny(home, "?#") || u.EscapedPath() != "" && u.EscapedPath() != "/" || strings.HasSuffix(u.Host, ":") {
		return invalid
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return invalid
		}
	}
	if net.ParseIP(u.Hostname()) == nil {
		// Manifest validation applies the same DNS label rules to configured hosts.
		m := sdk.Manifest{Name: "matrix", SecretEnv: "ADA_MATRIX_TOKEN", Hosts: []string{u.Hostname()}}
		if strings.ContainsAny(u.Host, "*[]") || m.Validate() != nil {
			return invalid
		}
	}
	return nil
}

func newService(cfg settings, manifestPath string, stderr io.Writer) (*service, error) {
	if err := validateHome(cfg.home); err != nil {
		return nil, err
	}
	u, err := url.Parse(cfg.home)
	if err != nil {
		return nil, errors.New("invalid ADA_MATRIX_HOMESERVER")
	}
	if manifestPath != "" {
		m, err := sdk.LoadManifest(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("read Matrix channel.json: %w", err)
		}
		if !listedHost(u.Hostname(), m.Hosts) {
			return nil, errors.New("ADA_MATRIX_HOMESERVER host must be listed in channel.json beside the binary")
		}
	}
	cfg.home = "https://" + strings.ToLower(u.Host)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &service{
		settings: cfg,
		client: &http.Client{
			Transport:     transport,
			Timeout:       45 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		log:     &sdk.Logger{Writer: stderr, Redact: []string{cfg.token}},
		backoff: sdk.Backoff{Base: time.Second, Max: 5 * time.Minute, Jitter: 0.2},
		ready:   make(chan struct{}),
		members: make(map[string]map[string]profile),
		now:     time.Now,
	}, nil
}

func listedHost(host string, patterns []string) bool {
	_, suffix, hasLabel := strings.Cut(host, ".")
	for _, pattern := range patterns {
		if strings.EqualFold(host, pattern) {
			return true
		}
		if rest, ok := strings.CutPrefix(pattern, "*."); ok && hasLabel && strings.EqualFold(suffix, rest) {
			return true
		}
	}
	return false
}

func serve(ctx context.Context, in io.Reader, out, stderr io.Writer) int {
	cfg, err := readSettings()
	log := &sdk.Logger{Writer: stderr, Redact: []string{cfg.token}}
	if err != nil {
		log.Printf("matrix: %v", err)
		return p.ExitFailed
	}
	exe, err := os.Executable()
	if err != nil {
		log.Printf("matrix: locate executable: %v", err)
		return p.ExitFailed
	}
	s, err := newService(cfg, filepath.Join(filepath.Dir(exe), "channel.json"), stderr)
	if err != nil {
		log.Printf("matrix: %v", err)
		return p.ExitFailed
	}
	defer s.client.CloseIdleConnections()
	err = sdk.Run(ctx, info, s, in, out, s.log)
	if errors.Is(err, sdk.ErrRefused) {
		return p.ExitRefused
	}
	if err != nil {
		log.Printf("matrix: %v", err)
		return p.ExitFailed
	}
	return p.ExitOK
}

func main() { os.Exit(serve(context.Background(), os.Stdin, os.Stdout, os.Stderr)) }
