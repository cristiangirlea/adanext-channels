package main

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cristiangirlea/adanext-channels/internal/ws"
	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

var info = sdk.Info{Name: "discord", Label: "Discord", Version: "1"}

type config struct{ apiBase, token, guild, channels, stateDir string }

type service struct {
	config
	client              *http.Client
	log                 *sdk.Logger
	allowed             map[string]bool
	backoff             sdk.Backoff
	now                 func() time.Time
	wait                func(context.Context, time.Duration) error
	dialGateway         func(context.Context, string, http.Header) (*ws.Conn, error)
	invalidSessionDelay func() time.Duration
	lastIdentify        time.Time
	// Receive and Commit are serialised by the SDK; the gateway also snapshots durable positions.
	stateMu sync.Mutex
	state   map[string]cursor
	pending map[string]string
	batches chan []sdk.Inbound
	ended   chan struct{}
	cancel  context.CancelFunc
	runErr  error
}

func newService(cfg config, stderr io.Writer) (*service, error) {
	if cfg.token == "" {
		return nil, errors.New("ADA_DISCORD_TOKEN is required")
	}
	if cfg.stateDir == "" {
		cfg.stateDir = "."
	}
	state, err := loadState(cfg.stateDir)
	if err != nil {
		return nil, err
	}
	allowed, err := parseConversationIDs(cfg.guild, cfg.channels)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &service{
		config:              cfg,
		dialGateway:         ws.Dial,
		now:                 time.Now,
		wait:                func(ctx context.Context, d time.Duration) error { return (sdk.Backoff{}).Wait(ctx, 0, d) },
		invalidSessionDelay: func() time.Duration { return time.Second + time.Duration(rand.Int64N(int64(4*time.Second)+1)) },
		allowed:             allowed,
		state:               state,
		pending:             make(map[string]string),
		batches:             make(chan []sdk.Inbound),
		ended:               make(chan struct{}),
		client: &http.Client{
			Transport:     transport,
			Timeout:       15 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		log:     &sdk.Logger{Writer: stderr, Redact: []string{cfg.token}},
		backoff: sdk.Backoff{Base: time.Second, Max: time.Minute, Jitter: 0.2},
	}, nil
}

func parseConversationIDs(guild, channels string) (map[string]bool, error) {
	if guild != "" && snowflake(guild) == 0 {
		return nil, errors.New("ADA_DISCORD_GUILD must be a valid Discord server ID")
	}
	allowed := make(map[string]bool)
	if channels != "" {
		for entry := range strings.SplitSeq(channels, ",") {
			id := strings.TrimSpace(entry)
			if snowflake(id) == 0 {
				return nil, errors.New("ADA_DISCORD_CHANNELS must contain comma-separated Discord channel IDs (no empty entries)")
			}
			allowed[id] = true
		}
	}
	return allowed, nil
}

func (s *service) stop() {
	if s.cancel != nil {
		s.cancel()
		<-s.ended
	}
	s.client.CloseIdleConnections()
}

func (s *service) redact(v string) string { return strings.ReplaceAll(v, s.token, "[redacted]") }

func serve(ctx context.Context, in io.Reader, out, stderr io.Writer) int {
	cfg := config{
		apiBase:  "https://discord.com/api/v10",
		token:    os.Getenv("ADA_DISCORD_TOKEN"),
		guild:    os.Getenv("ADA_DISCORD_GUILD"),
		channels: os.Getenv("ADA_DISCORD_CHANNELS"),
		stateDir: os.Getenv("ADA_DISCORD_STATE_DIR"),
	}
	s, err := newService(cfg, stderr)
	if err != nil {
		log := sdk.Logger{Writer: stderr, Redact: []string{cfg.token}}
		log.Printf("discord: %v", err)
		return p.ExitFailed
	}
	defer s.stop()
	err = sdk.Run(ctx, info, s, in, out, s.log)
	if errors.Is(err, sdk.ErrRefused) {
		return p.ExitRefused
	}
	if err != nil {
		s.log.Printf("discord: %v", err)
		return p.ExitFailed
	}
	return p.ExitOK
}

func main() { os.Exit(serve(context.Background(), os.Stdin, os.Stdout, os.Stderr)) }
