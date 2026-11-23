package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cristiangirlea/adanext-channels/internal/ws"
	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

var info = sdk.Info{Name: "slack", Label: "Slack", Version: "1"}
var appToken = regexp.MustCompile(`^xapp-[!-~]+$`)
var botToken = regexp.MustCompile(`^xoxb-[!-~]+$`)
var chatID = regexp.MustCompile(`^[CDG][A-Z0-9]+$`)

type config struct{ apiBase, tokens, channels, stateDir string }
type service struct {
	apiBase, app, bot, stateDir string
	client                      *http.Client
	log                         *sdk.Logger
	redact                      *strings.Replacer
	allowed                     map[string]bool
	dial                        func(context.Context, string, http.Header) (*ws.Conn, error)
	backoff                     sdk.Backoff
	events                      chan incoming
	cancel                      context.CancelFunc
	done                        chan struct{}
	// Socket reads, history recovery and outbound deliveries share thread references.
	threadMu    sync.Mutex
	threads     map[string]string
	threadOrder []string
	// Receive and Commit share a single SDK worker.
	positions map[string]string
	// Conversations whose catch-up did not finish keep their cursor until it does.
	incomplete map[string]bool
	// Passes in which a conversation itself ran out; after maxOverruns its gap is given up.
	overruns  map[string]int
	abandoned map[string]bool
	// Conversations a pass did not reach; the next pass visits them first.
	unreached map[string]bool
	names     map[string]string
	self      string
	pending   map[string]incoming
	recent    map[string]bool
	order     []string
}

func newService(cfg config, stderr io.Writer) (*service, error) {
	app, bot, ok := strings.Cut(cfg.tokens, ",")
	if !ok || strings.Contains(bot, ",") || !appToken.MatchString(app) || !botToken.MatchString(bot) {
		return nil, errors.New("ADA_SLACK_TOKENS requires an xapp- app token and an xoxb- bot token separated by one comma")
	}
	allowed := make(map[string]bool)
	if cfg.channels != "" {
		for entry := range strings.SplitSeq(cfg.channels, ",") {
			id := strings.TrimSpace(entry)
			if !chatID.MatchString(id) || id[0] == 'D' {
				return nil, errors.New("ADA_SLACK_CHANNELS requires comma-separated Slack channel IDs")
			}
			allowed[id] = true
		}
	}
	if cfg.stateDir == "" {
		cfg.stateDir = "."
	}
	positions, err := loadState(cfg.stateDir)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &service{
		apiBase:   cfg.apiBase,
		app:       app,
		bot:       bot,
		stateDir:  cfg.stateDir,
		allowed:   allowed,
		positions: positions,
		names:     make(map[string]string),
		threads:   make(map[string]string),
		pending:   make(map[string]incoming),
		recent:    make(map[string]bool),
		client: &http.Client{
			Transport:     transport,
			Timeout:       15 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		log:     &sdk.Logger{Writer: stderr, Redact: []string{app, bot}},
		redact:  strings.NewReplacer(app, "[redacted]", bot, "[redacted]"),
		dial:    ws.Dial,
		backoff: sdk.Backoff{Base: time.Second, Max: time.Minute, Jitter: 0.2},
		events:  make(chan incoming, 64),
		done:    make(chan struct{}),
	}, nil
}

func (s *service) stop() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
	s.client.CloseIdleConnections()
}

func serve(ctx context.Context, in io.Reader, out, stderr io.Writer) int {
	cfg := config{
		apiBase:  "https://slack.com/api",
		tokens:   os.Getenv("ADA_SLACK_TOKENS"),
		channels: os.Getenv("ADA_SLACK_CHANNELS"),
		stateDir: os.Getenv("ADA_SLACK_STATE_DIR"),
	}
	s, err := newService(cfg, stderr)
	if err != nil {
		log := sdk.Logger{Writer: stderr, Redact: strings.Split(cfg.tokens, ",")}
		log.Printf("slack: %v", err)
		return p.ExitFailed
	}
	defer s.stop()
	err = sdk.Run(ctx, info, s, in, out, s.log)
	if errors.Is(err, sdk.ErrRefused) {
		return p.ExitRefused
	}
	if err != nil {
		s.log.Printf("slack: %v", err)
		return p.ExitFailed
	}
	return p.ExitOK
}
func main() { os.Exit(serve(context.Background(), os.Stdin, os.Stdout, os.Stderr)) }
