package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type service struct {
	settings
	client                    *http.Client
	log                       *sdk.Logger
	backoff                   sdk.Backoff
	now                       func() time.Time
	ready                     chan struct{}
	userID, statePath, status string
	members                   map[string]map[string]profile

	// Only Receive and Commit access an uncommitted sync.
	next     string
	pending  map[string]bool
	batchIDs []string
	used     map[string]map[string]bool
	actions  map[string]p.Action

	// Deliver can publish a prompt while Receive is polling or committing.
	mu        sync.Mutex
	state     checkpoint
	ownEvents map[string][]string // Updated room histories for the uncommitted sync.
	armed     map[string]bool
}

var errNoResponse = &sdk.DeliveryError{Code: p.CodeInternal, Message: "Matrix HTTPS request failed"}

func (s *service) redact(text string) string { return strings.ReplaceAll(text, s.token, "[redacted]") }

func (s *service) skipRoomError(operation string, err error) bool {
	var failure *sdk.DeliveryError
	if !errors.As(err, &failure) || failure.Code != p.CodeBadRequest {
		return false
	}
	s.log.Printf("matrix: skipping %s: %v", operation, err)
	return true
}

func (s *service) call(ctx context.Context, method, path string, body io.Reader, output any) error {
	req, err := http.NewRequestWithContext(ctx, method, s.home+"/_matrix/client/v3/"+path, body)
	if err != nil {
		return errors.New("invalid Matrix request URL")
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Transport errors can quote request URLs and remote-controlled response bytes.
		return fmt.Errorf("%s: %w", method, errNoResponse)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var problem struct {
			Code  string `json:"errcode"`
			Delay int64  `json:"retry_after_ms"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&problem)
		failure := &sdk.DeliveryError{Code: p.CodeInternal, Message: fmt.Sprintf("Matrix returned HTTP %d", resp.StatusCode)}
		switch {
		case resp.StatusCode == 429 || problem.Code == "M_LIMIT_EXCEEDED":
			failure.Code = p.CodeRateLimited
			failure.RetryAfter = time.Duration(min(max(problem.Delay, 0), int64(math.MaxInt64/time.Millisecond))) * time.Millisecond
			header := resp.Header.Get("Retry-After")
			if seconds, err := strconv.ParseUint(header, 10, 63); err == nil {
				failure.RetryAfter = time.Duration(min(seconds, uint64(math.MaxInt64/time.Second))) * time.Second
			} else if date, err := http.ParseTime(header); err == nil {
				failure.RetryAfter = max(0, date.Sub(s.now()))
			}
		case problem.Code == "M_MISSING_TOKEN" || problem.Code == "M_UNKNOWN_TOKEN" || problem.Code == "M_USER_DEACTIVATED" || problem.Code == "M_USER_LOCKED":
			failure.Code = p.CodeUnauthorised
		case problem.Code == "M_FORBIDDEN" || problem.Code == "M_NOT_FOUND" || resp.StatusCode == 400 || resp.StatusCode == 404 || resp.StatusCode == 413 || problem.Code == "M_TOO_LARGE":
			failure.Code = p.CodeBadRequest
		}
		return failure
	}
	if output != nil {
		if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
			return &sdk.DeliveryError{Code: p.CodeInternal, Message: "invalid Matrix response"}
		}
	}
	return nil
}
