package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type service struct {
	baseURL, token string
	groups         bool
	client         *http.Client
	log            *sdk.Logger
	backoff        sdk.Backoff
	partial        multipartProgress

	// The SDK serialises Receive and Commit, including after out-of-order acks.
	offset         int64
	lastInbound    string
	trailingOffset int64
	verified       bool
	state          string
}

func newService(baseURL, token string, groups bool, stderr io.Writer) *service {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &service{
		baseURL: baseURL, token: token, groups: groups,
		client: &http.Client{
			Timeout:   45 * time.Second,
			Transport: transport,
			// A redirect could forward chat content to a host outside the manifest.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		log:     &sdk.Logger{Writer: stderr, Redact: []string{token}},
		backoff: sdk.Backoff{Base: time.Second, Max: 5 * time.Minute, Jitter: 0.2},
	}
}

func (s *service) redact(text string) string {
	if s.token == "" {
		return text
	}
	return strings.ReplaceAll(text, s.token, "[redacted]")
}

type response struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func (s *service) call(ctx context.Context, method string, args, result any) *sdk.DeliveryError {
	failure := func(code, detail string) *sdk.DeliveryError {
		message := method + ": " + s.redact(detail)
		s.log.Printf("%s", message)
		return &sdk.DeliveryError{Code: code, Message: message}
	}
	body, err := json.Marshal(args)
	if err != nil {
		return failure(p.CodeInternal, err.Error())
	}
	endpoint := s.baseURL + "/bot" + url.PathEscape(s.token) + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return failure(p.CodeInternal, "invalid request URL")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		// net/http includes the credential-bearing URL around transport errors.
		var outer *url.Error
		if errors.As(err, &outer) {
			err = outer.Err
		}
		return failure(p.CodeInternal, err.Error())
	}
	defer resp.Body.Close()
	var reply response
	err = json.NewDecoder(resp.Body).Decode(&reply)
	if resp.StatusCode != http.StatusOK || !reply.OK {
		code := p.CodeInternal
		switch {
		case resp.StatusCode == 429 || reply.ErrorCode == 429:
			code = p.CodeRateLimited
		case resp.StatusCode == 401 || reply.ErrorCode == 401:
			code = p.CodeUnauthorised
		case (method == "getMe" || method == "getUpdates") && (resp.StatusCode == 404 || reply.ErrorCode == 404):
			code = p.CodeUnauthorised
		case resp.StatusCode == 400 || reply.ErrorCode == 400 || resp.StatusCode == 403 || reply.ErrorCode == 403:
			code = p.CodeBadRequest
		}
		detail := reply.Description
		if detail == "" {
			detail = resp.Status
			if err != nil {
				detail += ": " + err.Error()
			}
		}
		if method == "getUpdates" && (resp.StatusCode == 409 || reply.ErrorCode == 409) {
			detail += " (409 conflict: if a webhook is set, remove it with deleteWebhook before polling)"
		}
		de := failure(code, detail)
		if code == p.CodeRateLimited {
			de.RetryAfter = time.Duration(reply.Parameters.RetryAfter) * time.Second
		}
		return de
	}
	if err != nil {
		return failure(p.CodeInternal, err.Error())
	}
	if result != nil {
		if err := json.Unmarshal(reply.Result, result); err != nil {
			return failure(p.CodeInternal, err.Error())
		}
	}
	return nil
}
