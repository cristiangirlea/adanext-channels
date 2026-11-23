package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

type apiResponse struct {
	retryable      bool
	OK             bool
	Error, URL, TS string
	UserID         string `json:"user_id"`
	User           json.RawMessage
	Messages       []message
	HasMore        bool `json:"has_more"`
	Metadata       struct {
		Next string `json:"next_cursor"`
	} `json:"response_metadata"`
}

func failure(code, msg string) *sdk.DeliveryError {
	return &sdk.DeliveryError{Code: code, Message: msg}
}

func apiFailure(status int, code, retry string) *sdk.DeliveryError {
	switch {
	case status == 429 || code == "ratelimited" || code == "rate_limited":
		de := failure(p.CodeRateLimited, "Slack rate limit")
		if seconds, err := strconv.ParseInt(retry, 10, 64); err == nil && seconds > 0 && seconds <= math.MaxInt64/int64(time.Second) {
			de.RetryAfter = time.Duration(seconds) * time.Second
		}
		return de
	case code == "invalid_auth" || code == "not_authed" || code == "account_inactive" || code == "token_revoked":
		return failure(p.CodeUnauthorised, "Slack authentication failed")
	case code == "channel_not_found" || code == "not_in_channel" || code == "is_archived" || code == "msg_too_long" || code == "no_text":
		return failure(p.CodeBadRequest, "Slack: "+code)
	default:
		// Slack and intermediaries can echo credentials in arbitrary error strings.
		return failure(p.CodeInternal, "Slack API request failed (HTTP "+strconv.Itoa(status)+")")
	}
}

func (s *service) call(ctx context.Context, method, endpoint string, query url.Values, body any) (apiResponse, error) {
	var result apiResponse
	addr := s.apiBase + "/" + endpoint
	if len(query) > 0 {
		addr += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return result, failure(p.CodeInternal, "cannot encode Slack request")
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, addr, reader)
	if err != nil {
		return result, failure(p.CodeInternal, "invalid Slack request")
	}
	token := s.bot
	if endpoint == "apps.connections.open" {
		token = s.app
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		result.retryable = true
		return result, failure(p.CodeInternal, "Slack API connection failed")
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	valid := readErr == nil && json.Unmarshal(data, &result) == nil
	if !valid || !result.OK || resp.StatusCode/100 != 2 {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		de := apiFailure(resp.StatusCode, result.Error, resp.Header.Get("Retry-After"))
		result.retryable = readErr != nil || resp.StatusCode/100 == 5 || de.Code == p.CodeRateLimited
		return result, de
	}
	return result, nil
}
