package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/cristiangirlea/adanext-channels/sdk"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

const userAgent = "DiscordBot (https://github.com/cristiangirlea/adanext-channels, 0.1)"

type httpFailure struct {
	*sdk.DeliveryError
	status int
}

func (e *httpFailure) Unwrap() error { return e.DeliveryError }

func failure(code, msg string) *sdk.DeliveryError {
	return &sdk.DeliveryError{Code: code, Message: msg}
}

func (s *service) call(ctx context.Context, method, path string, body, result any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return failure(p.CodeInternal, "cannot encode Discord request")
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.apiBase+path, reader)
	if err != nil {
		return failure(p.CodeInternal, "invalid Discord request")
	}
	req.Header.Set("Authorization", "Bot "+s.token)
	req.Header.Set("User-Agent", userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	// Transport errors can include the interaction token in the request URL.
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return failure(p.CodeInternal, "Discord request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		code := p.CodeInternal
		switch resp.StatusCode {
		case 400, 403, 404:
			code = p.CodeBadRequest
		case 401:
			code = p.CodeUnauthorised
		case 429:
			var limited struct {
				After float64 `json:"retry_after"`
			}
			json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&limited)
			if limited.After <= 0 {
				limited.After, _ = strconv.ParseFloat(resp.Header.Get("Retry-After"), 64)
			}
			de := failure(p.CodeRateLimited, "Discord rate limit")
			if limited.After > 0 && limited.After < float64(math.MaxInt64)/float64(time.Second) {
				de.RetryAfter = time.Duration(limited.After * float64(time.Second))
			}
			return de
		}
		return &httpFailure{failure(code, "Discord returned HTTP "+strconv.Itoa(resp.StatusCode)), resp.StatusCode}
	}
	if result != nil {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(result); err != nil {
			return failure(p.CodeInternal, "invalid Discord response")
		}
	}
	return nil
}
