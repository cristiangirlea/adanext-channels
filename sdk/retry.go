package sdk

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

type Backoff struct {
	Base, Max time.Duration
	// Jitter is the fractional spread around each delay, from zero to one.
	Jitter float64
	Sleep  func(context.Context, time.Duration) error
}

func (b Backoff) Next(attempt int) time.Duration {
	if b.Max <= 0 {
		b.Max = math.MaxInt64
	}
	d := min(b.Base, b.Max)
	if d <= 0 {
		return 0
	}
	for attempt > 0 && d < b.Max {
		d += min(d, b.Max-d)
		attempt--
	}
	spread := min(b.Jitter, 1)
	if spread > 0 {
		value := float64(d) * (1 + (2*rand.Float64()-1)*spread)
		if value >= float64(b.Max) {
			return b.Max
		}
		d = time.Duration(value)
	}
	return d
}

func (b Backoff) Wait(ctx context.Context, attempt int, retryAfter time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d := max(b.Next(attempt), retryAfter)
	if b.Sleep != nil {
		return b.Sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
