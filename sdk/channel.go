package sdk

import (
	"context"
	"errors"
	"time"

	"github.com/cristiangirlea/harness/contracts/pluginproto"
)

// Run fills in the envelope so services only supply message content.
type Inbound = pluginproto.Inbound

type Info struct {
	Name, Label, Version string
	// DrainGrace bounds accepted deliveries and acknowledged commits after stdin closes.
	// A non-positive value selects five seconds.
	DrainGrace time.Duration
}

type Service interface {
	// Receive blocks until messages follow the committed position or ctx ends.
	Receive(ctx context.Context) ([]Inbound, error)
	// Commit advances past one acknowledged message in receive order.
	Commit(ctx context.Context, id string) error
	Deliver(ctx context.Context, d pluginproto.Deliver) (messageID string, err error)
}

// DeliveryError carries the service's reason and optional delay back to the host.
type DeliveryError struct {
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *DeliveryError) Error() string { return e.Message }

type statusKey struct{}
type reportStatus func(context.Context, string, string) error

// Status reports a connection change using the context passed to the service.
func Status(ctx context.Context, state, text string) error {
	report, ok := ctx.Value(statusKey{}).(reportStatus)
	if !ok {
		return errors.New("status: no channel session")
	}
	return report(ctx, state, text)
}
