package fakecore_test

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/cristiangirlea/adanext-channels/sdk/fakecore"
	p "github.com/cristiangirlea/harness/contracts/pluginproto"
)

func TestDeliverQueuesInbound(t *testing.T) {
	run := func(ctx context.Context, r io.Reader, w io.Writer) error {
		dec := json.NewDecoder(r)
		send := json.NewEncoder(w).Encode
		var h p.Hello
		if err := dec.Decode(&h); err != nil {
			return err
		}
		// Uppercase extensions must not replace a case-sensitive envelope field.
		if err := send(json.RawMessage(`{"v":1,"seq":1,"SEQ":99,"type":"ready","kind":"channel","protocol":"v2","plugin":{"name":"test","version":"1"},"channel":{"name":"test"}}`)); err != nil {
			return err
		}
		var d p.Deliver
		if err := dec.Decode(&d); err != nil {
			return err
		}
		if err := send(p.Inbound{V: 1, Seq: 2, Type: p.TypeInbound, ID: "a", Text: "hello", Time: "2026-01-01T00:00:00Z", Sender: p.Sender{ID: "u"}, Chat: p.Chat{ID: "c"}}); err != nil {
			return err
		}
		return send(p.Delivered{V: 1, Seq: 3, Type: p.TypeDelivered, ID: d.ID, MessageID: "m"})
	}
	core := fakecore.Start(t, run)
	core.Handshake()
	sent, _ := core.Deliver(p.Deliver{ID: "d", ChatID: "c", Text: "reply"})
	if sent.MessageID != "m" || core.NextInbound().ID != "a" || len(core.Stored()) != 1 {
		t.Fatal(sent)
	}
}
