# Writing a channel

Put the binary's Go package, its `channel.json` and its tests in a directory named for the
service. Ada starts the binary, pairs senders and decides which replies to deliver. The
channel only reports incoming messages and sends deliveries requested by Ada.

## Implement the service

```go
type Service interface {
    Receive(ctx context.Context) ([]sdk.Inbound, error)
    Commit(ctx context.Context, id string) error
    Deliver(ctx context.Context, d pluginproto.Deliver) (messageID string, err error)
}
```

`Receive` blocks until there are messages after the saved position or its context ends.
Return a batch in service order with stable message, sender and chat IDs and RFC 3339
timestamps. `sdk.Inbound` is the contracts type. The runtime fills its wire envelope.
`Commit` saves the position past one ID durably. The runtime finishes committing each batch
before requesting another. `Deliver` may run concurrently with either method and with other
deliveries. All three methods must respect cancellation.

`sdk.Normalize(in)` returns the repaired inbound and a description of each fix. Run applies
it to each batch and logs each fix with the message ID. It replaces control characters in
short strings with U+FFFD and shortens oversized fields on a rune boundary. Oversized IDs
use the hexadecimal SHA-256 of the original ID, limited to the field size. Ada acknowledges
the wire ID and the service receives its original ID in `Commit`.

Run substitutes the batch receive time for missing or invalid timestamps. Empty text
without an action becomes `[message with no text]`. Run sends the first 16 attachments
and adds `[n more attachments not shown]` to the text for the rest. It reserves room
for this note and marks truncated text with `[message cut]`. A repeated in-flight ID,
including a second copy in one batch, is logged and sent only once. When repairs give two
different service IDs the same wire ID, the message is sent once and, after its ack,
`Commit` is called for each of the service's IDs in turn. Invalid UTF-8 and control
characters are replaced with U+FFFD; message text keeps line breaks and tabs. If an inbound still
fails protocol validation, Run logs the failure and treats the message as sent and
acknowledged locally. Its position advances after all earlier messages are acknowledged.

Pass `sdk.Info{Name: ..., Label: ..., Version: ...}`, the service, `os.Stdin`, `os.Stdout` and
a logger to `sdk.Run`. A channel name starting with a digit or hyphen retains its ID and
uses `channel` as the protocol's program name. Run negotiates v2, validates protocol lines,
serialises output and assigns sequence numbers. It rejects calls and malformed requests
and handles acknowledgements while a service operation is blocked. On stdin closure,
accepted deliveries and acknowledged commits have up to five seconds to finish. Set
`Info.DrainGrace` to override this interval. A stdout write still blocked for 250 ms
after EOF can close the output pipe. Canceling the parent context stops work immediately.
Use closable pipes for blocking I/O. Map `sdk.ErrRefused` to `pluginproto.ExitRefused` in main.

## What survives a restart

Ada stores each inbound under the pair of channel name and service message ID before
acknowledging it. Acks can arrive in any order. If Ada acknowledges the third message and
then the first, the channel can commit only the first. When the second ack arrives it can
commit the second and third in order. A failed commit ends the session.

After a crash, read from the saved position and replay the remaining messages using their
original IDs. Ada acknowledges replays and keeps the first stored copy. This covers a
crash before storage and a crash between storage and acknowledgement.

Each outbound delivery has a host-assigned ID. In-flight repeats share one service call
and each receives its outcome. The SDK remembers the last 4096 completed IDs with a
`delivered` outcome or a `bad_request` for an invalid destination, evicting the oldest
when full. It forgets `rate_limited`, `unauthorised` and `internal` outcomes after answering
the waiters, so Ada can retry the same ID. A malformed delivery with an unambiguous valid
ID receives `delivery_failed` with `bad_request`. A corrected payload can reuse that ID
when the destination was valid.

A crash after a service send can leave Ada unsure whether it arrived. Pass the delivery
ID as an idempotency key when the service supports one.

## Formatting, retries and diagnostics

- `Split(text, limit)` limits each part by bytes, preserves runes and keeps whitespace.
  It prefers a newline, then a space. Joining the parts recovers the original text.
  Empty text yields one empty part. A limit too small for a rune yields nil.
- `EscapeMarkdownV2` escapes literal text for Telegram. Use `PlainText` with markup disabled
  when a service accepts plain text.
- Use `Backoff{Base: ..., Max: ..., Jitter: ...}` and `Wait(ctx, attempt, retryAfter)` for
  reconnects and rate limits. Attempts start at zero. Jitter is a fraction from zero to
  one. A positive Max caps the computed backoff. Wait always honours a longer service
  retry-after. Set `Sleep` in deterministic tests.
- Return a `*sdk.DeliveryError` with a contracts code, a short message and an optional
  `RetryAfter` duration. Codes are `rate_limited`, `unauthorised`, `bad_request` and `internal`.
  Use `bad_request` for an invalid delivery destination.
  Use `sdk.Status(ctx, state, text)` with the service context to report a connection change.
- Configure `sdk.Logger{Redact: []string{token}}` before using the service client. Its
  `Printf` writes to stderr, replaces listed secrets with `[redacted]` and escapes controls,
  Unicode format characters, line separators and paragraph separators.

## Manifest

Install `channel.json` beside the executable. The echo channel ships this manifest.

```json
{"name":"echo","label":"Echo","secret_env":"ADA_ECHO_TOKEN","hosts":["localhost"]}
```

`sdk.LoadManifest(path)` validates the file. Names use 1 to 64 lowercase ASCII letters,
digits or hyphens. The token environment variable starts with an uppercase letter and
contains only uppercase letters, digits and underscores. Declare at least one host.
Use an exact hostname or a single leading wildcard label. For example, `*.discord.gg`
matches `gateway-us-east1-b.discord.gg`. The wildcard covers exactly one label and the
suffix must be a valid hostname. Unknown and duplicate fields are refused. Ada gives each
channel only the secret variable its manifest names. Ada records and displays the declared
hosts. [Ada's isolation work](https://github.com/cristiangirlea/adanext-ada/issues/8) will
enforce those host limits.

## Test with the fake core

`fakecore.Start(t, runner)` takes an in-process function with context, input and output.
Create a fresh service on each invocation. For a subprocess, wrap a command factory in
`fakecore.Command` and use `exec.CommandContext` with the supplied context.
Call `Handshake()`, read messages with `NextInbound()` and send explicit `Ack(id)` calls.
`Deliver(d)` returns either a delivered frame or a failure. `Restart()` interrupts the
channel and starts another. Handshake again and inspect `Stored()` for duplicates and order.

Run `go test ./echo` for the complete helper-process example. Build the reference with
`go build -o echo/ada-channel-echo ./echo` (add `.exe` on Windows). Pass `-dir DIR` to use
an existing service directory. Append one inbound JSON object plus a newline per record to
`messages.jsonl`. The channel atomically replaces `position` with the committed byte offset
and appends outbound deliveries to `deliveries.jsonl`. It runs offline without a token.
