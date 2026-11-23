# Matrix

Create a dedicated Matrix account on your homeserver. Sign in with a Matrix client
such as Element and copy the account's access token from its advanced settings.
Keep that session signed in. Logging it out invalidates its token.

Build with `go build -o matrix/ada-channel-matrix ./matrix` (add `.exe` on
Windows). Install the binary and `matrix/channel.json` together. Ada records the
hosts declared in that manifest. For another homeserver, add its hostname to
`hosts` before starting the channel.

| Variable | Value |
|---|---|
| `ADA_MATRIX_TOKEN` | Required bot account access token. |
| `ADA_MATRIX_HOMESERVER` | HTTPS homeserver origin, default `https://matrix.org`. An optional port and trailing slash are allowed. |
| `ADA_MATRIX_STATE_DIR` | Writable, persistent directory for this instance. Defaults to its working directory. |
| `ADA_MATRIX_GROUPS` | Set to `1` to join group invites and forward messages from group rooms. |

The channel checks its account with `whoami`. An invalid token reports
`unauthorised` and stops polling. Replace the token and restart the channel.
Requests carry the token in the Authorization header and go directly to the
configured homeserver. Redirects and environment proxies are disabled.

## Rooms and messages

The channel acts on its own in two cases.

- It joins a direct-room invite from any user so their messages can reach Ada
  for pairing. Turning on `ADA_MATRIX_GROUPS` extends this to group invites.
- On the first encrypted message in an eligible room, it sends a plain notice
  explaining that it cannot read encrypted messages. It remembers the last
  1,024 rooms it notified and reuses the same notice transaction ID per room.

The channel counts joined members to classify the room and refreshes its cache
on membership changes. A private room has exactly two joined members. Ada's
core handles pairing, permissions and replies.

Ada receives text, notices and emotes, including the sender's Matrix ID and
known display name, room and event IDs, timestamp and reply reference.
Images, files, audio and video carry a name, MIME type, size and MXC reference.
Media stays on the homeserver. The bot skips its own events. Other bot accounts
look like ordinary senders.

When room state declares encryption, the channel remembers it across restarts.
Ada deliveries to that room fail with `bad_request` and the message
`Ada cannot write to an encrypted room`. No delivery is sent.

## Replies and approvals

Deliveries use plain text in parts of up to 30,000 bytes. JSON escaping and
reply metadata can require smaller parts to stay within 60,000 encoded bytes.
Retrying a delivery reuses the Matrix transaction ID for each part. A delivery
with buttons ends with numbered choices, for example `Reply 1 to Approve, 2 to Deny`.
Send the number by itself in the same room to select the choice. Only the
latest pending prompt there accepts a choice. A number counts only after the
prompt's final event appears in the room timeline. The channel remembers the
last 64 of its own event IDs seen in each room so a retried prompt can recognise
an earlier echo. Each sender's first valid reply produces an action. Further
numbers from that sender are plain text.
Ada's core decides whose action counts. An action survives a restart until
Ada commits it. The channel remembers the last 32 prompts, each for ten minutes.

## First start and recovery

The first sync handles invitations and saves a starting position without
forwarding room history. Every sync includes the filter as inline JSON. The
channel atomically replaces its saved position once Ada commits every inbound
from that sync. After an interruption it resumes at the last complete batch.
Ada recognises replayed messages by event ID and stores each only once.
Keep the state directory across restarts so the bot retains its position.

When a sync has a gap, the channel pages backwards for up to 1,000 events per
room and forwards eligible messages in chronological order. Older events in
that gap are lost when the cursor advances. The log counts excess events in
the last returned page. If older pages remain, their event count is unknown.

Rate limits, authentication failures and cancellation interrupt an invite join
and the sync. Other HTTP error responses are logged and skipped so other rooms
can advance.
If the homeserver gives no response to a join request, the channel keeps its
cursor and retries the invite on the next sync.
If the homeserver rejects a room history or membership request (for example,
after the bot loses access), the channel skips that room. Its messages from
that sync are lost when the cursor advances past them.
