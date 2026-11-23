# Slack

Create a Slack app for the workspace and enable **Socket Mode**. Under **Basic
Information**, generate an app-level token with `connections:write`.
Under **OAuth & Permissions**, add the bot scopes `im:history`, `im:read`,
`chat:write` and `users:read`. For public channels also add `app_mentions:read`
and `channels:history`. Private channels need `groups:history` for recovery.

Enable **Event Subscriptions** and subscribe to the bot event `message.im`.
For channel mentions, also subscribe to `app_mention` and invite the bot to
each channel. Enable **Interactivity** under **Interactivity & Shortcuts**.
Enable the App Home **Messages Tab** and allow users to send messages there.
Install the app in the workspace, or reinstall it after changing scopes.
Socket Mode uses an outbound connection. See [Slack’s setup guide](https://docs.slack.dev/apis/events-api/using-socket-mode/).

Build with `go build -o slack/ada-channel-slack ./slack` (add `.exe` on Windows).
Install the binary beside `slack/channel.json`. Set these process variables.

| Variable | Value |
|---|---|
| `ADA_SLACK_TOKENS` | App-level token followed by the bot token, as `xapp-…,xoxb-…`. Keep both out of chat and command lines. |
| `ADA_SLACK_CHANNELS` | Optional comma-separated channel IDs. An empty list enables DMs only. |
| `ADA_SLACK_STATE_DIR` | Persistent writable directory for this app and workspace. Defaults to the working directory. |

Ada receives human DMs, app mentions in listed channels, and button presses
in those conversations. A message includes the sender, text, thread reference
and file metadata. Private file URLs are passed through without downloading
the files. Ada's core handles pairing and decides which replies to send.

## Recovery

Ada acknowledges eligible Slack envelopes after the core stores the inbound.
It saves the last committed message timestamp per conversation in
`slack-state.json`. After startup or reconnect, it pages through available
history after each saved timestamp and delivers it oldest first, before live
events. Message IDs include the conversation and timestamp across replays.
If a conversation's history is unavailable, catch-up skips it and continues.
Rate limits, server errors and connection failures get up to three retries
per history request, honoring Slack's retry delay up to one minute. One
catch-up pass stops after two minutes so live events are not held up. A
conversation whose catch-up did not finish keeps its saved timestamp, even
when newer live messages are committed, until a later pass reads the gap;
a permanent error, such as a missing scope, lets the timestamp move on.
Conversations a pass did not reach go first in the next one. A conversation
that uses up the whole pass three times in a row gives up its unread gap
until the channel restarts, and live messages move its timestamp again.
If Slack rejects socket authentication, reconnection stops until the channel
is restarted.

A conversation without a committed message has no history cursor. First
messages, deleted or expired messages, and thread replies absent from channel
history can be lost if Slack exhausts its retries. Removing a channel from
the list also excludes its history.

Ada acknowledges a button press only after the core stores it. If that takes
longer than three seconds, Slack shows the user an error. Slack does not
resend the press, and history cannot replay it. Press the button again to retry.

## Delivery

Ada sends escaped text in parts of up to 3,000 UTF-8 bytes, with link and
media unfurls disabled. For a reply to a message inside a thread, the channel
uses the parent timestamp from its last 4,096 distinct thread replies. If the
reference is unavailable, it uses the referenced message's own timestamp.
The last part can carry up to 25 buttons in groups of five. Its section shows
the original text as plain text, and its fallback stays escaped. Each button value
holds the delivery ID and button ID separated by U+001F, within 2,000 bytes.
Retrying a partially sent delivery can repeat parts that Slack already posted.
