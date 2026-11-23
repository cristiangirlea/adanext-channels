# Discord

Create an application in the [Discord Developer Portal](https://discord.com/developers/applications).
On its **Bot** page, generate a bot token and set `ADA_DISCORD_TOKEN` in the
channel process's environment. Keep the token out of chat and command lines.
For server channels, enable **Message Content Intent** on the same page.

Use the application's installation link with the `bot` scope to invite it to
your server. Grant **View Channel**, **Read Message History**, and **Send Messages**
in the channels Ada will use. To reply in threads, also grant **Send Messages
in Threads**. Members who share a server with the bot can open a DM with it.

Build with `go build -o discord/ada-channel-discord ./discord` (add `.exe` on
Windows). Install the binary alongside `discord/channel.json`. It opens an
outbound gateway connection and uses Discord's REST API.

## Choose conversations

DMs are enabled by default. To include one server, set `ADA_DISCORD_GUILD`
to its server ID and `ADA_DISCORD_CHANNELS` to comma-separated channel IDs.
Enable Developer Mode in Discord to copy IDs. Threads have their own IDs.
An empty channel list leaves server messages disabled. Invalid server or channel IDs,
including empty entries within a list, stop the channel at startup.

Ada receives human messages and button presses from these conversations.
Each message includes its Discord ID, timestamp, sender, chat, text, reply
reference and attachment metadata. File contents stay on Discord.
Ada's core handles pairing and decides what to send.

## Recovery

Set `ADA_DISCORD_STATE_DIR` to a persistent writable directory for this bot.
It defaults to the working directory. At startup, each configured server channel
without a saved position starts at its newest message. An empty channel starts
at zero. A DM gets its starting position when Ada first receives a message in
it. Ada saves these positions and each acknowledged message ID atomically in
`discord-state.json`.

Gateway Resume normally covers short gaps by replaying events after the last
processed sequence. If a Resume is not confirmed twice in a row, the channel
identifies again through the default gateway and relies on history catch-up.
After a restart or a fresh session, Ada fetches available
message history after each saved position in pages of 100, oldest first.
Overlapping messages keep their Discord IDs and are deduplicated.

A first DM sent during a gap can be lost if Ada has never seen a message from
that user. Deleted messages are absent from history. Button presses during a
gap can be lost if Resume cannot replay them. A press received before a crash
can also be lost if Ada's core has yet to acknowledge it.
If Discord revokes access to a channel, Ada retains its cursor for a later
catch-up and continues with accessible chats.

An exhausted session start quota delays the next Identify until the reset.
Authentication failure stops the gateway with status `unauthorised`. Invalid
sharding, API version or intents stop it with status `stopped`. Check the log
for the cause and restart after correcting it. REST deliveries remain available.

## Delivery

Text is split at 2,000 UTF-8 bytes and whitespace-only parts are skipped.
Mentions are disabled on every part. Replies can still be sent after the
referenced message is deleted. Each part carries a stable nonce so Discord
can recognise a recent retry. Buttons appear on the final part, in up to
five rows of five. The delivery ID, separator and button ID together must fit
in 100 characters. Discord receives an immediate deferred-update callback for
button presses, including presses that the channel skips. If these
acknowledgements back up, Discord may show a press as failed while Ada still
receives it.
