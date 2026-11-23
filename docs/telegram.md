# Telegram

Open a private chat with Telegram's @BotFather, run `/newbot`, and choose a
display name and bot username. Set `ADA_TELEGRAM_TOKEN` to the issued token
in the channel process's environment. Keep the token out of command lines and logs.

Build with `go build -o telegram/ada-channel-telegram ./telegram` (add
`.exe` on Windows). Install the binary alongside `telegram/channel.json`.
Start a chat with the bot and send a message. Ada's core handles pairing.

## What the channel reads

The bot long-polls for messages and button presses. By default it forwards
private chats only. Set `ADA_TELEGRAM_GROUPS=1` to also forward messages and
button presses from groups and supergroups that Telegram exposes to the bot.
Telegram's bot privacy settings affect which group messages it receives.
The channel skips bot senders and channel posts.

A webhook set on the bot makes `getUpdates` return 409. Remove it with
`deleteWebhook` before polling. On 409 the channel reports `reconnecting`,
logs the conflict and retries with backoff. A 401 or 404 from `getMe` or
`getUpdates` reports `unauthorised` and also backs off.

Ada receives the update ID, sender ID and display name, chat ID and type,
message time, text or caption, and reply reference when present. For photos
(largest size), documents, audio, video and voice it reports a file reference,
name, MIME type and byte size. File contents stay on Telegram.

## What the channel sends

The channel uses the token only with `api.telegram.org`. It checks the bot's
identity, polls with the confirmed update offset and acknowledges button
presses, including presses it skips. On delivery it sends the destination chat
ID and plain text in parts of up to 4096 bytes. When a part contains only
whitespace, it skips that part and any following whitespace, then splits the
remaining text again. Empty or all-whitespace text fails with `bad_request`.
A reply still goes through if its target message was deleted. A 403, such as
a user blocking the bot, fails the delivery with `bad_request`.
Within a session, the channel remembers up to 4096 incomplete multipart sends
so an identical retry resumes after the last confirmed part. A restart or a
lost Telegram response can cause a part to be sent twice.

Buttons appear on the last part, one per row. Telegram receives each button's
label and callback data. The delivery ID and button ID together can occupy
up to 63 UTF-8 bytes, plus one separator byte. Oversized IDs fail the delivery
before it sends any text, as do more than 100 buttons or a blank button label.
