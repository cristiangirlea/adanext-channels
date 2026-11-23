# adanext-channels

The Telegram, Discord and Slack channels, shared SDK and directory-backed echo channel are available. Issue #1 tracks the chat service integrations.

The chat apps [Ada](https://github.com/cristiangirlea/adanext-ada) talks through, each as a
separate plugin program: installed on its own, holding only its own token, reaching only its own
service, and deciding nothing. Who may talk to Ada, and what she may do, is decided in Ada's core.

Next are Matrix, Signal and email. WhatsApp and Teams
wait for a decision on their costs and sign-ups (issue #7).

| Path | Purpose |
|---|---|
| `sdk/` | Shared runtime and helpers. The SDK uses only the standard library and harness's contracts module. |
| `sdk/fakecore/` | Test host with a store that survives channel restarts. |
| `echo/` | Reference binary, `channel.json` and end-to-end tests. |
| `telegram/` | Telegram Bot API channel. See [Telegram setup and privacy](docs/telegram.md). |
| `discord/` | Discord DMs and selected channels in one server. See [Discord setup and recovery](docs/discord.md). |
| `slack/` | Slack DMs, selected app mentions and buttons over Socket Mode. See [Slack setup and recovery](docs/slack.md). |

Read [AGENTS.md](AGENTS.md) and [Writing a channel](docs/writing-a-channel.md) to add a service.

MIT licensed.
