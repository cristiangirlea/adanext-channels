# adanext-channels

The chat apps [Ada](https://github.com/cristiangirlea/adanext-ada) talks through, each as a
separate plugin program: installed on its own, holding only its own token, reaching only its own
service, and deciding nothing. Who may talk to Ada, and what she may do, is decided in Ada's core.

**Available:** Telegram, Discord, Slack and Matrix, the shared SDK, and an echo channel for
tests. **Next:** Signal and email. WhatsApp and Teams wait for a decision on their costs and
sign-ups ([issue #7](https://github.com/cristiangirlea/adanext-channels/issues/7)).
[Issue #1](https://github.com/cristiangirlea/adanext-channels/issues/1) tracks all of them.

| Path | Purpose |
|---|---|
| `sdk/` | Shared runtime and helpers. The SDK uses only the standard library and harness's contracts module. |
| `sdk/fakecore/` | Test host with a store that survives channel restarts. |
| `echo/` | Reference binary, `channel.json` and end-to-end tests. |
| `telegram/` | Telegram Bot API channel. See [Telegram setup and privacy](docs/telegram.md). |
| `discord/` | Discord DMs and selected channels in one server. See [Discord setup and recovery](docs/discord.md). |
| `slack/` | Slack DMs, selected app mentions and buttons over Socket Mode. See [Slack setup and recovery](docs/slack.md). |
| `matrix/` | Matrix direct rooms and optional group rooms. See [Matrix setup and recovery](docs/matrix.md). |

Read [AGENTS.md](AGENTS.md) and [Writing a channel](docs/writing-a-channel.md) to add a service.

MIT licensed.
