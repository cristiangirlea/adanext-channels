# adanext-channels

**Status: planned. Nothing here is usable yet.** The work is the epic, issue #1, and its slices.

The chat apps [Ada](https://github.com/cristiangirlea/adanext-ada) talks through, each as a
separate plugin program: installed on its own, holding only its own token, reaching only its own
service, and deciding nothing. Who may talk to Ada, and what she may do, is decided in Ada's core.

Planned, in order: Telegram, Discord and Slack; then Matrix, Signal and email. WhatsApp and Teams
wait for a decision on their costs and sign-ups (issue #7).

One repository, one binary per channel (`ada-channel-telegram`, ...), and a shared `sdk/`: the
channel protocol, message splitting, formatting, retries, and a fake core for tests. Contributors
and agents: read [AGENTS.md](AGENTS.md) first.

MIT licensed.
