# adanext-channels, Ada's chat channels

One Go module with one plugin binary per chat app and a shared `sdk/`. Each channel speaks the
channel kind of hrn's plugin protocol v2 (cristiangirlea/harness#982) to Ada's core
(cristiangirlea/adanext-ada), which starts it, pairs senders and decides everything.

Issue #1 is the epic and lists the slices in order; take one slice per branch. `docs/` arrives
with the SDK slice; until then the issues are the design.

## Layout

| Path | Holds |
|---|---|
| `sdk/` | Shared code: protocol, splitting, formatting, retries, the fake core for tests. |
| `<channel>/` | One channel each (`telegram/`, `discord/`, ...): its `main`, manifest and tests. |
| `.claude/hooks/` | gofmt, push and hidden-character checks; their tests run in CI. |
| `.github/workflows/ci.yml` | CI on GitHub's hosted Ubuntu, Windows and macOS runners (public repository). |

## Build and test

Go as pinned in `go.mod`. What CI runs; run the same before a PR (`/check`):

```bash
go vet ./...
go build ./...
go test -count=1 ./...
go test -race -count=1 ./...                                  # Linux only in CI
test -z "$(gofmt -l .)"
for f in .claude/hooks/*.sh; do bash -n "$f"; done                # the hooks parse
sh .claude/hooks/go-rules.test.sh && sh .claude/hooks/push-guard.test.sh
sh .claude/hooks/hidden-chars.test.sh && sh .claude/hooks/hidden-chars.sh
```

`-race` needs cgo; where it is missing run `go test -count=3 ./...` and leave `-race` to CI.
Never attach self-hosted runners: a public repository's pull requests must never reach a
maintainer's machines.

## Rules of this repository

- **A channel decides nothing.** It reports who wrote what and delivers what it is told. Pairing,
  allowlists, approvals and permissions belong to Ada's core; a channel never answers a sender on
  its own.
- **Only its own secret and its own hosts.** A channel reads one token from its environment, never
  logs it, and contacts only the hosts its manifest lists.
- **Free services without payment details.** A channel that needs a paid plan or a payment card is
  not built without a maintainer's decision (issue #7).
- **Dependencies per channel.** A channel may use its service's maintained client library;
  `sdk/` stays on the standard library. Each new dependency is named and argued in its PR.
- **Tests never call a real service.** Fake service APIs and the fake core only; no tokens in
  tests or fixtures. A manual check with a test bot is recorded in the PR, never automated.
- Every PR updates `docs/` for a behaviour it adds or changes, and ticks its slice in the epic.

## Working rules

- **A branch delivers what it is for, nothing else.** One purpose per branch (one issue; short
  name, as in `telegram`) and one PR that closes it, listing the files it touches and why. A
  branch finishes everything its issue asks.
- **Review findings:** every defect, security point or missing test in what the branch touches is
  fixed in the branch before the push. Nits are dropped. Only work outside the branch, or a
  decision a maintainer must make, becomes an issue.
- **Everything is tested before a merge**: the full local checks (`/check`), tests for every
  behaviour the change adds or alters, CI green, an independent review with its findings fixed,
  and a security read of the diff with nothing open, all on the head being merged. Merge the
  default branch in once before those final checks, or for a conflict. Merge only through the PR
  (`gh pr merge <n> --merge --delete-branch --match-head-commit <sha>`). Never `--auto` or
  `--admin`, never push to `main`, never tag or release: hand over the command. Rulesets protect
  `main` and release tags (`v*` and `<channel>/v*`).
- Commit messages: the area first, lower case, then a plain sentence (`telegram: retry a delivery
  after a rate limit`). Findings and reasons go into the PR review, not one commit per finding. No
  emoji, no AI attribution anywhere.
- Tests first, seen failing. Files are LF (`.gitattributes`). Secrets are never read, printed or
  committed. Small diffs: don't restyle what you didn't change. Not sure? Ask, with one proposal.
