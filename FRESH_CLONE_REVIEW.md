# FRESH_CLONE_REVIEW.md — Phase 16 Fresh-Clone Reviewer Test

Performed against a real `git clone` of this repo's local `.git` history
into a clean temp directory (not the working tree with in-progress
files), following `README.md` exactly, pretending to have never seen
the repo.

## Steps performed

1. `git clone` into a clean temp dir — succeeded; confirmed only
   committed files are present (none of the working-notes files like
   `CLAUDE.md`/`PROMPT.md`, which are intentionally uncommitted).
2. `go build ./...` — clean, no errors.
3. `go test ./...` — all packages pass.
4. `go test -race ./...` — all packages pass, no races.
5. `make demo` — all 9 steps PASS, real HTTP calls, ~6s runtime,
   self-cleaning (no leftover temp dirs after exit).
6. Started the real server (`go run ./cmd/server`), created a queue,
   enqueued a message via `curl`, confirmed it in `stats`.
7. Killed the server with a **real `kill -9`** (not the demo's
   in-process restart) — a genuine OS-level process death mid-session.
8. Restarted the server as a **new process** against the same WAL file
   and confirmed via `curl` that the message survived with its exact
   original ID and `enqueued_at` timestamp — this is strictly stronger
   evidence of restart durability than the in-process `make demo`
   restart alone, since it crosses a real process boundary.
9. Scanned for secrets/credentials/private keys — none found.
10. Ran `go vet ./...` and `gofmt -l .` across the fresh clone.
11. Cross-checked every table/claim in README against this session's
    actual command output (status codes, endpoint list, test count).

## Friction found and fixed

1. **No `.gitignore`** — following the README's own "Quick start"
   (`go run ./cmd/server`) creates `frankenqueue.wal` in the repo root
   with nothing to stop it from being accidentally committed. Fixed:
   added `.gitignore` (`*.wal`, `/frankenqueue.wal`).
2. **`gofmt -l .` flagged one file** — a stale comment-alignment
   artifact in `internal/queue/queue_test.go` (a line's trailing
   comment was misaligned relative to a neighboring line, from an
   earlier edit). Cosmetic only, no semantic change. Fixed: `gofmt -w`.

## Friction found, not fixed (low-value / out of scope)

- `go.mod` pins `go 1.27`; a reviewer on an older toolchain would need
  to bump it or install a matching version. Not fixed — no evidence
  this project needs anything from 1.27 specifically, but changing the
  module's Go version is a real semantic decision, not "friction," and
  isn't warranted by anything this review found.

## README claims verified against actual output

- All 7 HTTP endpoints and their status codes (README's HTTP API
  table) — matched live `curl` output and `internal/httpapi/handlers.go`
  exactly.
- Restart durability — verified more strongly than the README's own
  demo (real process kill, not just in-process restart).
- `make demo`'s 9-step pass/fail summary — reproduced verbatim.
- `go build`, `go test ./...`, `go test -race ./...` — all clean, as
  claimed.
- No secrets, no committed binaries or WAL files.

No other high-value friction was found. This review did not uncover
any README claim that didn't match actual behavior.
