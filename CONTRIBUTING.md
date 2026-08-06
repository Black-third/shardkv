# Contributing to shardkv

Thanks for looking. This file is the short version of how the project is built and what
a change has to satisfy before it lands. The long version lives in two places worth
reading first: [README.md](README.md) for what the server does and why, and
[CLAUDE.md](CLAUDE.md) for the invariants the implementation rests on.

## Two rules that are not negotiable

**No third-party dependencies.** `go.mod` has no `require` block and it stays that way.
The RESP codec, the skip list, the AOF, the replication protocol, the HyperLogLog, the
geohash and the CRC16/CRC64 implementations are hand-written *because that is the point
of the project* — a dependency that provided one of them would remove the thing worth
reading. The standard library is fair game; nothing else is. Test code is held to the
same rule: `docker run redis:7-alpine redis-cli` is how the suite talks to a real Redis
client, precisely so no client library has to be vendored.

**`go test -race ./...` stays green.** The README's claims about concurrency are the
project's whole thesis, so a data race is never an acceptable state to leave the tree
in — not on a branch, not "temporarily", not with a `// TODO`. If a change makes a race
appear, the change is wrong until it does not.

## Setting up

You need Go — the version in [`go.mod`](go.mod) is the floor, and CI also builds against
`stable`. Nothing else is required to build or test the server. Docker is needed only
for the container image and for the real-client checks below.

```bash
git clone https://github.com/Black-third/shardkv
cd shardkv
make build            # -> bin/shardkv
make run              # server on :6380 with an AOF under ./data
make help             # every target, with a one-line description
```

## The gate

`make check` runs what CI runs, in CI's order. Run it before you push; a green local run
means a green pipeline.

```bash
gofmt -l .                      # must print nothing
go vet ./...
go build ./...
go test -race -count=1 ./...
```

Two more that CI runs and `make check` does not, because they are slower:

```bash
make fuzz                       # fuzz the RESP parser (FUZZTIME=20s by default)
make lint                       # golangci-lint, which must report zero issues
```

`golangci-lint run` at **zero** issues is the standard, not "no new issues": the
configuration in `.golangci.yml` is the agreed set of checks, so a finding is either
fixed or the check is argued out of the config in the same PR. `make lint` installs the
linter into `GOPATH/bin` if it is missing.

The concurrency-sensitive tests are worth repeating locally when you have touched
blocking commands, the protocol or the databases, because a single green run does not
distinguish "correct" from "got lucky":

```bash
go test -race -count=3 \
  -run 'TestBlocking|TestClientUnblock|TestRESP|TestDatabase|TestSwapDB|TestSelectFrames' \
  ./internal/server
```

## Verifying against a real client

Wire compatibility with real Redis clients is a stated feature, so a protocol-level
change should be checked against one, not only through the Go tests. No local
`redis-cli` is needed:

```bash
go run ./cmd/shardkv -addr :6380 &
docker run --rm -i redis:7-alpine redis-cli -h host.docker.internal -p 6380 <<'EOF'
SET greeting hello
ZADD board 100 alice 200 bob
ZRANGE board 0 -1 WITHSCORES
INFO replication
EOF
```

Add `-3` to check the RESP3 side, which is where reply *shapes* differ (`HGETALL` is a
map, `SMEMBERS` a set, `ZSCORE` a double). For anything whose point is byte
compatibility — HyperLogLog register bodies, geohash scores, `DUMP` framing, error
strings — build the same thing on a real `redis:7-alpine` and compare the two outputs;
the README's "Verifying against a real client" notes in [CLAUDE.md](CLAUDE.md) give the
exact recipes.

The repository also carries a client-library compatibility matrix, which is the
stronger check because a library validates reply shapes and error text rather than
printing whatever arrives:

```bash
make compat                                   # all four libraries + their cluster clients
./test/compat/run.sh python ioredis           # only those
./test/compat/run.sh                          # same as `make compat`
```

The suites are `python` (redis-py, hiredis parser), `ioredis`, `noderedis` and `goredis`,
each in its own container, each also exercising that library's *cluster* client against a
three-node cluster. Every check runs twice — once against shardkv, once against a real
`redis:7-alpine` — and the matrix prints both columns. **Read the pair, not the shardkv
column.** A check that fails against both is a bug in the check or a quirk of the library;
only one that passes against Redis and fails here is an incompatibility, and only that
kind sets the exit status. If you add a check, add it so it runs unmodified against both
servers, or the reference column stops meaning anything.

The other half is Redis's own test suite, pointed at this server in external mode:

```bash
make compat-tcl
TCL_FILES="unit/type/hash unit/expire" ./test/compat/run.sh tcl   # narrow it while iterating
```

Read its output as **assertion counts, not pass or fail**. It reaches well past what this
server claims to implement, so files abort part-way; the number that matters is how many
of Redis's own assertions pass, and whether your change moved it. Each file runs in its
own invocation because the suite aborts a whole file on the first unsupported command —
so one missing command costs one file, not the run.

Two habits worth having with these harnesses. Set `PREFIX` if you want to run one
alongside another (two runs sharing container names destroy each other, and the wreckage
looks like a real finding — the runner now refuses rather than allowing it). And use
`KEEP=1` to leave the servers up when a single check is failing and you want to poke at it
with `redis-cli` yourself.

The deployment examples in [`deploy/`](deploy) are the other end-to-end check, and each
is one command:

```bash
docker compose -f deploy/docker-compose.single.yml  up -d --build
docker compose -f deploy/docker-compose.replica.yml up -d --build
docker compose -f deploy/docker-compose.cluster.yml up -d --build
```

## The invariants

Every failure mode this project actually fears is *silent divergence* — a master, its
replicas and its AOF quietly disagreeing — rather than a crash. Thirteen invariants in
[CLAUDE.md](CLAUDE.md#the-invariants-that-matter) are what rule that out, each with
tests. **If your change touches one of these areas, read that section first, and read
the tests it names.** In one line each:

1. Writes are totally ordered when propagation is active (`propMu` spans the mutation
   *and* the propagation; lock order `propMu` → `mu`).
2. The `write` flag drives durability, and a handler's `dirty` return decides whether a
   write is actually propagated.
3. Deadlines are absolute on the wire, derived from exactly one `store.Now()` reading —
   the handler resolves the deadline and returns the absolute wire form built from that
   same value; nothing on the propagation path reads a clock.
4. Non-deterministic commands propagate their *effect*, not their text — including
   everything whose result depends on the clock.
5. `Dump()` output must be replayable, chunked so no command exceeds the protocol's
   limits, and a stream's dump carries its id counters, groups, consumers and
   pending-entries list.
6. A replica that falls behind is dropped, not skipped. Same for a slow subscriber or
   monitor.
7. `affectedKeys` must list every key a write touches, or `WATCH` misses the conflict.
   Cluster routing reads the same extraction, deliberately.
8. Cross-shard writes lock shards in index order (`lockKeys`/`rlockKeys`); cross-*database*
   writes serialize on `crossDBMu`.
9. A blocked client holds no lock, and only the head of a key's wait queue is woken.
10. A blocking command propagates its effect, never itself, and does not block inside
    `MULTI`.
11. The database is part of the propagated stream (`SELECT n`, emitted lazily).
12. An observer that is not watching costs nothing, and a statistic must not lie.
13. In cluster mode, a key's slot decides which node may serve it — and the check is on
    the client path only, never on a replica applying its master's stream.

## Adding a command

The checklist, in order:

1. **Store method** next to its type's existing ones in `internal/store/`, with a doc
   comment covering the edge cases: missing key, empty collection, wrong type, negative
   index.
2. **Handler** in the matching `internal/server/commands_*.go`, registered with the
   right arity and `write` flag — `registerEffect` instead if the result depends on
   randomness, map iteration order or the clock (invariant 4).
3. **Relative expiry?** Register it with `registerEffect` and return the absolute wire
   form built from the deadline the handler already resolved — never from a second clock
   reading (invariant 3).
4. **More than one key?** Add it to `affectedKeys`, and to `crossDBTarget` if it writes
   a second database (invariants 7 and 8).
5. **Test it at the wire level**: happy path, missing key, empty collection, wrong type,
   out-of-range index, arity error, and the exact error strings Redis returns. Add a
   replica-convergence test if propagation is involved.
6. **Update the README's Commands table.**

Step 5's "exact error strings Redis returns" is not a stylistic preference: client
libraries match on error text to decide which exception to raise, so a reworded error is
a compatibility break that no Go test would notice on its own.

## Commits and pull requests

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org),
because the release changelog is generated from them — `.goreleaser.yml` groups `feat`,
`fix`, `perf` and `docs` into sections and drops `test`, `chore`, `ci`, `style` and
`refactor` entirely.

```
feat(stream): XAUTOCLAIM propagates the ids it claimed
fix(cluster): MIGRATE deletes the source key only after the destination acks
docs: state the cluster-bus boundary in full
perf(store): avoid an allocation per SCAN cursor
```

- Subject in the imperative mood, no trailing period, under ~72 characters.
- The body is for **why**, not what — the diff already says what. If a comment in the
  code would be the better home for that reasoning, put it there instead; this codebase
  prefers comments that explain the failure a construct prevents.
- A commit that changes behaviour includes its tests. A commit that only adds tests is
  fine on its own.

For pull requests:

- One logical change per PR. A refactor and a feature in the same diff make both harder
  to review and impossible to revert independently.
- Fill in the [pull request template](.github/PULL_REQUEST_TEMPLATE.md) — in particular
  the invariants checklist, which exists because those are the failures that do not show
  up as a failing test on the author's machine.
- Say what you ran. "`make check` is green, plus `./test/compat/run.sh python`" is a
  useful sentence; "tests pass" is not.
- Rebase rather than merge to keep the history linear, and squash fixup commits before
  review.

## Releases

Maintainers only, and mostly automated:

1. Bump `const Version` in `internal/server/commands_admin.go` — the linker cannot stamp
   a constant, so the version in the source *is* the version, and the tag has to agree
   with it.
2. Tag and push: `git tag -a v0.4.0 -m 'shardkv 0.4.0' && git push origin v0.4.0`.
3. `.github/workflows/release.yml` re-runs the gate and GoReleaser builds the
   linux/darwin × amd64/arm64 archives, `checksums.txt` and the changelog;
   `.github/workflows/docker.yml` pushes the multi-arch image to `ghcr.io`.

## Reporting a bug, and reporting a vulnerability

Bugs go through the [issue forms](https://github.com/Black-third/shardkv/issues/new/choose);
the bug form asks for the exact command sequence, shardkv's reply *and* real Redis's
reply, the client library and version, and the server's version and flags, because with
those four a wire incompatibility is usually reproducible in one paste.

Security issues do **not** go in the issue tracker — see [SECURITY.md](SECURITY.md).
