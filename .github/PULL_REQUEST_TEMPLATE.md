<!--
Thank you. Please read CONTRIBUTING.md if you have not: the gate below is not
box-ticking, it is what keeps a silent divergence between a master, its replicas and
its AOF from reaching main. Delete whatever does not apply.
-->

## What this changes

<!-- One paragraph. What it does, and why it is worth doing. -->

## Why this way

<!--
The part that is most useful to a reviewer here: the alternative you rejected and the
reason. This codebase documents its reasoning in comments rather than only its
behaviour, and a PR that explains the choice tends to produce the comment that should
be in the code.
-->

## The gate

All of these, locally, before review:

- [ ] `gofmt -l .` prints nothing
- [ ] `go vet ./...`
- [ ] `go build ./...`
- [ ] `go test -race -count=1 ./...` — the race detector is not optional; the
      concurrency claims in the README are the project's whole thesis
- [ ] `golangci-lint run` reports **0 issues** (fix the code rather than widening
      `.golangci.yml`)
- [ ] `make fuzz` if anything under `internal/resp` changed

## If this adds or changes a command

- [ ] The store method sits next to its type's existing ones, with a doc comment
      covering the edge cases: missing key, empty collection, wrong type, negative index
- [ ] Registered with the right arity and `write` flag — and with `registerEffect` if
      the result depends on randomness, on map iteration order, or on the clock
      (invariant 4)
- [ ] A relative expiry is rewritten to its absolute form in `propagationForm`
      (invariant 3)
- [ ] Every key the command touches is listed in `affectedKeys` — including keys that
      are not the first argument. `WATCH` and cluster routing both read that list, and a
      key missing from it fails silently in both (invariants 7 and 13)
- [ ] A write spanning two databases is registered in `crossDBTarget` (invariant 8)
- [ ] Wire-level tests: happy path, missing key, empty collection, wrong type,
      out-of-range index, arity error, and the **exact error strings real Redis returns**
- [ ] A replica-convergence test if anything propagates
- [ ] The README's Commands table is updated

## Verified against a real Redis

<!--
For anything protocol-level, paste the comparison. No local redis-cli is needed:

    docker run --rm -d --name real-redis -p 6399:6379 redis:7-alpine
    docker run --rm -i redis:7-alpine redis-cli -h host.docker.internal -p 6399

Add -3 for the RESP3 side, which is where the reply shapes differ. If the change
touches the client-facing surface, `./test/compat/run.sh` drives real client libraries
against both servers and prints the difference.
-->

- [ ] Checked against `redis:7-alpine`, RESP2 and RESP3
- [ ] `./test/compat/run.sh` shows no new incompatibility
- [ ] Not applicable

## Invariants

- [ ] I read the thirteen invariants in `CLAUDE.md` and this change does not break one
- [ ] This change touches one of those areas, and I read its tests first

## Anything left undone

<!-- Known gaps, follow-ups, things deliberately out of scope. Being explicit here is
     worth more than a tidy diff. -->
