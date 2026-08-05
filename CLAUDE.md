# shardkv — working notes

A Redis-wire-compatible, sharded, in-memory data-structure server in Go. No
third-party dependencies: everything (RESP codec, skip list, AOF, replication) is
hand-written, which is the point of the project.

## Layout

```
cmd/shardkv        entrypoint: flags, signal handling, AOF/replication wiring
internal/store     sharded store, typed values, skip-list sorted set, LRU eviction, snapshot
                   stream.go (ms-seq ids, entries, consumer groups, PELs)
                   bitmap.go (bit addressing over the string type, BITOP, BITFIELD)
                   hyperloglog.go (Redis's HYLL format: sparse/dense, murmur64a, tau/sigma)
                   memory.go (the per-key footprint estimate MEMORY USAGE reports)
internal/resp      RESP protocol reader/writer (+ fuzz target)
internal/server    TCP server, command table + handlers, transactions, replication
                   observability.go (slow log, MONITOR, latency monitor, per-command stats)
                   geohash.go (52-bit geohash, haversine, the nine-cell search area)
                   cluster_slots.go (CRC16-XMODEM, hash tags, key -> slot)
                   cluster.go (copy-on-write slot map, node table, nodes.conf, MEET)
                   cluster_redirect.go (MOVED/ASK/CROSSSLOT/TRYAGAIN/CLUSTERDOWN)
                   commands_dump.go (the DUMP payload format, RESTORE, MIGRATE)
internal/aof       append-only-file persistence: append, fsync policy, replay, rewrite
```

## Commands

```bash
make check      # what CI runs: fmt-check, vet, build, test -race
make test       # go test -race -count=1 ./...
make fuzz       # fuzz the RESP parser (FUZZTIME=20s by default)
make bench      # store benchmarks with allocation counts
make lint       # golangci-lint (installs into GOPATH/bin if missing)
make run        # server on :6380 with an AOF under ./data
make docker     # build the container image
```

`go test -race ./...` must stay green — the concurrency claims in the README are
the project's whole thesis, so a race is never an acceptable state to leave the
tree in.

## The invariants that matter

Break one of these and the failure is silent divergence between a master, its
replicas, and its AOF — not a crash. Every one of them has tests; if you change
this area, read them first.

1. **Writes are totally ordered when propagation is active.** A write holds
   `propMu` across *both* the store mutation and the propagation, so the order
   applied to memory is exactly the order written to the AOF and shipped to
   replicas. A pure single-node cache (no AOF, no replicas) skips `propMu`
   entirely and keeps sharded-concurrent writes. Lock order is `propMu` → `mu`
   and `propMu` → `watchMu`; store removal hooks always fire outside shard locks.

2. **The `write` flag drives durability.** A command registered with
   `write: true` is persisted and replicated automatically *when its handler
   reports it actually changed something* (the `dirty` return). Returning `dirty`
   for a no-op write propagates noise; returning `false` for a real change loses
   it.

3. **Deadlines are absolute on the wire.** Relative TTLs (`EXPIRE`, `SET EX`,
   `SETEX`, `GETEX EX`, …) are rewritten to absolute form (`PEXPIREAT`,
   `SET … PXAT`) by `propagationForm` before reaching the AOF or a replica, so a
   replay reconstructs the same expiry instant however much later it happens.
   Server-side "now" always comes from `store.Now()`, never `time.Now()`, so an
   injected clock keeps memory and the wire in agreement.

4. **Non-deterministic commands propagate their effect, not their text.**
   `SPOP`, `ZPOPMIN`/`ZPOPMAX`, `INCRBYFLOAT`, `HINCRBYFLOAT` register with
   `registerEffect` and ship what they actually did (`SREM <members popped>`,
   `SET key <result> KEEPTTL`, …). Shipping the command verbatim would have the
   replica pick different members and diverge silently. Any new command whose
   result depends on randomness or map iteration order must do the same.

   **The clock is a source of non-determinism too**, and the stream commands are
   where it shows: `XADD *` takes its id from the clock, `XCLAIM`/`XAUTOCLAIM` act on
   entries "idle for at least N ms" and stamp a new delivery time, `XREADGROUP`
   depends on what the group had already delivered, and `XGROUP CREATE|SETID ... $`
   resolves against the stream's current end. All of them register with
   `registerEffect` and ship concrete ids and *absolute* `TIME` operands -- the same
   absolute-instant discipline invariant 3 requires of every TTL. A replica replaying
   `XADD *` would mint its own id and every id in the two streams would disagree from
   that moment on, silently, because both copies would look internally consistent.
   The deterministic members of the family (`XDEL`, `XTRIM`, `XACK`, `XSETID`,
   `XGROUP DESTROY|CREATECONSUMER|DELCONSUMER`, and the whole bitmap, HyperLogLog and
   GEO surface) propagate verbatim, which for `PFADD` is also far cheaper than
   shipping the 12 KB value it produced.

5. **`Dump()` output must be replayable.** Collections are chunked (256 elements
   per emitted command) because a single command carrying a large collection
   exceeds `resp.MaxMultiBulk`, and the reader rejects the *whole stream* rather
   than the one oversized command. A chunk boundary must never split an `HSET`
   field/value or `ZADD` score/member pair, and a key's `PEXPIREAT` follows its
   last chunk.

   **A stream's state is not only its entries.** `Dump` must also emit the id
   counters no entry records (`XSETID ... ENTRIESADDED ... MAXDELETEDID ...`) and
   every consumer group: its last-delivered id and read counter
   (`XGROUP CREATE ... ENTRIESREAD`), each of its consumers
   (`XGROUP CREATECONSUMER`, so a consumer with nothing outstanding survives), and
   every pending entry with its delivery time and retry count
   (`XCLAIM ... TIME ... RETRYCOUNT ... FORCE JUSTID`). A pending-entries list is the
   record of *work in flight*: a snapshot that dropped it would silently re-deliver
   acknowledged work, or silently lose outstanding work, on every restart and every
   replica sync, and nothing would report it. The order matters -- entries, then
   `XSETID` (refused below the top entry), then groups (they need the key), then each
   group's consumers before its pending entries. One command per entry rather than per
   256, because each entry names its own id and so cannot share an `XADD`.

6. **A replica that falls behind is dropped, not skipped.** Skipping a write for
   a full-buffered replica leaves it permanently and silently diverged with
   nothing to ever tell it so; dropping the feed ends the connection so it
   reconnects and resyncs. The same reasoning applies to slow Pub/Sub
   subscribers.

7. **`affectedKeys` must list every key a write touches**, or `WATCH` misses the
   conflict and `EXEC` succeeds when it should abort. Multi-key writes (`COPY`,
   `SMOVE`, `LMOVE`, `RPOPLPUSH`, `RENAME`/`RENAMENX`, `MSET`, `DEL`/`UNLINK`)
   are the ones that get forgotten. So are the commands whose key is *not* the first
   argument: `XGROUP <sub> <key>` and `BITOP <op> <dest>` both put it second, and the
   default of "the first argument" would silently name a subcommand as a key -- which
   also means a client blocked on `XREADGROUP` would never be woken by the
   `XGROUP SETID` that an `XREADGROUP` effect ships. `COMMAND GETKEYS` is built on the
   same extraction (see `commandKeys`), deliberately: two tables would drift, and the
   one that drifted silently is the one `WATCH` depends on.

8. **Cross-shard writes lock shards in index order.** Use `Store.lockKeys` /
   `rlockKeys`; ordering by key *name* does not prevent deadlock, and duplicate
   or same-shard key pairs have to be handled. A write that spans two *databases*
   (`MOVE`, `COPY ... DB`, `SWAPDB`) holds a lock in two independent stores, where
   no shard ordering can help, so all three are serialized against each other by
   `crossDBMu`. Single-database commands never hold locks in two stores and so can
   never join such a cycle.

9. **A blocked client holds no lock.** By the time a blocking command decides to
   wait, the store call has returned, `propMu` is released, and the connection's
   writer lock has been handed back so a subscribed connection's Pub/Sub pump keeps
   running. The wait itself is a `select` on the waiter's own single-slot channel;
   the only state it owns is its place in the per-key FIFO queues under `blockMu`,
   a leaf lock held for the length of a map lookup. Wakeup is a non-blocking send
   from the write path, so a push never waits on a waiter, and **only the head of
   a key's queue is ever woken** -- that is what makes the wakeup FIFO, because a
   client behind the head is never told there might be data and so cannot race for
   it. A waiter that is served hands the queue on before it leaves. When nobody is
   blocked, the whole feature is one atomic load on the write path.

10. **A blocking command propagates its effect, never itself.** `BLPOP` ships the
    `LPOP` it performed, `BLMOVE` the `LMOVE`, `BZPOPMIN` the `ZREM`. A replica
    replaying `BLPOP` would wait forever on a connection that has no client behind
    it. For the same reason a blocking command reports its *effect* to `WATCH` and
    to keyspace notifications rather than its own arguments: its arguments are a
    list of candidate keys, and only the effect names the key that changed. Inside
    `MULTI`/`EXEC` it must not block at all -- it takes its non-blocking behaviour,
    because an `EXEC` that could wait would hold the batch and `propMu` with it.

11. **The database is part of the propagated stream.** The AOF and the replica
    stream are a flat sequence of commands with no database context, so a write in
    database *n* is preceded by `SELECT n` (`selectOnStream`), emitted lazily -- only
    when the stream's position actually changes. A database-0-only workload
    therefore ships byte-for-byte what it shipped before databases existed, which is
    what keeps an older AOF replayable. A snapshot (`dumpAll`) frames each database
    the same way and **ends by returning the replayer to the position the shared
    stream is in**: the stream's position is shared by every replica while a
    snapshot goes to one of them, so a snapshot that ended elsewhere would silently
    redirect every command that followed it. `WATCH` registrations, blocked-client
    queues and keyspace-notification channels are all keyed by `(database, key)`;
    Pub/Sub channels are global, as in Redis, which is why the database appears in
    the `__keyspace@<n>__` name rather than anywhere else.

12. **An observer that is not watching costs nothing, and a statistic must not lie.**
    Every observability hook sits on the path of every command, so each is gated by a
    single atomic load *before* any string is built or any argument copied: the slow log
    by its threshold, the latency monitor by its threshold, `MONITOR` by a count of
    attached monitors. The always-on part -- the per-command statistics behind
    `INFO commandstats`/`latencystats` -- is three atomic adds on the `*command` the
    dispatcher already holds, which is why those counters live on the table entry rather
    than in a side map: a map keyed by name would cost a hash and a lock per command.
    `TestObservabilityCostsNothingWhenUnused` pins all of it with `AllocsPerRun`, and it
    finishes by *enabling* the slow log and checking an entry appears, so the measurement
    is of a disabled path and not a broken one.

    Two things about what the numbers mean. `keyspace_hits`/`keyspace_misses` are
    counted in `Store.readEntry`, a helper used *only* by methods holding the shard's
    shared lock -- not in `liveEntry`, which resolves keys for writes as well, and where
    a counter would report an `LPUSH` that created a list as a cache miss. "This is a
    read" is therefore structural: a write cannot reach that function without taking the
    exclusive lock the read methods do not take. And time a client spent parked in a
    blocking command is subtracted (`session.blockedFor`) before the slow log and the
    latency histogram see it, because a `BLPOP` that waited five seconds is not the
    server being slow -- without that, every timed-out blocking command would drown the
    slow log.

    `MONITOR` is also the one feature that can leak a credential, since it echoes
    everything clients send: `redactedArgs` is the single place that decides what a
    monitor may see, and it must keep covering `AUTH`, `HELLO ... AUTH` and
    `CONFIG SET requirepass|masterauth`. A monitor that stops reading is dropped, not
    waited for, on the same contract as a slow replica or subscriber (invariant 6).

13. **In cluster mode, a key's slot decides which node may serve it.** A node that
    answers for a slot it does not own does not produce an error -- it produces a second
    copy of the data, on a node no client will consult again once the slot map is read,
    and nothing anywhere reports it. Same failure shape as every invariant above.

    The decision is taken once, in `executeCommand`, *before* anything runs and before
    anything is queued -- a `MULTI` must not accumulate commands this node has no right
    to apply, which is why a redirected queued command sets `queueErr` and a redirected
    `EXEC` discards the batch outright. `EXEC` is checked against the union of its
    queued commands' keys, because a transaction runs on one node or not at all.

    Three things about that check are load-bearing.

    **It reads the same key extraction `WATCH` does** (`commandKeys` -> `affectedKeys`,
    invariant 7). Routing must never grow a list of its own: two lists drift, and the
    drift is silent in *both* directions -- a command missing from the routing list is
    served by the wrong node, one missing from the `WATCH` list commits over a concurrent
    change. `MIGRATE` is the case that proves it, since its keys are at argument 3 or
    after a `KEYS` keyword and the default would have named the destination *host* as a
    key.

    **It is gated by one atomic load** (`ClusterEnabled`), so a standalone server never
    computes a slot or builds a key slice -- invariant 12's discipline applied to a gate
    that sits on every command. The slot map itself is copy-on-write: a lookup is one
    atomic load and an index, and a multi-key command resolves every key against one
    immutable generation, so a concurrent `SETSLOT` cannot manufacture a `CROSSSLOT` that
    was never true. Nodes are immutable for the same reason -- the table holds them by
    pointer and is read without a lock.

    **It is on the client path only.** `dispatch` -- what an AOF replay and a master's
    stream go through -- is deliberately never redirected: a replica must apply every
    write its master sends whatever its own slot map says, or it silently drops data it
    is supposed to hold. Redis draws the same line (`mustObeyClient`).

    A migration is the case where ownership and *possession* disagree, and the three
    answers are different for a reason: the owner still holding the key serves it; the
    owner that has handed it over answers `ASK` (not `MOVED` -- ownership has not moved,
    so the client must not update its routing table); and the importing node serves only
    a client that sent `ASKING`, for exactly one command. A flag that persisted would let
    a node serve a slot it does not own, which is the split-brain the whole protocol
    exists to prevent. `MIGRATE` deletes a key here only *after* the destination
    acknowledged it, and propagates the `DEL` of what left rather than itself (invariant
    4: its non-determinism is another machine).

    What is *not* implemented is the cluster bus, so configuration does not propagate:
    every node must be told. `CLUSTER MEET` is the single exception and adopts only slots
    that are locally unassigned, so it can never move a slot -- two nodes that disagree
    keep disagreeing visibly instead of silently swapping ownership. See the README's
    Cluster section for the full boundary.

## Adding a command

1. Add the store method next to its type's existing ones (`internal/store/`),
   with a doc comment covering the edge cases: missing key, empty collection,
   wrong type, negative index.
2. Add the handler in the matching `internal/server/commands_*.go`, registered
   with the right arity and `write` flag (`registerEffect` if it is
   non-deterministic).
3. If it takes a relative expiry, add its rewrite to `propagationForm`.
4. If it writes more than one key, add it to `affectedKeys` (and, if it writes a
   second *database*, to `crossDBTarget`).
5. Test it at the wire level: happy path, missing key, empty collection, wrong
   type, out-of-range index, arity error, and the exact error strings Redis
   returns. Add a replica-convergence test if propagation is involved.
6. Update the README's Commands table.

## Verifying against a real client

No local `redis-cli` is needed — use the official image:

```bash
go run ./cmd/shardkv -addr :6380 &
docker run --rm -i redis:7-alpine redis-cli -h host.docker.internal -p 6380 <<'EOF'
SET greeting hello
ZADD board 100 alice 200 bob
ZRANGE board 0 -1 WITHSCORES
XADD orders * item widget
XGROUP CREATE orders fulfil 0
XREADGROUP GROUP fulfil alice STREAMS orders >
XPENDING orders fulfil
PFADD uniques a b c
GEOADD Sicily 13.361389 38.115556 Palermo
GEODIST Sicily Palermo Palermo
BITCOUNT greeting
INFO replication
EOF
```

The three families worth checking against a *real* Redis rather than only against
`redis-cli`, because the point of them is byte compatibility:

```bash
docker run --rm -d --name real-redis -p 6399:6379 redis:7-alpine
# Build the same HyperLogLog on both and compare GET output: the register bodies must
# be identical (only the cached-cardinality field differs, deliberately -- see the
# README's design notes), and each server must count the other's sketch to the same
# number. The same for a geo set's ZSCORE, and for GEODIST/GEOHASH output.
```

Add `-3` to check the RESP3 side, which is where the reply *shapes* differ:

```bash
docker run --rm -i redis:7-alpine redis-cli -3 -h host.docker.internal -p 6380 <<'EOF'
HGETALL user:1
SMEMBERS tags
ZSCORE board bob
EOF
```

Two caveats found while doing this: `redis-cli` in non-interactive (piped) mode
cannot print the big-number or attribute types and fails with a protocol error --
verified to fail identically against real `redis:7-alpine`, so it is a client
limitation and not a server bug. Use the raw-socket form to check those.

Wire compatibility with real clients is a stated feature, so protocol-level
changes should be checked this way and not only through the Go tests.

For cluster mode, the real test is a cluster-aware client following the redirects, since
that is the only thing that exercises the reply *formats* rather than their contents:

```bash
for p in 7001 7002 7003; do
  go run ./cmd/shardkv -addr :$p -cluster-enabled -cluster-config-file /tmp/n$p.conf \
      -cluster-announce-ip host.docker.internal &
done
# slots first, then the MEET mesh: MEET adopts only slots that are unassigned locally,
# and it is one-directional, so run it on each node for each peer.
docker run --rm -i redis:7-alpine redis-cli -h host.docker.internal -p 7001 CLUSTER ADDSLOTSRANGE 0 5460
# ... 7002: 5461-10922, 7003: 10923-16383, then MEET each pair over 127.0.0.1 ...
docker run --rm -i redis:7-alpine redis-cli -c -h host.docker.internal -p 7001 <<'EOF'
SET foo bar
MSET {user1000}.following 1 {user1000}.followers 2
MGET foo hello
EOF
```

Two addresses are in play and confusing them is the mistake to expect.
`-cluster-announce-ip` is what *clients* are redirected to; `CLUSTER MEET` and
`MIGRATE`'s host operand are dialled by the *node*. On a host driven by a containerised
`redis-cli` those differ (`host.docker.internal` vs `127.0.0.1`), and a `MIGRATE` given
the announce name fails with `IOERR` because the node cannot resolve it.

`CLUSTER KEYSLOT` must be checked against a *cluster-enabled* real Redis
(`redis-server --cluster-enabled yes`); a standalone one answers
`ERR This instance has cluster support disabled`.
