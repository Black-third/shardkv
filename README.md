# shardkv

[![CI](https://github.com/Black-third/shardkv/actions/workflows/ci.yml/badge.svg)](https://github.com/Black-third/shardkv/actions/workflows/ci.yml)

A concurrent, sharded, in-memory data-structure server written in Go — a compact
Redis: five data types, append-only-file persistence, and primary–replica
replication, all wire-compatible with the Redis protocol (RESP) so `redis-cli`
and any Redis client library work against it unchanged.

`shardkv` is a study in the systems fundamentals that matter most: **concurrency
and synchronization, network server design, data structures, and durability.**
It partitions the keyspace across many independently-locked shards so thousands
of clients can read and write in parallel without serializing behind one global
lock, and the whole suite — including tests that hammer shared state from dozens
of goroutines and TCP clients — passes under the Go race detector.

```text
                        ┌────────────────────────────────────────────┐
   redis-cli ──TCP──▶   │  server: one goroutine per connection        │
   client lib ──TCP──▶  │  RESP parse ─▶ command table ─▶ RESP reply    │
   replica ───PSYNC─▶   │  write commands ─▶ AOF + replica stream       │
                        └───────────────────────┬──────────────────────┘
                                                │ concurrent calls
                        ┌───────────────────────▼──────────────────────┐
                        │  store: N shards (power of two)               │
                        │  key ─ FNV-1a ─ (hash & mask) ─▶ shard        │
                        │   ┌────────┐ ┌────────┐      ┌────────┐       │
                        │   │RWMutex │ │RWMutex │  …   │RWMutex │       │
                        │   │ string │ │ list   │      │ zset   │       │
                        │   │ hash   │ │ set    │      │ (skip  │       │
                        │   │ …      │ │ …      │      │  list) │       │
                        │   └────────┘ └────────┘      └────────┘       │
                        └───────────────────────────────────────────────┘
```

## Features

- **Five data types** — strings, lists, hashes, sets, and **sorted sets backed
  by a hand-written skip list** with O(log n) insertion, deletion, and rank
  queries.
- **Sharded locking** — the keyspace is split across `N` power-of-two shards,
  each with its own `RWMutex`, so operations on different keys run in true
  parallel. Shard selection is an allocation-free FNV-1a hash plus a bitmask.
- **AOF persistence** — every write is appended to a log (configurable fsync
  policy: `always` / `everysec` / `no`) and replayed on startup. Supports
  compacting rewrite.
- **Primary–replica replication** — a replica issues `PSYNC`, receives a
  snapshot, then applies the master's live write stream; replicas are read-only.
- **Transactions** — `MULTI`/`EXEC`/`DISCARD` command batching, with
  `WATCH`/`UNWATCH` optimistic locking: `EXEC` aborts if a watched key was
  modified by another client.
- **Approximate-LRU eviction** — an optional `maxkeys` cap; a background pass
  samples keys and evicts the least-recently-used, Redis-style.
- **TTL expiration** — lazy on read plus a background janitor that reclaims
  memory.
- **Self-registering command table** — each command declares an arity and a
  `write` flag; the `write` flag is what automatically drives both AOF
  persistence and replica propagation, so new mutating commands wire themselves
  into durability and replication.
- **Observability** — `INFO` (role, uptime, connections, commands, keyspace),
  `DBSIZE`, `TYPE`.
- **Tested hard** — unit + integration tests under `-race`, plus a Go **fuzz
  test** for the RESP parser.

## Commands

| Group   | Commands |
| ------- | -------- |
| Strings | `SET key val [EX s\|PX ms]` · `GET` · `INCR` · `DECR` · `MSET` · `MGET` |
| Keys    | `DEL` · `EXISTS` · `EXPIRE` · `TTL` · `TYPE` · `KEYS` |
| Lists   | `LPUSH` · `RPUSH` · `LPOP` · `RPOP` · `LRANGE` · `LLEN` |
| Hashes  | `HSET` · `HGET` · `HDEL` · `HGETALL` · `HLEN` |
| Sets    | `SADD` · `SREM` · `SMEMBERS` · `SISMEMBER` · `SCARD` |
| Sorted  | `ZADD` · `ZSCORE` · `ZRANK` · `ZRANGE [WITHSCORES]` · `ZREM` · `ZCARD` |
| Tx      | `MULTI` · `EXEC` · `DISCARD` · `WATCH` · `UNWATCH` |
| Server  | `PING` · `INFO` · `DBSIZE` · `FLUSHALL` · `REPLICAOF` · `COMMAND` |

## Quick start

```bash
go run ./cmd/shardkv -addr :6380

# real redis-cli works against it:
redis-cli -p 6380 set greeting hello
redis-cli -p 6380 zadd leaderboard 100 alice 200 bob 150 carol
redis-cli -p 6380 zrange leaderboard 0 -1 withscores   # alice 100 carol 150 bob 200
redis-cli -p 6380 lpush tasks build test ship
redis-cli -p 6380 hset profile lang Go role systems
redis-cli -p 6380 info replication

# or with no Redis installed, a raw socket:
printf 'SET foo bar\r\nGET foo\r\n' | nc 127.0.0.1 6380
```

## Persistence (AOF)

```bash
go run ./cmd/shardkv -aof data.aof -aofsync everysec
```

Every write command is appended to `data.aof` in RESP. On the next start the log
is replayed to rebuild the dataset. `-aofsync` trades durability against
throughput: `always` fsyncs after each write, `everysec` flushes once per second
in the background (default), `no` leaves flushing to the OS.

## Replication

```bash
# terminal 1 — master
go run ./cmd/shardkv -addr :6380

# terminal 2 — replica
go run ./cmd/shardkv -addr :6381 -replicaof 127.0.0.1:6380
```

The replica connects, receives a snapshot of the master's current dataset, then
applies the master's live write stream. Replicas reject client writes
(`READONLY`). Replication can also be toggled at runtime with `REPLICAOF host
port` / `REPLICAOF NO ONE`.

**Consistency model.** Replication is asynchronous and eventually consistent.
The replica is registered for the live stream before the initial snapshot is
taken, so a write that races the snapshot may be applied twice — fine for
idempotent operations. Making initial sync exact would require a
copy-on-write snapshot or a replication offset/backlog (as real Redis does);
that trade-off is called out deliberately rather than hidden.

## Transactions

```bash
redis-cli -p 6380 <<'EOF'
WATCH balance
MULTI
DECRBY balance 10
INCR transfers
EXEC
EOF
```

`MULTI` begins a batch; subsequent commands reply `QUEUED` and are validated for
arity/existence (a bad command makes the whole `EXEC` fail with `EXECABORT`).
`EXEC` runs the batch and returns the array of replies; `DISCARD` throws it away.
`WATCH` adds optimistic locking: if any watched key is modified — by this client
or another — between `WATCH` and `EXEC`, `EXEC` aborts and returns a null array,
the basis for lock-free check-and-set patterns.

## Eviction

```bash
go run ./cmd/shardkv -maxkeys 100000
```

With `-maxkeys` set, a background pass keeps the store near the cap by evicting
approximately-least-recently-used keys: it samples up to 16 keys from a random
shard and removes the oldest, repeating until under the cap. Access time is
tracked only when eviction is enabled, so the default (unbounded) configuration
pays nothing on the read path. `INFO` reports `evicted_keys`.

## Design notes

- **Why sharding.** One global mutex makes the lock the bottleneck: every
  operation, even on unrelated keys, waits in line. Hashing keys to independent
  shards turns that into per-shard contention that scales with cores.
- **Skip list for sorted sets.** A `member → score` map gives O(1) score lookup;
  a skip list ordered by `(score, member)`, with a span recorded at every level,
  gives O(log n) `ZRANK`/`ZRANGE`. Cross-checked against a brute-force sorted
  slice over thousands of randomized operations.
- **Command table + `write` flag.** Commands self-register with an arity and a
  `write` flag. Dispatch checks arity, rejects writes on replicas, and — only
  when a write actually modified state — propagates the verbatim command to the
  AOF and replicas. Durability and replication are therefore a property of the
  table, not scattered through handlers.
- **Optimistic locking, not a global lock.** `WATCH` registers a key→sessions
  map; any write that modifies an affected key marks watching sessions dirty
  (via an atomic flag), so `EXEC` can detect a conflict without serializing the
  whole keyspace.
- **Pointer entries for LRU.** Values are stored as `*entry` so a key's access
  time can be updated atomically on the read path, under a shared read lock,
  without upgrading to an exclusive lock. The cost is one allocation per write;
  a `sync.Pool` or arena would remove it if write throughput dominated.
- **Value copies.** Stored bytes are copied in and out so callers can never
  mutate state through an aliased slice.
- **Injectable clock.** The store reads time from a function field, so TTL logic
  is tested deterministically instead of with sleeps.

## Benchmarks

Apple Silicon, `go test -bench`, 8 logical cores:

| Operation         | ns/op | allocs/op | throughput     |
| ----------------- | ----- | --------- | -------------- |
| `GET` (parallel)  | ~55   | 1         | ~18 M ops/sec  |
| `SET`             | ~250  | 3         | ~4 M ops/sec   |
| `INCR` (parallel) | ~180  | 3         | ~5 M ops/sec   |

```bash
go test -bench=. -benchmem ./internal/store
```

The write-path allocation is the `*entry` that enables in-place LRU bookkeeping
(see design notes); for a networked store the RESP/TCP path dominates these
in-memory costs in practice.

## Testing

```bash
go test -race ./...                                   # unit + integration, race on
go test -fuzz=FuzzReadCommand -fuzztime=30s ./internal/resp   # fuzz the protocol parser
go vet ./...
```

The suite includes a master/replica convergence test, an AOF round-trip and
rewrite test, a skip-list fuzz-against-sort cross-check, and concurrency tests
that drive shared keys from many goroutines and TCP clients.

## Layout

```text
cmd/shardkv        entrypoint: flags, signal handling, AOF/replication wiring
internal/store     sharded store, typed values, skip-list sorted set, LRU eviction, snapshot   (+ tests, benchmarks)
internal/resp      RESP protocol reader/writer                                                  (+ tests, fuzz)
internal/server    TCP server, command table + handlers, transactions, replication             (+ tests)
internal/aof       append-only-file persistence: append, fsync policy, replay                   (+ tests)
```

## Roadmap

- Replication offsets + partial resync for exact initial sync
- `sync.Pool` for entries to remove the write-path allocation
- Pipelining throughput tuning; RESP3
- Keyspace notifications

## License

MIT — see [LICENSE](LICENSE).
