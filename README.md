# shardkv

A concurrent, sharded in-memory key–value store written in Go, wire-compatible
with the Redis protocol (RESP) — so any Redis client, including `redis-cli`,
talks to it unchanged.

`shardkv` is a focused study in **concurrency, synchronization, and network
server design**: it partitions the keyspace across many independently-locked
shards so thousands of clients can read and write in parallel without
serializing behind a single global lock.

```text
                        ┌─────────────────────────────────────────┐
   redis-cli ──TCP──▶   │  server: one goroutine per connection    │
   client lib ──TCP──▶  │  RESP parse ─▶ dispatch ─▶ RESP reply     │
                        └───────────────────┬─────────────────────┘
                                            │ concurrent calls
                        ┌───────────────────▼─────────────────────┐
                        │  store: N shards (power of two)          │
                        │  key ─ FNV-1a hash ─ (hash & mask) ─▶    │
                        │   ┌──────┐ ┌──────┐ ┌──────┐   ┌──────┐  │
                        │   │shard0│ │shard1│ │shard2│ … │shardN│  │
                        │   │RWMutex│ │RWMutex│ │RWMutex│  │RWMutex│ │
                        │   │ map  │ │ map  │ │ map  │   │ map  │  │
                        │   └──────┘ └──────┘ └──────┘   └──────┘  │
                        └──────────────────────────────────────────┘
```

## Why sharding

A naive store puts every key behind one `sync.Mutex`. Under load that lock
becomes the bottleneck — every operation, even on unrelated keys, waits in line.

`shardkv` hashes each key (allocation-free FNV-1a) to one of `N` shards, each
with its own `sync.RWMutex` and map. Two operations on different keys almost
always touch different shards and proceed in true parallel; reads on the same
shard share an `RLock`. The shard count is rounded up to a power of two so shard
selection is a single `hash & mask` instead of a modulo.

## Features

- **RESP wire protocol** — works with `redis-cli` and standard Redis client
  libraries; also accepts inline commands for quick socket testing.
- **Sharded locking** for concurrent throughput that scales across cores.
- **TTL expiration** — lazy (on read) plus a background janitor that reclaims
  memory from keys that are written and never read again.
- **Atomic `INCR`/`DECR`** via per-shard read-modify-write under lock.
- **Graceful shutdown** — `context`-driven; in-flight connections drain before
  exit.
- **Race-clean** — the full suite passes under `go test -race`, including tests
  that hammer a shared counter from dozens of goroutines and TCP clients.

## Commands

`PING` · `SET key value [EX s | PX ms]` · `GET` · `DEL` · `EXISTS` ·
`INCR` · `DECR` · `EXPIRE` · `TTL` · `KEYS` · `DBSIZE` · `FLUSHALL`

## Quick start

```bash
go run ./cmd/shardkv -addr :6380 -shards 256

# in another terminal (real redis-cli works):
redis-cli -p 6380 set greeting "hello"
redis-cli -p 6380 get greeting
redis-cli -p 6380 incr visits

# or with no Redis installed, a raw socket:
printf 'SET foo bar\r\nGET foo\r\n' | nc 127.0.0.1 6380
```

Flags: `-addr` (listen address), `-shards` (lock shards, rounded to a power of
two), `-sweep` (active-expiration interval).

## Design notes

- **Value copies.** `Set` copies the incoming value and `Get` returns a copy, so
  a caller can never mutate stored bytes through an aliased slice. The cost is
  one allocation per call; correctness wins on a shared store.
- **Lazy + active expiration.** Reads treat expired keys as absent and delete
  them opportunistically; the janitor sweeps the rest on an interval, bounding
  memory without putting expiry checks on a timer per key.
- **Injectable clock.** The store takes its time from a function field, so TTL
  logic is tested deterministically instead of with `time.Sleep`.

## Benchmarks

Apple Silicon, `go test -bench`, 8 logical cores:

| Operation          | ns/op | allocs/op | throughput      |
| ------------------ | ----- | --------- | --------------- |
| `GET` (parallel)   | ~31   | 1         | ~32 M ops/sec   |
| `SET`              | ~76   | 2         | ~13 M ops/sec   |
| `INCR` (parallel)  | ~90   | 2         | ~11 M ops/sec   |

```bash
go test -bench=. -benchmem ./internal/store
```

## Testing

```bash
go test -race ./...        # unit + integration, race detector on
go vet ./...
```

## Layout

```text
cmd/shardkv        entrypoint: flags, signal handling, wiring
internal/store     sharded store: locking, TTL, INCR, janitor   (+ tests, benchmarks)
internal/resp      RESP protocol reader/writer                  (+ tests)
internal/server    TCP server, command dispatch                 (+ integration tests)
```

## Roadmap

- Approximate-LRU eviction under a `maxmemory` cap
- `MGET`/`MSET`, hashes and lists
- RESP3 and pipelining throughput tuning
- Optional append-only-file persistence

## License

MIT — see [LICENSE](LICENSE).
