# shardkv

[![CI](https://github.com/Black-third/shardkv/actions/workflows/ci.yml/badge.svg)](https://github.com/Black-third/shardkv/actions/workflows/ci.yml)

A concurrent, sharded, in-memory data-structure server written in Go — a compact
Redis: six data types (including **streams with consumer groups**) plus bitmaps,
HyperLogLog and geospatial indexes, multiple databases, blocking queue commands,
append-only-file persistence, primary–replica replication with partial resync,
Pub/Sub, keyspace notifications, authentication and TLS, and the full observability
surface (`SLOWLOG`, `MONITOR`, `LATENCY`, `INFO commandstats`) — all wire-compatible
with the Redis protocol in **both RESP2 and RESP3**, so `redis-cli` and any Redis
client library work against it unchanged.

`shardkv` is a study in the systems fundamentals that matter most: **concurrency
and synchronization, network server design, data structures, and durability.**
It partitions the keyspace across many independently-locked shards so thousands
of clients can read and write in parallel without serializing behind one global
lock, and the whole suite — including tests that hammer shared state from dozens
of goroutines and TCP clients — passes under the Go race detector.

```text
                        ┌─────────────────────────────────────────────────┐
   redis-cli ─TCP/TLS▶  │  server: one goroutine per connection            │
   client lib ─TCP/TLS▶ │  AUTH ─▶ RESP2/3 parse ─▶ command table ─▶ reply │
   replica ───PSYNC─▶   │  write commands ─▶ SELECT? ─▶ AOF + repl stream  │
   subscriber ◀─push──  │  PUBLISH / keyspace events ─▶ subscriber pump    │
   BLPOP ◀─── wake ───  │  blocked clients ◀─ per-key FIFO wait queues     │
                        └──────────────────────────┬──────────────────────┘
                                    SELECT n       │ concurrent calls
                        ┌──────────────────────────▼──────────────────────┐
                        │  database 0 … database N-1, each independent    │
                        │  store: N shards (power of two)                 │
                        │  key ─ FNV-1a ─ (hash & mask) ─▶ shard          │
                        │   ┌────────┐ ┌────────┐      ┌────────┐         │
                        │   │RWMutex │ │RWMutex │  …   │RWMutex │         │
                        │   │ string │ │ list   │      │ zset   │         │
                        │   │ hash   │ │ set    │      │ (skip  │         │
                        │   │ stream │ │ …      │      │  list) │         │
                        │   └────────┘ └────────┘      └────────┘         │
                        └────────────────────────────────────────────────-┘

   strings also carry bitmaps (SETBIT/BITCOUNT/BITFIELD) and HyperLogLog sketches
   in Redis's own HYLL format; sorted sets also carry geospatial indexes, whose
   score is a 52-bit geohash — exactly as in Redis, so every string command works
   on a bitmap and every sorted-set command works on a geo set.
```

## Features

- **Six data types** — strings, lists, hashes, sets, **sorted sets backed by a
  hand-written skip list** with O(log n) insertion, deletion and rank queries, and
  **streams** with consumer groups. 224 commands, including both set algebras
  (`SINTER`/`SUNION`/`SDIFF` and `ZUNION`/`ZINTER`/`ZDIFF` with their `*STORE` forms,
  `WEIGHTS` and `AGGREGATE`), score, rank *and* lexicographic range queries
  (`ZRANGEBYLEX`, `ZRANGE ... BYSCORE|BYLEX|REV|LIMIT`, `ZRANGESTORE`), `SORT` with its
  `BY`/`GET` patterns, `LCS`, the positional list operations, and the cursor iterators for
  every collection. See [Compatibility](#compatibility-what-is-missing) for what a client
  may still expect and not find.
- **Cluster mode** — CRC16-XMODEM hash slots with hash tags, `MOVED`/`ASK`/
  `CROSSSLOT`/`TRYAGAIN`/`CLUSTERDOWN` redirects, the `CLUSTER` administration
  surface (`NODES`/`SLOTS`/`SHARDS` in the exact formats clients parse), and live
  slot migration via `DUMP`/`RESTORE`/`MIGRATE`. `redis-cli -c` works against it.
  The **client-facing half of Redis Cluster is implemented; the binary gossip bus
  is deliberately not** — see [Cluster](#cluster) for the precise boundary.
- **Streams with consumer groups** — `XADD` (generated or explicit ids,
  `NOMKSTREAM`, `MAXLEN`/`MINID` trimming), `XRANGE`/`XREVRANGE` with exclusive
  bounds, `XREAD` with `BLOCK` and `$`, and the full group surface: `XGROUP`,
  `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`, `XINFO`. Ids are
  `ms-seq` pairs that stay monotonic even when the clock steps *backwards*, and the
  three commands whose outcome depends on the clock — `XADD *`, `XCLAIM`,
  `XAUTOCLAIM` — propagate the concrete ids they assigned rather than their own
  text, because a replica generating its own would diverge with nothing to signal
  it. A snapshot reconstructs the groups, their last-delivered ids *and* their
  pending-entry lists, so in-flight work survives a restart. See
  [Streams](#streams).
- **Bitmaps** — `SETBIT` `GETBIT` `BITCOUNT` (with `BYTE`/`BIT` ranges) `BITPOS`
  `BITOP AND|OR|XOR|NOT` `BITFIELD` (`u`/`i` types up to 64 bits, `#`-relative
  offsets, `OVERFLOW WRAP|SAT|FAIL`) and `BITFIELD_RO`. A bitmap is a string, as in
  Redis, so `STRLEN` measures it, `APPEND` extends it and `GETRANGE` reads it.
- **HyperLogLog** — `PFADD` `PFCOUNT` `PFMERGE`, plus `PFDEBUG` and `PFSELFTEST`.
  The real algorithm: 16384 six-bit registers, both the sparse and the dense
  encoding, MurmurHash64A with Redis's seed, and Ertl's tau/sigma estimator. The
  stored string is **byte-for-byte a Redis HLL**, verified by round-tripping
  sketches through a real `redis:7-alpine` in both directions. Measured worst-case
  error over 1k–200k distinct elements: **1.42 %**. See
  [HyperLogLog](#hyperloglog).
- **Geospatial indexes** — `GEOADD` `GEOPOS` `GEODIST` `GEOHASH` `GEOSEARCH`
  (`FROMMEMBER`/`FROMLONLAT`, `BYRADIUS`/`BYBOX`, `ASC`/`DESC`, `COUNT [ANY]`,
  `WITHCOORD`/`WITHDIST`/`WITHHASH`) and `GEOSEARCHSTORE`. Built on the sorted set
  with a 52-bit geohash as the score, as Redis does, so a geo set *is* a sorted set:
  `ZCARD` counts it, `ZREM` edits it, `ZSCORE` shows the raw hash. A search
  resolves its area to nine geohash cells and filters the candidates by real
  distance, so the answer is exact rather than rectangular. See
  [Geospatial](#geospatial).
- **RESP2 and RESP3** — `HELLO 3` negotiates the newer protocol per connection, with
  the map, set, double, boolean, big-number, verbatim-string, null, push and
  attribute types. The reply *shapes* change where Redis changes them: `HGETALL` and
  `CONFIG GET` become maps, `SMEMBERS` and `SPOP key count` sets, `ZSCORE` a double,
  `WITHSCORES` ranges a list of pairs instead of a flattened one, `INFO` a verbatim
  string. Above all, Pub/Sub messages become out-of-band **pushes**, so a RESP3
  client can stay subscribed *and* keep issuing ordinary commands — the restriction
  RESP2 subscribers live under exists only because RESP2 has no push frame. A RESP2
  connection receives byte-identical replies to the ones it always did, which is
  pinned by a raw-byte test.
- **Blocking commands** — `BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` `BZPOPMIN`
  `BZPOPMAX` `BLMPOP` `BZMPOP`, with fractional-second timeouts (0 = forever). A
  blocked client holds no lock of any kind and is woken by an exact signal from the
  write path rather than by polling; when several clients wait on one key the
  earliest is served first, because only the head of a key's queue is ever woken.
  What propagates is the pop that happened, never the blocking command. See
  [Blocking commands](#blocking-commands).
- **Multiple databases** — `SELECT`, `SWAPDB`, `MOVE`, `COPY ... DB n`, `FLUSHDB`
  against `FLUSHALL`, per-database `DBSIZE`, and one `INFO keyspace` line per
  database. Each database is an independent sharded keyspace, so a `SELECT 1`
  workload never contends on database 0's locks. The AOF and the replica stream
  carry the database as a lazily emitted `SELECT`, exactly as Redis does. See
  [Databases](#databases).
- **Sharded locking** — the keyspace is split across `N` power-of-two shards,
  each with its own `RWMutex`, so operations on different keys run in true
  parallel. Shard selection is an allocation-free FNV-1a hash plus a bitmask.
- **AOF persistence with compacting rewrite** — every write is appended to a log
  (configurable fsync policy: `always` / `everysec` / `no`) and replayed on
  startup. `BGREWRITEAOF` and an automatic growth policy
  (`-aof-rewrite-percentage` against the size after the last rewrite, floored by
  `-aof-rewrite-min-size`) replace the history with a snapshot of the present, so
  a key written a million times stops costing a million records to replay.
- **Point-in-time snapshots** — `SAVE`, `BGSAVE`, `LASTSAVE`, the `save <seconds>
  <changes>` schedule and `DEBUG RELOAD` write one framed, length-prefixed,
  CRC-64-checksummed file that is complete as of a single instant across every database.
  Written to a temporary file and renamed, so a crash cannot replace a good snapshot with
  a truncated one. **Not an RDB file, and the header says so** — see
  [Persistence (snapshots)](#persistence-snapshots).
- **Primary–replica replication with partial resync** — a replica issues
  `PSYNC <replid> <offset>`; a master that still holds that offset in its bounded
  backlog answers `+CONTINUE` and streams only the bytes the replica missed,
  otherwise `+FULLRESYNC` with a consistent point-in-time snapshot. Replicas are
  read-only and can be chained, report their progress with `REPLCONF ACK`, and
  `WAIT numreplicas timeout` reports how many copies hold a given write. A
  keepalive on each feed plus a read deadline on the replica means a master that
  vanishes behind a partition (TCP connection open, no bytes) is detected and
  resynced from, rather than leaving the replica stale forever.
- **Authentication and TLS** — `-requirepass` with `AUTH [username] password`
  (constant-time comparison; unauthenticated connections may only `AUTH`, `HELLO`,
  `PING`, `QUIT` and `RESET`, and `PSYNC` is gated too, so a snapshot of the whole
  dataset is not a way around the password). A replica presents `-masterauth` on
  every connection, including every reconnect. Optional TLS on the listener
  (`-tls-cert`/`-tls-key`/`-tls-ca`) and, independently, on the connection a
  replica opens to its master (`-tls-replication`); plain TCP remains the default.
- **Pub/Sub** — `SUBSCRIBE`/`PSUBSCRIBE` with glob patterns, `PUBLISH`, and
  `PUBSUB CHANNELS|NUMSUB|NUMPAT`. A RESP2 subscriber enters subscriber mode; a RESP3
  one does not, because its messages arrive as push frames. A publisher is never
  blocked by a slow subscriber: each subscriber has a bounded queue and one that
  overflows is disconnected, the same reasoning the replica feeds use. `PUBLISH` on a
  master reaches subscribers on its replicas.
- **Keyspace notifications** — `-notify-keyspace-events` with Redis's flag
  characters (`K E g $ l s h z x e A`), publishing `__keyspace@<db>__:<key>` and
  `__keyevent@<db>__:<event>` on writes, expirations and evictions. Disabled it costs
  one atomic load and zero allocations on the write path.
- **Consistent durability** — when persistence/replication is active, writes are
  totally ordered, so the memory state, the AOF, and every replica stream share
  one order (no divergence under concurrent writes); the initial sync is exact
  (no double-apply); and a `MULTI`/`EXEC` transaction is propagated wrapped in
  `MULTI`/`EXEC`, so a crash that truncates the AOF mid-transaction replays none
  of it. Relative TTLs are rewritten to absolute deadlines (so a replica or AOF
  replay reconstructs the same expiry instant) and evictions propagate as `DEL`.
  A replica that falls too far behind to buffer is disconnected so it resyncs —
  from the backlog if its offset is still retained, from a fresh snapshot if not.
  A write is never quietly skipped for one copy. An AOF rewrite takes its snapshot
  and swaps the file under the same ordering lock, so the compacted log is exactly
  the dataset at one instant followed by every write ordered after it.
- **Effect propagation for non-deterministic writes** — `SPOP` and `ZPOPMIN`/
  `ZPOPMAX` choose members by chance or by iteration order, so the command text
  itself is not replayable: a replica running it would remove *different* members
  and diverge with nothing to signal it. Each ships the effect it actually had
  (`SREM`/`ZREM` of exactly the members removed) instead, and the float increments
  ship their result (`SET ... KEEPTTL`, `HSET`) rather than the addition, which is
  what real Redis does for the same reason.
- **Transactions** — `MULTI`/`EXEC`/`DISCARD` command batching, with
  `WATCH`/`UNWATCH` optimistic locking: `EXEC` aborts if a watched key was
  modified — or expired — between `WATCH` and `EXEC`.
- **Cursor iteration & pipelining** — `SCAN` walks the keyspace incrementally
  (with `MATCH`/`COUNT`) instead of the O(n) `KEYS`, and `HSCAN`/`SSCAN`/`ZSCAN`
  offer the same contract over one collection; all filter with the same glob
  matcher (`*`, `?`); pipelined requests are served with a single coalesced flush.
- **Byte-bounded eviction** — `maxmemory` with all eight of Redis's policies
  (`noeviction`, `allkeys-lru`/`-lfu`/`-random`, `volatile-lru`/`-lfu`/`-random`/`-ttl`).
  `used_memory` is a running total maintained as values are written, so the budget is
  compared against the dataset rather than against the Go heap, and a policy that cannot
  evict refuses the writes that would grow the keyspace with Redis's own `OOM` error while
  reads and deletions keep working. See [Memory and eviction](#memory-and-eviction).
- **Approximate-LRU eviction** — an optional `maxkeys` cap; a background pass
  samples keys and evicts the least-recently-used, Redis-style.
- **TTL expiration** — lazy on read plus a background janitor that reclaims
  memory.
- **Hardened** — the RESP parser bounds the multibulk count and bulk length
  before allocating (a crafted header can't overflow or OOM the server);
  `INCR`/`DECR` reject int64 overflow, `INCRBYFLOAT`/`HINCRBYFLOAT` reject an
  operand or a result that is not finite (an `inf` no later increment could
  recover from), and the expire family rejects an operand whose deadline
  arithmetic would overflow instead of wrapping it into the past
  (which would delete the key it was meant to keep); every timestamp and counter
  is parsed at full int64 width regardless of build word size; `SETRANGE` refuses
  an offset that would grow a value past the largest string the protocol can carry
  *before* allocating it; snapshots are chunked so no emitted command can exceed
  the protocol's multibulk limit; option lists (`SCAN`, `SET`, `ZADD`, the expire
  flags) reject malformed and incompatible input rather than letting the last one
  win; AOF rewrites survive a failed swap and surface write/fsync errors instead
  of silently losing durability; passwords are compared in constant time and never
  logged; a replica always verifies its master's TLS certificate; and the
  per-client growth paths — a replica feed, a subscriber queue, a monitor feed, the
  replication backlog, the AOF itself — are each bounded, with the overflow answered
  by a visible disconnect or a compaction rather than by unbounded memory.

  The connection table is bounded too. `-maxclients` (default 10000, as in Redis) is
  checked *before* a goroutine is spent on an accepted connection, and a refused client is
  told `-ERR max number of clients reached` and then hung up on rather than left to report
  "connection reset". An optional `-timeout` closes idle connections, and it exempts the
  four kinds that are silent by design — a replica feed, a Pub/Sub subscriber, a `MONITOR`
  feed and a client parked in `BLPOP` — because reaping those would break exactly the
  clients that are working correctly. `INFO clients` reports `connected_clients`,
  `maxclients` and `rejected_connections`.
- **Self-registering command table** — each command declares an arity and either
  a `write` flag, an effect handler, or a session handler for the connection-control
  commands. The `write` flag is what automatically drives both AOF persistence and
  replica propagation, so new mutating commands wire themselves into durability and
  replication; the table is also what `COMMAND COUNT`/`INFO`/`DOCS` report, so a
  client library sees exactly what the server can run.
- **Client and server introspection** — `HELLO [2|3]` negotiating the protocol,
  `CONFIG GET|SET|RESETSTAT` glob-matched over the settings that exist,
  `CLIENT ID|GETNAME|SETNAME|LIST|INFO|KILL|UNBLOCK` — the `CLIENT INFO`/`CLIENT LIST` line
  carries Redis 7.4's full field set in Redis's order, including `laddr`, `fd`, `watch`,
  `resp`, `tot-mem` and a `flags` field that is a *set* of letters (`x` in a transaction,
  `b` blocked, `d` after a watched key changed, `P` subscribed, `S` a replica feed, `O` a
  monitor), because tools parse that line and some of them parse it positionally —
  `COMMAND COUNT|INFO|DOCS`,
  `DEBUG PROTOCOL|SLEEP|OBJECT|RELOAD|CHANGE-REPL-ID`, `RESET`, `SELECT`,
  `SAVE`, `BGSAVE [SCHEDULE]`, `LASTSAVE`,
  `SHUTDOWN [NOSAVE]`, and `INFO <section>` filtering.
- **Observability** — `INFO` in eight sections: server, clients
  (`blocked_clients`, `total_blocking_keys`), persistence
  (`aof_rewrite_in_progress`, `aof_base_size`, `aof_current_size`,
  `aof_last_bgrewrite_status`, `rdb_last_save_time`,
  `rdb_changes_since_last_save`), stats (commands, `keyspace_hits`/`keyspace_misses`,
  evictions, `sync_full`, `sync_partial_ok`, `sync_partial_err`, `replica_drops`,
  Pub/Sub and MONITOR drop counters), replication (`master_replid`,
  `master_repl_offset`, `slave_repl_offset`, per-replica acknowledged offsets,
  backlog size and history length), **`commandstats`** and **`latencystats`** (both
  excluded from a bare `INFO`, as in Redis) and keyspace (one line per non-empty
  database). Plus **`SLOWLOG GET|LEN|RESET`** with `-slowlog-log-slower-than` and
  `-slowlog-max-len`, **`MONITOR`**, **`LATENCY HISTORY|LATEST|RESET`** with
  `-latency-monitor-threshold`, **`MEMORY USAGE`**/**`MEMORY STATS`**/**`MEMORY DOCTOR`**, and
  **`COMMAND GETKEYS`** for the commands whose keys are not at a fixed position.
  The whole surface costs nothing when unused — per-command statistics are three
  atomic adds on the table entry, and the slow log, latency monitor and MONITOR feed
  are each one atomic load — which is pinned by an `AllocsPerRun` test. See
  [Observability](#observability).
- **Tested hard** — unit + integration tests under `-race` (master/replica
  convergence, partial resync, expiry-propagation, transactions, AOF
  crash/rewrite-under-load, auth, TLS with a certificate generated in the test,
  Pub/Sub fan-out and slow-subscriber handling, RESP2/RESP3 byte-for-byte reply
  encodings, blocking-command fairness and goroutine-leak checks with real
  concurrent TCP clients, cross-database AOF replay and replica convergence), plus a
  Go **fuzz test** for the RESP parser.

## Commands

224 commands, with Redis's replies, error strings, and edge-case behaviour
(missing keys, empty collections, negative indexes, wrong-type errors). Every
reply is available in both RESP2 and RESP3; where the two differ, the RESP3 shape
is the one real Redis 7 sends (see [Protocol](#protocol-resp2-and-resp3)).

| Group   | Commands |
| ------- | -------- |
| Strings | `SET key val [NX\|XX] [GET] [KEEPTTL] [EX s\|PX ms\|EXAT ts\|PXAT ts]` · `GET` · `GETEX key [EX s\|PX ms\|EXAT ts\|PXAT ts\|PERSIST]` · `GETSET` · `GETDEL` · `SETNX` · `SETEX` · `PSETEX` · `APPEND` · `STRLEN` · `SETRANGE` · `GETRANGE`/`SUBSTR` · `INCR` · `DECR` · `INCRBY` · `DECRBY` · `INCRBYFLOAT` · `MSET` · `MGET` · `LCS key1 key2 [LEN\|IDX] [MINMATCHLEN n] [WITHMATCHLEN]` |
| Keys    | `DEL` · `UNLINK` · `EXISTS` · `TOUCH` · `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT key n [NX\|XX\|GT\|LT]` · `PERSIST` · `TTL` · `PTTL` · `TYPE` · `RENAME` · `RENAMENX` · `COPY key dst [DB n] [REPLACE]` · `RANDOMKEY` · `OBJECT ENCODING\|REFCOUNT\|IDLETIME` · `KEYS pattern` · `SCAN cursor [MATCH p] [COUNT n] [TYPE t]` · `EXPIRETIME` · `PEXPIRETIME` · `SORT`/`SORT_RO key [BY pat] [LIMIT off n] [GET pat...] [ASC\|DESC] [ALPHA] [STORE dst]` |
| Lists   | `LPUSH` · `RPUSH` · `LPUSHX` · `RPUSHX` · `LPOP key [count]` · `RPOP key [count]` · `LRANGE` · `LLEN` · `LINDEX` · `LSET` · `LINSERT key BEFORE\|AFTER pivot v` · `LREM` · `LTRIM` · `LPOS key v [RANK n] [COUNT n] [MAXLEN n]` · `RPOPLPUSH` · `LMOVE src dst LEFT\|RIGHT LEFT\|RIGHT` · `LMPOP numkeys key... LEFT\|RIGHT [COUNT n]` |
| Blocking | `BLPOP key... timeout` · `BRPOP key... timeout` · `BLMOVE src dst LEFT\|RIGHT LEFT\|RIGHT timeout` · `BRPOPLPUSH src dst timeout` · `BZPOPMIN key... timeout` · `BZPOPMAX key... timeout` · `BLMPOP timeout numkeys key... LEFT\|RIGHT [COUNT n]` · `BZMPOP timeout numkeys key... MIN\|MAX [COUNT n]` |
| Hashes  | `HSET` · `HMSET` · `HGET` · `HDEL` · `HGETALL` · `HLEN` · `HKEYS` · `HVALS` · `HEXISTS` · `HMGET` · `HSTRLEN` · `HSETNX` · `HINCRBY` · `HINCRBYFLOAT` · `HRANDFIELD key [count [WITHVALUES]]` · `HSCAN key cursor [MATCH p] [COUNT n] [NOVALUES]` |
| Sets    | `SADD` · `SREM` · `SMEMBERS` · `SISMEMBER` · `SMISMEMBER` · `SCARD` · `SPOP key [count]` · `SRANDMEMBER key [count]` · `SMOVE` · `SINTER` · `SUNION` · `SDIFF` · `SINTERSTORE` · `SUNIONSTORE` · `SDIFFSTORE` · `SINTERCARD numkeys key... [LIMIT n]` · `SSCAN` |
| Sorted  | `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member...` · `ZINCRBY` · `ZSCORE` · `ZMSCORE` · `ZREM key member...` · `ZCARD` · `ZCOUNT` · `ZLEXCOUNT` · `ZRANK key member [WITHSCORE]` · `ZREVRANK` · `ZRANGE key start stop [BYSCORE\|BYLEX] [REV] [LIMIT off n] [WITHSCORES]` · `ZREVRANGE` · `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT off n]` · `ZREVRANGEBYSCORE` · `ZRANGEBYLEX key min max [LIMIT off n]` · `ZREVRANGEBYLEX` · `ZRANGESTORE dst src min max [BYSCORE\|BYLEX] [REV] [LIMIT off n]` · `ZREMRANGEBYRANK` · `ZREMRANGEBYSCORE` · `ZREMRANGEBYLEX` · `ZRANDMEMBER key [count [WITHSCORES]]` · `ZPOPMIN key [count]` · `ZPOPMAX key [count]` · `ZMPOP numkeys key... MIN\|MAX [COUNT n]` · `ZSCAN` |
| Sorted algebra | `ZUNIONSTORE dst numkeys key... [WEIGHTS w...] [AGGREGATE SUM\|MIN\|MAX]` · `ZINTERSTORE` · `ZDIFFSTORE dst numkeys key...` · `ZUNION numkeys key... [WEIGHTS w...] [AGGREGATE ...] [WITHSCORES]` · `ZINTER` · `ZDIFF numkeys key... [WITHSCORES]` · `ZINTERCARD numkeys key... [LIMIT n]` |
| Streams | `XADD key [NOMKSTREAM] [MAXLEN\|MINID [=\|~] n [LIMIT c]] <*\|ms-*\|id> field val...` · `XLEN` · `XRANGE key start end [COUNT n]` · `XREVRANGE` · `XDEL key id...` · `XTRIM key MAXLEN\|MINID [=\|~] n [LIMIT c]` · `XSETID key id [ENTRIESADDED n] [MAXDELETEDID id]` · `XREAD [COUNT n] [BLOCK ms] STREAMS key... id...` |
| Stream groups | `XGROUP CREATE key g <id\|$> [MKSTREAM] [ENTRIESREAD n]` · `XGROUP SETID\|DESTROY\|CREATECONSUMER\|DELCONSUMER` · `XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS key... <id\|>>...` · `XACK key g id...` · `XPENDING key g [[IDLE ms] start end count [consumer]]` · `XCLAIM key g c min-idle id... [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]` · `XAUTOCLAIM key g c min-idle start [COUNT n] [JUSTID]` · `XINFO STREAM\|GROUPS\|CONSUMERS` |
| Bitmaps | `SETBIT key offset 0\|1` · `GETBIT key offset` · `BITCOUNT key [start end [BYTE\|BIT]]` · `BITPOS key 0\|1 [start [end [BYTE\|BIT]]]` · `BITOP AND\|OR\|XOR\|NOT dst src...` · `BITFIELD key [GET type off] [SET type off v] [INCRBY type off n] [OVERFLOW WRAP\|SAT\|FAIL]...` · `BITFIELD_RO` |
| HyperLogLog | `PFADD key [element...]` · `PFCOUNT key [key...]` · `PFMERGE dst [src...]` · `PFDEBUG GETREG\|ENCODING\|TODENSE key` · `PFSELFTEST` |
| Geo     | `GEOADD key [NX\|XX] [CH] lon lat member...` · `GEOPOS key [member...]` · `GEODIST key m1 m2 [m\|km\|ft\|mi]` · `GEOHASH key [member...]` · `GEOSEARCH key <FROMMEMBER m\|FROMLONLAT lon lat> <BYRADIUS r unit\|BYBOX w h unit> [ASC\|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` · `GEOSEARCHSTORE dst src ... [STOREDIST]` · `GEORADIUS key lon lat radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n [ANY]] [ASC\|DESC] [STORE dst] [STOREDIST dst]` · `GEORADIUSBYMEMBER key member radius unit ...` · `GEORADIUS_RO` · `GEORADIUSBYMEMBER_RO` |
| Tx      | `MULTI` · `EXEC` · `DISCARD` · `WATCH` · `UNWATCH` |
| Pub/Sub | `SUBSCRIBE` · `UNSUBSCRIBE` · `PSUBSCRIBE` · `PUNSUBSCRIBE` · `PUBLISH` · `PUBSUB CHANNELS [pattern]\|NUMSUB [ch...]\|NUMPAT` |
| Connection | `AUTH [username] password` · `HELLO [2\|3 [AUTH u p] [SETNAME n]]` · `PING` · `SELECT index` · `RESET` · `QUIT` · `CLIENT ID\|GETNAME\|SETNAME\|SETINFO\|LIST\|INFO\|KILL\|UNBLOCK id [TIMEOUT\|ERROR]\|REPLY ON\|OFF\|SKIP` |
| Databases | `SELECT index` · `SWAPDB index1 index2` · `MOVE key db` · `COPY key dst [DB n] [REPLACE]` · `FLUSHDB` · `FLUSHALL` · `DBSIZE` |
| Server  | `INFO [section...]` · `DBSIZE` · `FLUSHDB` · `FLUSHALL` · `SWAPDB` · `MOVE` · `CONFIG GET\|SET\|RESETSTAT` · `COMMAND [COUNT\|INFO\|DOCS\|GETKEYS\|HELP]` · `DEBUG PROTOCOL\|SLEEP\|OBJECT\|RELOAD\|CHANGE-REPL-ID\|HELP` · `BGREWRITEAOF` · `SAVE` · `BGSAVE [SCHEDULE]` · `LASTSAVE` · `SHUTDOWN [NOSAVE]` |
| Observability | `SLOWLOG GET [count]\|LEN\|RESET\|HELP` · `MONITOR` · `LATENCY HISTORY event\|LATEST\|HISTOGRAM [cmd...]\|RESET [event...]\|HELP` · `MEMORY USAGE key [SAMPLES n]\|STATS\|DOCTOR\|HELP` · `COMMAND GETKEYS cmd args...` · `INFO commandstats` · `INFO latencystats` |
| Replication | `REPLICAOF host port\|NO ONE` · `SLAVEOF` · `ROLE` · `PSYNC replid offset` · `REPLCONF listening-port\|ACK` · `WAIT numreplicas timeout` |
| Cluster | `CLUSTER INFO\|MYID\|SLOTS\|SHARDS\|NODES\|KEYSLOT key` · `CLUSTER ADDSLOTS slot...\|DELSLOTS\|ADDSLOTSRANGE start end...\|DELSLOTSRANGE` · `CLUSTER SETSLOT slot IMPORTING\|MIGRATING\|STABLE\|NODE id` · `CLUSTER COUNTKEYSINSLOT slot\|GETKEYSINSLOT slot count` · `CLUSTER MEET ip port [bus-port]\|FORGET id\|REPLICATE id\|RESET [HARD\|SOFT]` · `ASKING` · `READONLY` · `READWRITE` |
| Migration | `DUMP key` · `RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]` · `MIGRATE host port key\|"" db timeout [COPY] [REPLACE] [AUTH pw\|AUTH2 user pw] [KEYS key...]` |

## Install

Five ways in. Pick by what you already have: a Go toolchain, `brew`, Docker, or nothing
but `curl`.

**From source, with Go.** The module has no third-party dependencies, so this reaches the
network for the toolchain's module proxy and nothing else — there is no dependency tree to
audit:

```bash
go install github.com/Black-third/shardkv/cmd/shardkv@latest
shardkv -addr :6380
```

**A release binary.** Cross-compiled for linux and darwin on amd64 and arm64, with a
`checksums.txt` covering every archive. Verify it — the whole point of publishing that file
is that somebody checks it:

```bash
VERSION=0.3.0 OS=darwin ARCH=arm64
BASE="https://github.com/Black-third/shardkv/releases/download/v$VERSION"
curl -fsSLO "$BASE/shardkv_${VERSION}_${OS}_${ARCH}.tar.gz"
curl -fsSLO "$BASE/checksums.txt"
shasum -a 256 --check --ignore-missing checksums.txt   # sha256sum -c on linux
tar xzf "shardkv_${VERSION}_${OS}_${ARCH}.tar.gz"
./shardkv -addr :6380
```

**Homebrew**, from the tap:

```bash
brew tap black-third/tap
brew install shardkv
brew install --HEAD shardkv     # or build the branch from source
```

**The container image**, multi-arch (`linux/amd64` and `linux/arm64`), built from the
`Dockerfile` in this repository and published with OCI annotations, build provenance and an
SBOM:

```bash
docker run --rm -p 6380:6380 -v shardkv-data:/data ghcr.io/black-third/shardkv:latest
```

It runs as a non-root user whose only writable directory is `/data`, and it carries a
`HEALTHCHECK` that sends a real `PING` over the socket — so `docker ps` reports healthy
only once the server is answering commands, not merely holding the port open.

**Compose**, for the three shapes worth trying. Each is one command, and each file's header
comment says exactly how to talk to what it started:

```bash
docker compose -f deploy/docker-compose.single.yml  up -d --build   # one node + AOF
docker compose -f deploy/docker-compose.replica.yml up -d --build   # primary + replica
docker compose -f deploy/docker-compose.cluster.yml up -d --build   # three-node cluster
```

The cluster file is the one to read before running. There is no cluster bus here, so slots
do not assign themselves and configuration does not gossip: an `init` sidecar runs
`CLUSTER ADDSLOTSRANGE` on each node and then `CLUSTER MEET` for every ordered pair, and it
is idempotent — a second `up` finds the slot map already in `nodes.conf` and leaves it
alone. `docker compose -f deploy/docker-compose.cluster.yml logs init` shows what it did.
See [Cluster](#cluster) for the boundary of what that mode does and does not implement.

## Quick start

```bash
go run ./cmd/shardkv -addr :6380

# real redis-cli works against it:
redis-cli -p 6380 set greeting hello ex 60 nx
redis-cli -p 6380 zadd leaderboard 100 alice 200 bob 150 carol
redis-cli -p 6380 zrange leaderboard 0 -1 withscores   # alice 100 carol 150 bob 200
redis-cli -p 6380 zrangebyscore leaderboard '(100' +inf   # carol bob
redis-cli -p 6380 lpush tasks build test ship
redis-cli -p 6380 lmove tasks done left right
redis-cli -p 6380 hset profile lang Go role systems
redis-cli -p 6380 hrandfield profile 2 withvalues
redis-cli -p 6380 sadd a 1 2 3 && redis-cli -p 6380 sadd b 2 3 4
redis-cli -p 6380 sinterstore both a b                 # 2
redis-cli -p 6380 info replication

# or with no Redis installed, a raw socket:
printf 'SET foo bar\r\nGET foo\r\n' | nc 127.0.0.1 6380
```

Or as a three-node cluster, with keys routed by hash slot and a real `redis-cli -c`
following the redirects:

```bash
for p in 7001 7002 7003; do
  ./shardkv -addr :$p -cluster-enabled -cluster-config-file n$p.conf &
done
redis-cli -p 7001 CLUSTER ADDSLOTSRANGE 0 5460
redis-cli -p 7002 CLUSTER ADDSLOTSRANGE 5461 10922
redis-cli -p 7003 CLUSTER ADDSLOTSRANGE 10923 16383
for a in 7001 7002 7003; do for b in 7001 7002 7003; do
  [ "$a" = "$b" ] || redis-cli -p $a CLUSTER MEET 127.0.0.1 $b
done; done

redis-cli -c -p 7001 SET foo bar     # -> Redirected to slot [12182] at 127.0.0.1:7003
```

See [Cluster](#cluster) for the slot migration flow and for the precise boundary of what
of Redis Cluster is and is not implemented.

RESP3, blocking commands and databases:

```bash
# RESP3: HGETALL is a map, SMEMBERS a set, ZSCORE a double
redis-cli -3 -p 6380 hset user:1 name ada lang go
redis-cli -3 -p 6380 hgetall user:1        # 1# "name" => "ada"  2# "lang" => "go"
redis-cli -3 -p 6380 smembers tags         # a set reply
redis-cli -3 -p 6380 zscore leaderboard bob   # (double) 200

# a work queue: this blocks until something is pushed, or 5s elapses
redis-cli -p 6380 blpop jobs 5 &
redis-cli -p 6380 rpush jobs 'render 42'   # the blocked client returns "jobs" "render 42"

# databases
redis-cli -p 6380 -n 1 set tenant acme     # -n selects the database
redis-cli -p 6380 -n 1 dbsize              # 1
redis-cli -p 6380 dbsize                   # database 0 is unaffected
redis-cli -p 6380 swapdb 0 1
redis-cli -p 6380 info keyspace            # one line per non-empty database
```

Streams and consumer groups:

```bash
redis-cli -p 6380 xadd orders '*' item widget qty 3    # 1785915671675-0
redis-cli -p 6380 xadd orders '*' item gadget qty 1
redis-cli -p 6380 xlen orders                          # 2
redis-cli -p 6380 xrange orders - +                    # both entries, oldest first
redis-cli -p 6380 xrevrange orders + - count 1         # just the newest

# a consumer group: two workers share the stream, and what each is given stays
# pending until it acknowledges
redis-cli -p 6380 xgroup create orders fulfil 0
redis-cli -p 6380 xreadgroup group fulfil alice count 1 streams orders '>'
redis-cli -p 6380 xpending orders fulfil               # 1 entry, owned by alice
redis-cli -p 6380 xack orders fulfil 1785915671675-0   # 1

# a worker that died: XAUTOCLAIM hands its unacknowledged work to another
redis-cli -p 6380 xreadgroup group fulfil bob count 1 streams orders '>'
redis-cli -p 6380 xautoclaim orders fulfil carol 0 0
redis-cli -p 6380 xinfo groups orders

# XREAD BLOCK with $ waits for entries added after the call
redis-cli -p 6380 xread block 0 streams orders '$' &
redis-cli -p 6380 xadd orders '*' item doohickey qty 7
```

Bitmaps, HyperLogLog and geospatial indexes — all on the existing types:

```bash
# a bitmap is a string, so every string command still works on it
redis-cli -p 6380 setbit visitors 1000 1
redis-cli -p 6380 bitcount visitors                    # 1
redis-cli -p 6380 strlen visitors                      # 126 bytes
redis-cli -p 6380 bitfield counters set u8 0 250 overflow sat incrby u8 0 10  # 0, 255

# HyperLogLog: 12KB counts billions to within about 1%
redis-cli -p 6380 pfadd uniques alice bob carol
redis-cli -p 6380 pfadd uniques2 carol dave
redis-cli -p 6380 pfcount uniques uniques2             # 4 (the union)
redis-cli -p 6380 type uniques                         # string — it is a Redis HYLL

# geospatial: a sorted set whose score is a 52-bit geohash
redis-cli -p 6380 geoadd Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
redis-cli -p 6380 geodist Sicily Palermo Catania km     # 166.2742
redis-cli -p 6380 geosearch Sicily fromlonlat 15 37 byradius 200 km asc withdist
redis-cli -p 6380 zscore Sicily Palermo                 # the raw geohash
```

Observability:

```bash
redis-cli -p 6380 config set slowlog-log-slower-than 1000   # microseconds
redis-cli -p 6380 slowlog get 5
redis-cli -p 6380 info commandstats     # cmdstat_get:calls=2,usec=5,...
redis-cli -p 6380 info latencystats     # latency_percentiles_usec_get:p50=...
redis-cli -p 6380 info stats            # keyspace_hits / keyspace_misses
redis-cli -p 6380 memory usage greeting
redis-cli -p 6380 command getkeys mset a 1 b 2          # a b

# watch every command the server runs (AUTH arguments are redacted)
redis-cli -p 6380 monitor
```

With a password, TLS, and keyspace notifications on:

```bash
go run ./cmd/shardkv -addr :6380 -requirepass s3cret \
    -tls-cert server.pem -tls-key server-key.pem -tls-ca ca.pem \
    -notify-keyspace-events KEA

redis-cli -p 6380 --tls --cert client.pem --key client-key.pem --cacert ca.pem \
    -a s3cret set foo bar
redis-cli -p 6380 -a s3cret psubscribe '__keyevent@0__:*'   # watch every change
```

Every flag:

```
-addr :6380                     TCP address to listen on
-shards 256                     lock shards per database (rounded to a power of two)
-databases 16                   how many databases SELECT can switch between
-sweep 1s                       interval between active expiration sweeps
-maxkeys 0                      approximate-LRU cap on live keys, per database
-maxmemory 0                    byte budget for the dataset (100mb, 1gb, …); 0 = unbounded
-maxmemory-policy               what happens at the budget; default derived from -maxkeys
-maxmemory-samples 16           keys the sampler examines before choosing a victim
-aof path                       append-only file (empty disables persistence)
-aofsync everysec               always | everysec | no
-aof-rewrite-min-size 67108864  smallest log that may trigger an automatic rewrite
-aof-rewrite-percentage 100     growth over the post-rewrite size that triggers one
-replicaof host:port            replicate from this master
-repl-backlog-size 1048576      stream retained for partial resync
-requirepass secret             require AUTH
-masterauth secret              password presented to a master
-tls-cert / -tls-key / -tls-ca  listener TLS
-tls-replication                dial the master over TLS
-notify-keyspace-events KEA     keyspace notification classes
-slowlog-log-slower-than 10000  microseconds a command must take to be logged
                                (negative disables, 0 logs everything)
-slowlog-max-len 128            how many slow-log entries are retained
-latency-monitor-threshold 0    milliseconds an event must take to be sampled
                                (0 disables the latency monitor)
-maxclients 10000               simultaneous connections; further ones are refused
                                with -ERR max number of clients reached, and closed
-timeout 0                      seconds a client may be idle before being closed
                                (0 = never; replicas, subscribers, monitors and
                                blocked clients are exempt)
-enable-debug-command no        whether DEBUG may be run: no|yes|local
                                (local = loopback connections only). Off by
                                default, as in Redis 7: DEBUG SET-ACTIVE-EXPIRE
                                and DEBUG CHANGE-REPL-ID change server-wide
                                behaviour, and nothing a client library does in
                                normal operation calls DEBUG at all
```

`CONFIG GET`/`SET` reach the same settings at runtime under Redis's names, plus the
representation thresholds `OBJECT ENCODING` reports from — `hash-max-listpack-entries`,
`hash-max-listpack-value`, `list-max-listpack-size`, `set-max-intset-entries`,
`set-max-listpack-entries`, `set-max-listpack-value`, `zset-max-listpack-entries`,
`zset-max-listpack-value` and `hll-sparse-max-bytes`, each also answering to its older
`ziplist` spelling. Those are real settings rather than remembered numbers: lowering one
changes what `OBJECT ENCODING` says about a value, and `hll-sparse-max-bytes` genuinely
moves the point at which a HyperLogLog stops being sparse. See
[OBJECT ENCODING](#object-encoding-and-the-representation-thresholds).

The `maxmemory` family is real and settable: `maxmemory` is a byte budget enforced on the
write path, `maxmemory-policy` selects between Redis's eight policies, `maxmemory-samples`
changes how many keys the sampler examines, and `lfu-log-factor`/`lfu-decay-time` tune the
LFU counter. Operands take Redis's suffixes and read back as Redis reports them — `CONFIG SET
maxmemory 100mb` answers `104857600`, not `100mb`. The policy is read from the same accessor
`INFO` reports it from, so the two cannot disagree. `maxmemory-samples` defaults to 16 rather
than Redis's 5 (a sample here is drawn from one shard, so it sees 1/256th of the keyspace),
and the number reported is the number the sampler uses. `maxmemory-clients 0` stays read-only,
because there is no client eviction to configure — a settable number that changed nothing
would be worse than an honest constant.

All three are also startup flags (`-maxmemory 100mb`, `-maxmemory-policy allkeys-lfu`,
`-maxmemory-samples 32`), applied through the same table `CONFIG SET` uses, so a flag and a
runtime change cannot come to mean different things and a mistyped flag is refused with the
sentence `CONFIG SET` would have sent. A flag left off is not applied at all rather than
applied as its default, which is what keeps `-maxmemory-policy`'s default *derived*: a server
with `-maxkeys` and no explicit policy evicts by `allkeys-lru`.

Pub/Sub, in two terminals:

```bash
redis-cli -p 6380 subscribe news sports
redis-cli -p 6380 publish news 'deploy finished'     # (integer) 1
redis-cli -p 6380 pubsub channels                    # news, sports
```

## Persistence (AOF)

```bash
go run ./cmd/shardkv -aof data.aof -aofsync everysec
```

Every write command is appended to `data.aof` in RESP. On the next start the log
is replayed to rebuild the dataset. `-aofsync` trades durability against
throughput: `always` fsyncs after each write, `everysec` flushes once per second
in the background (default), `no` leaves flushing to the OS.

**Rewrite.** A log of history alone only grows: a counter incremented a million
times replays as a million records. `BGREWRITEAOF` replaces it with a snapshot of
the present, and the automatic policy does the same when the log has grown past
`-aof-rewrite-percentage` percent of its size after the *last* rewrite (default
100, i.e. a doubling) and is at least `-aof-rewrite-min-size` bytes (default 64 MB).
Both conditions matter: the percentage alone rewrites a 200-byte log constantly,
and the size alone rewrites a large log that is not growing. `INFO persistence`
reports `aof_rewrite_in_progress`, `aof_base_size`, `aof_current_size` and
`aof_last_bgrewrite_status`.

```bash
redis-cli -p 6380 bgrewriteaof     # Background append only file rewriting started
redis-cli -p 6380 info persistence
redis-cli -p 6380 config get 'auto-aof-rewrite-*'
```

The two policy knobs are readable and writable at runtime under Redis's names
(`auto-aof-rewrite-percentage`, `auto-aof-rewrite-min-size`), so an existing tool
finds them where it expects; the command-line flags drop the `auto-` prefix
(`-aof-rewrite-percentage`, `-aof-rewrite-min-size`).

**What the rewrite guarantees.** It holds the write-ordering lock (`propMu`) across
*both* `Store.Dump()` and the file swap. Write commands hold that same lock across
their mutation and their append, so while the rewrite runs no write can be applied
and no record appended. The result is an exact log: the snapshot is the dataset as
of the instant the rewrite began, and every write ordered after that instant appends
after the swap — nothing lost, nothing applied twice, no record of the discarded
history surviving. Replaying the rewritten log reconstructs exactly the state
replaying the old one would have.

The price is that writers block for the duration of the rewrite, O(dataset), and
that is deliberate. Real Redis forks and accumulates concurrent writes in a diff
buffer, which keeps writes flowing at the cost of a copy-on-write child, a second
buffer, and the reconciliation between them. One lock is the version whose
correctness fits in a sentence and is checked by a test that hammers the server from
eight goroutines while a rewrite runs and then compares a replay of the log against
the live dataset. `BGREWRITEAOF` is still asynchronous with respect to the *client*:
the rewrite runs on its own goroutine and the caller is acknowledged immediately.
A failed rewrite (a temp file that cannot be created, a rename that fails) leaves
the existing log open and appendable, and is reported as
`aof_last_bgrewrite_status:err`.

## Persistence (snapshots)

```bash
go run ./cmd/shardkv -snapshot dump.skv -save "3600 1 300 100 60 10000"
```

An AOF is a *history*; a snapshot is a *state*. The difference matters twice: a cold
start on an AOF replays every write the server ever accepted, so start-up time grows
with the workload rather than with the dataset, and there is no single file an operator
can copy for a backup, because the log is being appended to while they copy it. A
snapshot is one file, complete as of one instant, and holds one command per key rather
than one per write.

```bash
redis-cli -p 6380 save              # OK          -- synchronous, blocks this client
redis-cli -p 6380 bgsave            # Background saving started
redis-cli -p 6380 lastsave          # (integer) 1786027613
redis-cli -p 6380 info persistence  # rdb_* fields
redis-cli -p 6380 debug reload      # save, then load it back in place
```

`-save` takes Redis's `<seconds> <changes>` pairs and defaults to Redis's own default, so
`CONFIG GET save` reports the string a client expects; it only ever fires on a server that
was given a `-snapshot` path. An empty string disables scheduled saving. Each rule means
"save if at least *changes* writes have changed the dataset and at least *seconds* have
passed since the dataset was last written out in full" — where "in full" counts a
successful AOF rewrite as well as a successful save, because both put a complete copy on
disk.

### This is not an RDB file

The format is shardkv's own. **Redis cannot read a shardkv snapshot and shardkv cannot
read a `dump.rdb`.** The first line of every file says so in ASCII, so an operator who
finds one on disk learns it from the file:

```
SHARDKV-SNAPSHOT v1 -- shardkv native format. NOT an RDB file: Redis cannot read this.
```

The alternative was attempted-RDB, and it was rejected for the same reason `DUMP`/`RESTORE`
uses its own payload format (see [No RDB, in either direction](#compatibility-what-is-missing)):
RDB is versioned and has a per-type opcode table with several size-threshold-selected
encodings per type — listpack, quicklist, intset, ziplist, stream listpacks with their own
framing. A half-implemented writer produces a file that either fails to load, which is
merely useless, or loads *wrongly*, which is silent corruption of the one artefact whose
entire purpose is to be trustworthy after everything else has already gone wrong.

What that costs, stated because it is real:

- **`redis-check-rdb`, `rdb-tools` and every other RDB inspector cannot read a snapshot.**
  The mitigation is that the body is a plain RESP command stream, so `redis-cli --pipe`
  can replay a snapshot's body into any Redis server, and any RESP reader can dump it.
- **A real Redis's `dump.rdb` cannot be loaded here.** Migrating from Redis goes over the
  wire — a replica sync, or `DUMP`/`RESTORE` per key — not through the file.
- **No cross-version binary promise beyond the version in the magic line.** A future v2
  will be a different magic line, and this reader refuses it by name rather than misreading
  it. A `dump.rdb` handed to it is named as such: `not a shardkv snapshot file: this looks
  like a Redis RDB file, which shardkv cannot read`.

### The format

```
<magic line>        ASCII, ends with '\n'; names the format, the version, and that it is not an RDB
uint64 big-endian   the instant the snapshot was taken, Unix milliseconds
uint64 big-endian   how many commands the body holds
uint64 big-endian   how many bytes the body holds
<body>              exactly that many bytes: RESP arrays of bulk strings — the same encoding
                    the AOF and the replica stream use
uint64 big-endian   CRC-64 (ECMA) of the body
```

The body is `Store.Dump()`'s command stream, which is what the AOF rewrite and a replica
seed already use, so the snapshot inherits the parts of this that are hard and already
tested: collections chunked at 256 elements so no command exceeds `resp.MaxMultiBulk`, a
chunk boundary that never splits an `HSET` field/value or `ZADD` score/member pair, a key's
`PEXPIREAT` after its last chunk, and a stream's id counters, consumer groups, consumers
and pending-entries list. Each database is preceded by the `SELECT` that puts a replayer
into it, and a dataset that only uses database 0 emits no `SELECT` at all.

The three counters exist for one reason: **a snapshot has to be able to say it is
incomplete.** A bare command stream cannot — a file truncated after 900 of 1000 commands
parses cleanly as a smaller dataset, and nothing can tell that the other 100 keys ever
existed. The byte length catches truncation, the command count catches a body that parses
to fewer commands than were written, and the checksum catches the rest. Any of the three
failing is an error and the server refuses to start; it is never a partial load. That is
the opposite of the AOF's behaviour, and deliberately: a torn AOF tail is the expected
shape of a crash, whereas a snapshot is written to a temporary file and renamed, so it is
whole or absent and anything else has been damaged after the fact.

**The write is atomic.** The bytes go to `<path>.tmp`, are fsynced, and are then renamed
over the destination; the directory is fsynced afterwards so the rename itself survives a
power loss. Rename is atomic within a directory, so a reader sees either the previous
snapshot or the new one and never a mixture — the crash that made the backup necessary
cannot be the crash that destroys it. The directory fsync is best effort, as in the AOF
rewrite: some platforms refuse it, and failing a save whose data has already reached the
disk would be the worse answer. A failed save leaves the previous snapshot untouched and
no `.tmp` debris behind, and reports `rdb_last_bgsave_status:err`.

### What consistency a save provides

**The cut is a single instant for the whole keyspace.** Not per-shard-consistent, and not
Redis's copy-on-write: `propMu`, `crossDBMu` and every shard's read lock in every database
are all held together for the whole walk, so no write can be applied anywhere while it is
being read. There is no window in which one shard is captured before a write and another
after it — which is exactly what a shard-at-a-time walk would produce, since a key and
another key it is written with hash to different shards.

Three things follow, and the third is a limit rather than a guarantee:

- Every individual write is wholly in the snapshot or wholly out of it, in every
  propagation mode.
- A command that is atomic across shards on its own — the ones built on `Store.lockKeys`:
  `RENAME`, `COPY`, `SMOVE`, `LMOVE`/`RPOPLPUSH`, `MSETNX` — is likewise wholly in or
  wholly out. This is pinned by a test that shuttles an element between two lists in
  different shards from four connections while snapshots are taken, and requires the total
  across the pair to be invariant in every file.
- A write that is **not** internally atomic across shards is not made atomic by the cut.
  `MSET` is the case that exists today: it is a loop of independent single-key writes with
  no cross-shard lock, so a cut can land between its keys — exactly as a concurrent `MGET`
  can. The snapshot neither creates that nor can fix it. Likewise a `MULTI`/`EXEC` batch is
  atomic against the cut **only when propagation is active**, because that is when `EXEC`
  holds `propMu`; on a pure cache (no AOF, no replica) `EXEC` deliberately does not, and
  its commands are applied one at a time to any observer, this one included.

**What "background" means here is narrower than in Redis**, and the difference is stated
rather than papered over. `BGSAVE` does not block the *client* — it returns as soon as the
save starts — and does not block anything for the *file write*: encoding, fsync and rename
all happen on the background goroutine with no lock held. It *does* block **writers** for
the length of the in-memory walk, which is O(dataset) with no I/O in it; readers are
unaffected, because the walk holds read locks. Real Redis forks and pays copy-on-write page
faults instead. There is no fork here, so the choice is between blocking writers briefly
and producing a file that never existed as a state, and the first is the one whose
correctness fits in a sentence. The peak memory cost is the command stream itself — roughly
the serialized size of the dataset — held while the file is written, which is the price of
not forking and the same order as the copy-on-write cost a fork can reach.

`SAVE` takes the same cut and holds nothing longer, but writes the file on the calling
connection's goroutine, so the client waits for the disk.

### Snapshot and AOF together

**The AOF wins when both exist.** That is Redis's rule (its `loadDataFromDisk` loads the
AOF when `appendonly` is on and the RDB only otherwise) and it is the right one: the AOF
recorded every write up to the crash while the snapshot stopped at the last save, so the
AOF is by construction at least as recent. Applying both would double-apply everything the
snapshot already describes — harmless for `SET`, wrong for `RPUSH`, `SADD`, `XADD` and
every other command whose replay is not idempotent.

One case is handled differently from Redis, on purpose. With an AOF configured but *empty*
next to a snapshot that is not, Redis starts empty and the operator loses the dataset by
enabling a durability feature. Here the snapshot is loaded and the AOF is then rewritten
from it immediately, before any client can connect, so the log describes the whole dataset
from its first byte. A failure of that rewrite is fatal rather than logged: continuing
would leave an AOF that is authoritative on the next restart and describes only part of
what is in memory.

`LASTSAVE` after a restart reports the instant recorded *in the file*, not the moment this
process started. Redis reports its start time there; a backup taken three days ago is not
a save that happened at boot, and the whole use of the field is to answer "how stale is the
copy on disk".

### `INFO persistence`

`rdb_last_save_time` and `rdb_changes_since_last_save` are the two an operator polls: the
first is the last full write of the dataset (a save, an AOF rewrite, or the instant in a
loaded snapshot), and the second is how many writes have changed it since. The counter is
*decremented* by what it read at the cut rather than zeroed, so writes that landed while
the file was being written are still correctly reported as unsaved.

`DEBUG RELOAD` saves and loads back in place — Redis's own test hook, and what its suite
uses after almost every interesting mutation to check that every type survives the round
trip. The load is completed and verified *before* anything is discarded, so a snapshot
that does not parse leaves the dataset alone. With snapshots disabled it does the same
round trip through memory, using the same encoder and decoder, so the check is available
on the default configuration too.

## Replication

```bash
# terminal 1 — master
go run ./cmd/shardkv -addr :6380

# terminal 2 — replica
go run ./cmd/shardkv -addr :6381 -replicaof 127.0.0.1:6380
```

The replica authenticates if `-masterauth` is set, announces itself with
`REPLCONF listening-port`, then issues `PSYNC`. On a first connection it receives a
point-in-time snapshot of the master's dataset and applies the live write stream
after it. Replicas reject client writes (`READONLY`), re-propagate the stream to
their own AOF and downstream replicas (chaining), and can be toggled at runtime with
`REPLICAOF host port` / `REPLICAOF NO ONE`.

**Offsets and partial resync.** The master counts the bytes it has put on its
replication stream (`master_repl_offset`) and retains the tail of that stream in a
bounded ring (`-repl-backlog-size`, default 1 MB). A replica tracks how far it has
processed (`slave_repl_offset`) and reports it back with `REPLCONF ACK`, so on a
reconnect it asks `PSYNC <replid> <offset>`:

- the offset is still in the backlog → `+CONTINUE`, and the master streams only the
  bytes that were missed (`sync_partial_ok`);
- it is not, or the replication ID names a history this master does not serve → 
  `+FULLRESYNC <replid> <offset>` and a fresh snapshot (`sync_partial_err`,
  `sync_full`).

Sizing the backlog is one trade: memory against how long a replica may be absent and
still come back cheaply. Write rate × tolerable disconnect window gives the number;
1 MB covers a few seconds of a busy master or minutes of a quiet one. `0` disables
continuations entirely. `INFO replication` reports `repl_backlog_size` and
`repl_backlog_histlen`, which is how much window is currently left.

The bound is in bytes rather than commands because the cost being controlled is
memory, and one command may be a 4-byte `PING` or a 512 MB `SET`. It is a fixed-size
ring rather than an append-and-trim slice because trimming the front of a slice either
leaks the discarded prefix or costs an O(size) copy per append.

A full resync flushes the replica's dataset before applying the snapshot. A snapshot
describes what the master *has*, so it never mentions a key the master deleted while
the replica was disconnected — without the flush, such a key would survive on the
replica forever.

**Why a replica advertises its own replication ID.** A replica re-encodes the stream
it forwards to its own replicas rather than relaying its master's bytes, so its
downstream stream is a different byte sequence with its own offsets. It therefore
serves (and advertises in `master_replid`) its *own* stream ID: advertising the
upstream one would invite a downstream replica to ask for a continuation at an offset
that means something else entirely. Being promoted with `REPLICAOF NO ONE` discards
the continuation point held for the old master, since a promoted master accepts
writes and its history has diverged from the one it was following.

**`WAIT`.** `WAIT numreplicas timeout` asks the replicas to acknowledge and reports
how many have processed everything written so far. It is a measurement, not a
guarantee: the write it waits on was already applied and acknowledged to its client,
so `WAIT` cannot make it durable retroactively — what it offers is the number of
copies that hold it, which is what a caller needs to decide whether to proceed.

**Liveness.** Each feed carries a periodic no-op `PING`, and the replica applies a
read deadline an order of magnitude longer than that interval. A master that dies
without closing its socket — a network partition, a half-open connection, a hung
process — would otherwise leave the replica blocked in a read forever, serving
stale data and never reconnecting; instead the deadline elapses and it resyncs.
The keepalive is point-to-point: it is never persisted to an AOF, never forwarded
down a chain (each feed sends its own), never buffered into a `MULTI` group (so it
cannot splice a stray command into a replayed transaction), and **never counted into
the replication offset** — it is written straight to one feed rather than through the
shared stream, so counting it would give every replica a different idea of the same
offset. The replica discounts its bytes explicitly, and piggybacks its `REPLCONF ACK`
on the wakeup the keepalive causes rather than running a timer of its own.

**Consistency model.** When persistence or replication is active, write commands
are totally ordered through a single lock that spans the store mutation *and* its
propagation, so the order applied to memory is exactly the order written to the
AOF and shipped to replicas — concurrent writes to the same key can no longer
make a master and its replica/AOF diverge. The `PSYNC` decision is taken under that
same lock, which is what makes both of its answers exact: a `+FULLRESYNC` snapshot is
a consistent cut (every write is in either the snapshot or the live stream, never
both, so there is no double-apply of `INCR`/`RPUSH`), and a `+CONTINUE` reads the
backlog up to exactly the offset the feed then continues from, so the replayed bytes
and the queued commands abut with no gap and no overlap. An AOF rewrite is taken under
the same lock for the same reason. A pure single-node cache (no AOF, no replicas)
keeps writes sharded-concurrent and pays none of this. One narrow window remains: a
master started *without* `-aof` only begins serializing writes at the first `PSYNC`,
so a write already in flight at that instant may be missed by that first replica —
run a replicated master with `-aof` to close it.

**Falling behind.** A replica's feed is a bounded buffer, so a replica slower than
the write rate eventually fills it. Skipping the command for that replica would
leave it missing a write with nothing to ever tell it so — permanently and
silently diverged — so the master instead drops the feed, which closes the
connection and makes the replica reconnect. A resync is the only route back to
agreement; with the backlog in place, that resync is usually a cheap `+CONTINUE`
rather than a full snapshot, because the commands it missed are still retained.
`INFO` counts every outcome (`replica_drops`, `sync_full`, `sync_partial_ok`,
`sync_partial_err`).

**Transactions on the wire.** An `EXEC`'d transaction is shipped to the AOF and
replicas wrapped in `MULTI`/`EXEC`; replay and replica-apply buffer the group and
commit it only on `EXEC`, so a crash that truncates the AOF mid-transaction (no
`EXEC`) replays none of it — all-or-nothing.

**Databases on the wire.** The stream carries no database context of its own, so a
write in database *n* is preceded by a `SELECT n` emitted only when the stream's
position changes; a snapshot frames each database the same way and ends by returning the
replayer to the position the ongoing stream is in. A database-0-only workload therefore
ships exactly the bytes it shipped before databases existed. A `SELECT` in the stream is
applier state, not a command to apply, and it is honoured even inside a replayed
transaction — a transaction whose writes spanned two databases would otherwise land
entirely in whichever one it started in. See [Databases](#databases).

**Blocking commands on the wire.** A blocking command never reaches the AOF or a
replica; the pop it performed does. A replica replaying `BLPOP` would wait forever on a
connection with no client behind it. See [Blocking commands](#blocking-commands).

## Protocol: RESP2 and RESP3

Both protocol versions are spoken in full. A connection starts in RESP2 and
`HELLO 3` switches it; `HELLO 2` switches back, and `RESET` returns it to the
default. The version lives on the connection's writer, because the reply encoding is
a property of one socket and of nothing else — an AOF replay, a replica feed and a
`CLIENT LIST` on another connection are all unaffected by what one client negotiated.

RESP3 adds encodings (`%` map, `~` set, `,` double, `#` boolean, `(` big number,
`=` verbatim string, `_` null, `>` push, `|` attribute), but the changes that matter
are the ones where a reply's *shape* differs. These follow real Redis 7 exactly — every
expected byte string in the tests was captured from `redis:7-alpine` over a `HELLO 3`
connection:

| Command | RESP2 | RESP3 |
| --- | --- | --- |
| `HGETALL` | flat `[f, v, f, v]` array | map `%2` |
| `CONFIG GET` | flat name/value array | map |
| `SMEMBERS`, `SPOP key count`, `SINTER`/`SUNION`/`SDIFF` | array | set `~` |
| `SRANDMEMBER key count` | array | **array** — a negative count may repeat a member, so it is not a set |
| `ZSCORE`, `ZINCRBY`, `ZADD ... INCR` | bulk string | double `,` |
| `ZRANGE ... WITHSCORES` and the range family | `2n` flattened elements | `n` pairs, score a double |
| `ZPOPMIN key` / `ZPOPMIN key count` | flat pair / flat pairs | flat pair / **list of** pairs |
| `HRANDFIELD ... WITHVALUES` | flattened | list of pairs |
| `INFO`, `CLIENT INFO`, `CLIENT LIST` | bulk string | verbatim string `=txt:` |
| `GET` on a missing key | `$-1` | `_` |
| aborted `EXEC`, `LPOP key count` on a missing key, a `BLPOP` timeout | `*-1` | `_` |
| Pub/Sub `message`, `subscribe`, `unsubscribe` | array reply | push `>` |

Two findings worth stating, because they contradict what the RESP3 specification
suggests:

- **`EXISTS`, `SISMEMBER`, `HEXISTS`, `SETNX`, `EXPIRE`, `PERSIST`, `RENAMENX`,
  `MOVE` and `COPY` keep replying with integers**, not booleans. The protocol has a
  boolean type and its documentation implies these would use it, but Redis 7 replies
  `:1`/`:0` to both protocols — so this does too. Wire compatibility with the clients
  people actually run beats the specification's suggestion. The boolean type *is*
  implemented; `DEBUG PROTOCOL true` and the map `DEBUG PROTOCOL map` returns are
  where it is used, which is also where Redis uses it.
- **`INCRBYFLOAT` and `HINCRBYFLOAT` stay bulk strings** in RESP3. They render a long
  double rather than a double, and Redis reflects that by not using the double type
  for them.

The behavioural difference, rather than the encoding one, is Pub/Sub. A RESP2
subscriber is restricted to the subscribe family plus `PING`/`QUIT`/`RESET`, because
RESP2 has no way to tag a delivered message and the client could not tell it from the
reply it was waiting for. RESP3 pushes are tagged, so a RESP3 connection keeps the
whole command surface while subscribed:

```bash
redis-cli -3 -p 6380 --timeout 0 subscribe news &
redis-cli -3 -p 6380 publish news hello
```

`DEBUG PROTOCOL <type>` returns one value of each type, which is how a client
library's protocol tests check a server and the only place the big-number and
attribute types have a use here. (`redis-cli` in piped, non-interactive mode cannot
print those two and reports a protocol error — verified to fail identically against
real Redis, so it is a client limitation.)

## Blocking commands

`BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` `BZPOPMIN` `BZPOPMAX` `BLMPOP` `BZMPOP` wait
for data instead of answering "nothing there". Timeouts are fractional seconds and 0
means forever.

```bash
# a worker pool: three consumers waiting on one queue
for i in 1 2 3; do redis-cli -p 6380 blpop jobs 0 & done
redis-cli -p 6380 info clients | grep blocked_clients   # blocked_clients:3
redis-cli -p 6380 rpush jobs a b c                      # all three are served, in order
```

**A blocked client holds nothing.** By the time the command decides to wait, the store
call has returned (so no shard lock), `propMu` is released (so no other write is held
up), and the connection's writer lock has been handed back — which is what lets a
subscribed RESP3 connection be blocked on a list and still receive messages.

**Wakeup is exact, not polled.** Every write that can create data signals the keys it
touched, driven by the same `affectedKeys` list that `WATCH` invalidation uses, so
anything that can make a key appear — `RPUSH`, `RENAME`, `LMOVE`, a transaction,
`SWAPDB` — wakes a waiter. The signal is a non-blocking send on the waiter's own
single-slot channel, so a pusher never waits on a waiter and a signal that arrives
while the waiter is mid-retry is not lost. There is no timer and no rescan; a waiter
that is never pushed to costs one parked goroutine. When nobody is blocked anywhere,
the whole feature is **one atomic load** on the write path.

**Fairness is FIFO, and it comes from waking only the head.** Each key's queue is
ordered by arrival, and only its head is ever signalled. A client behind the head is
never told there might be data, so it cannot race the head for it; if the head then
finds nothing — because a plain `LPOP` took the element in between — it goes back to
sleep at the head and nobody behind it was disturbed. A waiter that *is* served leaves
every queue it was in and signals the new head of each, because one push can carry
several elements and the next client in line has to be told about the rest.

**An arriving command declines to jump the queue.** A blocking command normally makes one
opportunistic attempt before joining any queue — that is the fast path, and it must not pay
for the wait machinery when the list already has an element. But if a waiter that *could be
served instead of it* is already queued on one of its keys, it skips that attempt and joins
the back. Without this, a single pipelining client could take its own push away from a
client that had been waiting: `LPUSH k v` and `BLPOP k 0` arrive in one read, and the
wakeup the push sends is a channel send whose recipient has not necessarily been scheduled
before the second command runs. Redis cannot have that problem — it serves blocked clients
between commands — and its own suite checks it. The check costs one atomic load when nobody
is blocked anywhere, which is the normal state.

It is deliberately type-aware: an arriving `BLPOP` does *not* queue behind a `BZPOPMIN`
waiter, because a list can never serve one and that waiter will never leave the queue. Only
a waiter that could take the same value is worth queueing behind.

What remains open, and is worth naming: a plain `LPOP` racing the same instant can always
take the element first, and two *different* connections issuing a write and a blocking read
at the same moment are ordered by whichever goroutine the scheduler reaches first. Closing
that would mean serializing every arriving command behind one lock, which is what a
single-threaded server does and what this one is built not to do.

**What propagates is the pop, never the command.** `BLPOP` ships the `LPOP` it
performed, `BLMOVE` the `LMOVE`, `BZPOPMIN` the `ZREM` of the member it removed. A
replica replaying `BLPOP` would wait forever on a connection with no client behind it.
For the same reason a blocking command reports its *effect* to `WATCH` and to keyspace
notifications rather than its own arguments: its arguments are a list of candidate
keys, and only the effect names the key that changed.

The rest of the decisions:

- **Inside `MULTI`/`EXEC` it does not block.** It takes its non-blocking behaviour, as
  in Redis: an `EXEC` that could wait would hold the batch, and `propMu` with it, for
  the whole timeout.
- **A disconnecting client leaves every queue.** A blocked client has stopped reading
  its own socket, so nothing would notice it hanging up. A watchdog goroutine peeks at
  the socket — a peek consumes nothing, so a pipelined command behind the blocking one
  is still there afterwards — and is stopped by a read deadline set in the past,
  followed by a handshake that guarantees the peek has finished before the connection's
  goroutine reads again.
- **A wakeup is filtered by type, so the wrong kind of value does not release a client.**
  A waiter is signalled by anything that creates one of its keys, and what appeared may be
  a string: `ZADD k 0 x; DEL k; SET k v` wakes a `BZPOPMIN`, which then has nothing it can
  serve. Answering `WRONGTYPE` there would fail a command whose own arguments were never
  wrong, over a value that is already gone by the time the client reads the error — so the
  client stays blocked and a later `ZADD` serves it, which is what Redis does. A refusal
  that *is* about the command still lands: `BRPOPLPUSH` onto a string destination answers
  `WRONGTYPE` when its source finally receives an element.
- **`FLUSHALL` and `FLUSHDB` release an `XREADGROUP` and nobody else.** They remove keys, so
  a `BLPOP` waiter is signalled, finds no list, and goes back to sleep. `XREADGROUP` is
  different in kind: it waits on a consumer *group*, and a flush destroyed it, so the client
  is now waiting for something that can never arrive and is told so (`NOGROUP`). The same
  reasoning releases it when its key is deleted or overwritten with another type. A key
  merely *expiring* releases nobody: an expired list has no element to hand over.
- **A demotion to replica unblocks everyone**, with the same `-UNBLOCKED` error Redis
  uses. These are write commands, and a replica refuses writes, so waiting for one to
  succeed would be waiting for something that can no longer happen.
- **`CLIENT UNBLOCK id [TIMEOUT|ERROR]`** ends another connection's wait: `TIMEOUT`
  makes it reply as if the timeout had elapsed, `ERROR` makes it reply `-UNBLOCKED`.
- **`INFO clients`** reports `blocked_clients` and `total_blocking_keys`, which is what
  an operator polls to see whether a queue has consumers — and what the tests poll to
  know every client has reached the wait before pushing.

## Cluster

The repository is called shardkv because it shards a keyspace across locks inside one
process. Cluster mode is the other half of that name: sharding across *nodes*.

**The client-facing half of Redis Cluster is implemented completely. The cluster bus is
not implemented at all.** That boundary is deliberate, and it is stated in full below
rather than left for you to discover.

### What is implemented

| | |
| --- | --- |
| **Hash slots** | CRC16-XMODEM over 16384 slots, with hash tags. `CLUSTER KEYSLOT` exposes it. Checked key-for-key against a live cluster-enabled `redis:7-alpine`, including the tag edge cases (`{}` empty tag, unclosed `{`, stray `}`, nested `{a{b}c`, repeated `{tag}{tag}`). |
| **Redirects** | `-MOVED <slot> <host>:<port>`, `-ASK` with the one-shot `ASKING` flag, `-CROSSSLOT` for a multi-key command spanning slots, `-TRYAGAIN` for one straddling a half-migrated slot, `-CLUSTERDOWN Hash slot not served` for an unassigned slot. |
| **Administration** | `CLUSTER INFO`, `MYID`, `NODES`, `SLOTS`, `SHARDS`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `ADDSLOTS`, `DELSLOTS`, `ADDSLOTSRANGE`, `DELSLOTSRANGE`, `SETSLOT`, `MEET`, `FORGET`, `REPLICATE`, `RESET`, `HELP`. |
| **Migration** | `DUMP`, `RESTORE` (`REPLACE`, `ABSTTL`, `IDLETIME`, `FREQ`), `MIGRATE` (`COPY`, `REPLACE`, `KEYS`, `AUTH`/`AUTH2`), and the `IMPORTING`/`MIGRATING` slot states that make `ASK` work. |
| **Replica reads** | `READONLY`/`READWRITE`. `CLUSTER REPLICATE` starts real replication against the master's client port, so a cluster replica actually carries the data. |
| **Configuration** | `-cluster-enabled`, `-cluster-config-file`, `-cluster-announce-ip`, `-cluster-announce-port`. The config file is the `CLUSTER NODES` text plus Redis's `vars` line, written atomically; a restarted node comes back as itself, owning the same slots. |

### What is not implemented, and what follows from it

Real Redis Cluster runs a **binary gossip protocol** on a second port (the client port
plus 10000). Nodes exchange `PING`/`PONG` packets carrying their view of the cluster,
which is how configuration propagates, how failures are detected, how a replica is
promoted, and how two nodes that disagree about a slot settle it by config epoch.

None of that is here. Concretely:

- **Configuration does not propagate.** A `CLUSTER ADDSLOTS` or `SETSLOT` on one node is
  known only to that node. You must tell every node, which is exactly what a resharding
  script already does (`redis-cli --cluster` sends `SETSLOT` to every master).
- **There is no failure detection.** No `PFAIL`, no `FAIL`, no quorum. `CLUSTER INFO`
  reports `cluster_slots_pfail:0` and `cluster_slots_fail:0` permanently, and
  `CLUSTER SHARDS` reports every known node as `health:online` — which is honest: this
  node knows of no node being down because it never checks.
- **There is no automatic failover.** A master that dies stays dead until an operator
  runs `CLUSTER REPLICATE`/`SETSLOT` by hand. There is no replica election.
- **Config epochs are recorded but never used to resolve conflicts**, because there is no
  gossip through which a conflict could arrive. `cluster_current_epoch` counts slot-map
  changes so an operator can see that something moved.
- **`cluster_stats_messages_sent`/`received` are zero and stay zero.** There is no bus to
  count messages on, and a fabricated number would be evidence of liveness checks that
  never happened.
- **The `@<cport>` in `CLUSTER NODES` is announced but never listened on.** It is
  `port + 10000`, present because the field is positional and clients parse it.
- **Pub/Sub is node-local.** In Redis Cluster, `PUBLISH` is broadcast to every node over
  the bus, so a subscriber on any node sees it. Here a message reaches only subscribers
  of the node it was published to. `SPUBLISH`/`SSUBSCRIBE` (sharded Pub/Sub, which routes
  by slot) are not implemented. Channels are not keys, have no slot, and are never
  redirected. **If you need cluster-wide Pub/Sub, subscribe to every node.**
- **Keyspace notifications stay node-local**, as they already were — each node reports
  the changes it applies.
- **`CLUSTER MEET` is synchronous and one-directional.** It is the only place
  configuration crosses a node boundary without an operator typing it: the node opens an
  *ordinary RESP client connection* to the peer, reads its `CLUSTER NODES`, and adopts
  the peer's id, announced address and the slots it claims — **once**, and only for slots
  that are still unassigned locally. That last rule is what makes a gossip-free cluster
  safe to assemble in any order: `MEET` can never move a slot from one node to another,
  so two nodes that disagree about an owner keep disagreeing *visibly* (one redirects to
  the other) instead of silently swapping ownership under a client. Moving a slot is
  `SETSLOT`'s job, and `SETSLOT` is explicit. Because it is one-directional, run it on
  each node for each peer.
- **`COUNTKEYSINSLOT`/`GETKEYSINSLOT` scan the keyspace**, O(keys on this node). Redis
  keeps a slot-to-keys index updated on every insert and delete; that index would put
  slot arithmetic on the write path of every command in order to speed up two
  administrative ones, which is the wrong trade for this server.
- **One database.** `SELECT n` for n > 0, `SWAPDB`, `MOVE` and `COPY ... DB` are refused
  in cluster mode, as in Redis.

The reason for drawing the line here rather than approximating the bus: a
half-implemented gossip protocol fails by *disagreeing silently* — two nodes each
convinced they own a slot, both accepting writes. That is the same failure class as
every invariant in [CLAUDE.md](CLAUDE.md), and a deliberately simple design is the one
thing that rules it out entirely.

### DUMP/RESTORE: the serialization

**These payloads are not Redis's RDB object encoding, and are deliberately not
interchangeable with it.** Reproducing RDB byte-for-byte would mean reimplementing
ziplist, listpack, intset and quicklist encodings whose only purpose is to save memory
in representations this store does not have — and getting one subtly wrong would produce
a payload a real Redis accepts and then *misreads*, which is the worst possible outcome
for a serialization.

The framing mirrors Redis's exactly, because that framing is what makes a foreign
payload detectable rather than misparsed:

```
+------------+---------------------+-----------+-----------------+
| "SHARDKV1" | RESP command array  | version   | CRC-64/ECMA     |
| 8 bytes    | N bytes             | 2 B LE    | 8 B LE          |
+------------+---------------------+-----------+-----------------+
```

Three independent gates reject anything this server did not write — the magic (a real
RDB payload fails on byte 0), the version (a payload from a newer format), and the
checksum (corruption or truncation) — and each answers with Redis's own
`ERR DUMP payload version or checksum are wrong`. The reverse holds too: a real Redis
rejects one of these, because the CRC-64 it computes is the Jones-polynomial one and
will not match.

The body is **the command sequence `Store.DumpKey` renders** — the same encoder the AOF
rewrite and the replica seed already use. That is why the format is cheap and why it is
trustworthy: invariant 5 already guarantees that sequence reconstructs every stored kind
exactly, chunked so no command can exceed the protocol's limits, with a stream's id
counters, consumer groups, consumers and pending-entries list included. A second encoder
written just for `DUMP` would be a second thing to keep correct, and the one that
drifted would drift silently. It is read back with the package's own `resp.Reader` — the
parser that has a fuzz target — rather than a hand-rolled one.

Two further properties:

- **The body is a whitelist, not a script.** Only the nine commands `DumpKey` emits are
  accepted (`SET`, `RPUSH`, `HSET`, `SADD`, `ZADD`, `XADD`, `XSETID`, `XCLAIM`,
  `XGROUP`), and each is retargeted at the key `RESTORE` named. A checksum proves the
  bytes are intact, never that they are benign: without the whitelist a payload carrying
  `FLUSHALL` would be a remote wipe with a valid checksum.
- **`RESTORE` is all-or-nothing.** The payload is rebuilt in a private scratch store and
  only the finished value is published (`store.AdoptKey`), so a payload that fails on its
  fourth command leaves no three-command remnant, invalidates no `WATCH`, and emits no
  keyspace notification for a value that never existed.

`FREQ` carries an LFU access counter and is applied: under an `allkeys-lfu` or
`volatile-lfu` policy the counter really is what the sampler ranks by, and `OBJECT FREQ`
reads it back. `IDLETIME` is applied too, so a key arriving from another node keeps the age
it had there. Which of the two a policy consults is the same either-or Redis has, since both
describe the same field: `OBJECT FREQ` is refused under a non-LFU policy and `OBJECT IDLETIME`
under an LFU one, with Redis's wording for each.

### Quick start: a three-node cluster

```bash
go build -o shardkv ./cmd/shardkv

for p in 7001 7002 7003; do
  ./shardkv -addr :$p -cluster-enabled -cluster-config-file n$p.conf \
            -cluster-announce-ip 127.0.0.1 -aof n$p.aof &
done

# 1. Give each node a third of the slot space.
redis-cli -p 7001 CLUSTER ADDSLOTSRANGE 0 5460
redis-cli -p 7002 CLUSTER ADDSLOTSRANGE 5461 10922
redis-cli -p 7003 CLUSTER ADDSLOTSRANGE 10923 16383

# 2. Introduce every node to every other. MEET is one-directional, so both ways.
for a in 7001 7002 7003; do for b in 7001 7002 7003; do
  [ "$a" = "$b" ] || redis-cli -p $a CLUSTER MEET 127.0.0.1 $b
done; done

redis-cli -p 7001 CLUSTER INFO | head -3   # cluster_state:ok, 16384 slots assigned
```

`-cluster-announce-ip` is the address **clients** are redirected to, which is not always
the address the nodes reach each other at. Behind a port mapping — a node on the host
driven by a `redis-cli` in a container — announce the client-visible name and `MEET`
over the node-visible one:

```bash
./shardkv -addr :7001 -cluster-enabled -cluster-announce-ip host.docker.internal &
redis-cli -p 7001 CLUSTER MEET 127.0.0.1 7002     # how the nodes reach each other
```

Then drive it with a cluster-aware client, which follows the redirects:

```console
$ redis-cli -c -p 7001
127.0.0.1:7001> SET foo bar
-> Redirected to slot [12182] located at 127.0.0.1:7003
OK
127.0.0.1:7001> MSET {user1000}.following 1 {user1000}.followers 2
-> Redirected to slot [3443] located at 127.0.0.1:7001
OK
127.0.0.1:7001> MGET foo hello
(error) CROSSSLOT Keys in request don't hash to the same slot
```

The hash tag is what makes the second command legal: `{user1000}.following` and
`{user1000}.followers` both hash `user1000`, so they share slot 3443 and one node owns
both. Without a tag, `foo` and `hello` land on different nodes and no node can serve a
command naming both.

### Migrating a slot

The four steps are Redis's, and the order is what the `ASK` redirect depends on:

```bash
SRC=$(redis-cli -p 7001 CLUSTER MYID)
DST=$(redis-cli -p 7003 CLUSTER MYID)

# 1. Open the slot on both sides.
redis-cli -p 7003 CLUSTER SETSLOT 3443 IMPORTING $SRC
redis-cli -p 7001 CLUSTER SETSLOT 3443 MIGRATING $DST

# 2. Move the keys in batches, until none are left.
while [ "$(redis-cli -p 7001 CLUSTER COUNTKEYSINSLOT 3443)" != 0 ]; do
  keys=$(redis-cli -p 7001 CLUSTER GETKEYSINSLOT 3443 10)
  redis-cli -p 7001 MIGRATE 127.0.0.1 7003 "" 0 5000 KEYS $keys
done

# 3. Hand the slot over, on every node.
for p in 7001 7002 7003; do redis-cli -p $p CLUSTER SETSLOT 3443 NODE $DST; done
```

Between steps 1 and 3 the slot is **open**, and that is when the redirects earn their
keep. On the source: a key still here is served, a key already moved draws
`-ASK 3443 <target>`, and a multi-key command straddling the two draws `-TRYAGAIN`. On
the target: an ordinary client gets `-MOVED` back to the source, and only a client that
sent `ASKING` first is served — for exactly one command, because ownership has not
changed yet. `redis-cli -c` follows all of it transparently.

Two guards make the loop safe. `CLUSTER SETSLOT ... NODE` is **refused while this node
still holds keys for the slot** (`ERR Can't assign hashslot 3443 to a different node
while I still hold keys for this hash slot.`) — handing it over early would strand those
keys, since every request for them would be redirected away. And `MIGRATE` deletes a key
here only *after* the destination has acknowledged it, so a connection that drops mid-way
leaves the key where it was rather than nowhere.

What `MIGRATE` puts on the wire for its own AOF and replicas is the `DEL` of the keys
that left — never the `MIGRATE` itself. Shipping the command verbatim would have every
replica open its own connection to the destination and send the same keys again, and an
AOF replay would do it once more on every restart, to a node that may no longer exist.
That is invariant 4 (propagate the effect, not the text) applied to a command whose
non-determinism is another machine.

Note that `MIGRATE` holds `propMu` for its whole network round trip, because it is a
write and invariant 1 orders a write against its propagation. Writes on the source are
therefore serialized for its duration — which is exactly what a single-threaded Redis
does with its own `MIGRATE`, and why the timeout operand exists.

## Databases

`SELECT index` switches the connection's database; `-databases N` sets how many there
are (16 by default, as in Redis). `SWAPDB`, `MOVE key db`, `COPY key dst DB n`,
`FLUSHDB` against `FLUSHALL`, a per-database `DBSIZE`, and one `INFO keyspace` line per
non-empty database.

```bash
redis-cli -p 6380 -n 1 mset tenant acme plan pro
redis-cli -p 6380 -n 1 dbsize        # 2
redis-cli -p 6380 dbsize             # 0 — a different keyspace entirely
redis-cli -p 6380 -n 1 move tenant 2
redis-cli -p 6380 swapdb 1 2
redis-cli -p 6380 info keyspace
# db1:keys=1,expires=0,avg_ttl=0
# db2:keys=1,expires=0,avg_ttl=0
# db_keys:2
# databases:16
```

**N independent keyspaces, not a database dimension inside each shard.** The two
shapes were the real choice. Putting a database dimension inside every shard
(`shard.data[db][key]`) would share one set of locks across all databases, so a client
working in database 1 would contend with every client in database 0 — the exact thing
the sharded design exists to avoid, reintroduced one level up. Giving each database its
own `Store` means its own shards, its own mutexes and its own eviction pass, so
databases are independent by construction, and it leaves the store completely unaware
that databases exist: `SELECT` is a server-level concept and stays one. The cost is one
map and mutex per shard per database — a few kilobytes for the default 16 × 256, which
is the right trade for removing a whole class of contention.

Threading a database index through 135 handler signatures was not necessary either.
A `Server` is a small value pairing the shared server state with *one* database, and a
connection's database is resolved once per command to one of a fixed set of these views
(built at startup, never allocated per command). A handler that never mentions
databases operates on the right one, and the paths that must know *which* database a
change happened in — propagation, `WATCH`, keyspace notifications, blocked-client
wakeups — read it off the view.

**Propagation: the database goes into the stream, as a lazy `SELECT`.** The AOF and the
replica stream are a flat sequence of commands, and a command names keys but never a
database, so the database has to become part of the stream. The alternative — tagging
every command with its database — would change the wire format and leave an AOF
unreplayable by the very server that wrote it. So a write in database *n* is preceded by
`SELECT n`, emitted only when the stream's position actually changes:

- A database-0-only workload ships **byte-for-byte what it shipped before databases
  existed**, so an AOF written by an older build replays identically and a replica sees
  no new commands.
- The stream stays a faithful record: a `SELECT` appears where the database actually
  changed, not once per write.
- A snapshot (a full resync, or an AOF rewrite) frames each database the same way and
  **ends by returning the replayer to the position the ongoing stream is in**. That last
  part is load-bearing: the stream's position is shared by every replica while a snapshot
  goes to one of them, so a snapshot that ended in database 15 would silently redirect
  every command that followed it into database 15 on that one replica.
- A client's `SELECT` is never itself propagated. The database a *connection* looks at
  is connection state, like its name or its protocol; what reaches the log is the
  database each individual write belongs to. The two happen to share a name.

**Pub/Sub is global; keyspace notifications are per database.** Channels have no
database in Redis and none here — `PUBLISH` from database 3 reaches a subscriber that
never left database 0, and `PUBLISH` is replicated without any `SELECT`, since it
belongs to no database. Keyspace notifications, which *are* about keys, carry the
database in the channel name: `__keyspace@7__:mykey` and `__keyevent@7__:set`. `MOVE`
is the one command whose two events land in two databases, and it fires `move_from` in
the source and `move_to` in the destination.

**`WATCH` is per database.** A registration is keyed by `(database, key)`, so a write
to the same key name in another database is not a conflict — and a `MOVE` *into* a
watched database is, because it writes that key there. `FLUSHDB` invalidates only its
own database's watchers; `FLUSHALL` invalidates every watcher; `SWAPDB` invalidates
both databases' watchers wholesale, because there is no key-level answer available once
two datasets have been exchanged. Blocked clients are keyed the same way, so a push to
`q` in database 0 leaves a client blocked on `q` in database 1 exactly where it was.

**Cross-database commands are serialized against each other.** `MOVE`, `COPY ... DB`
and `SWAPDB` each hold a lock in two independent stores, where no shard ordering can
help, so all three take one mutex first. It costs nothing — they are administrative
commands — and ordinary single-database commands are unaffected, because they never
hold locks in two stores and so cannot join such a cycle.

`maxkeys` applies per database, which is the only meaning it can have when each
database has its own eviction pass; `CONFIG SET maxkeys` applies it to all of them.
`databases` is reported by `CONFIG GET` and is read-only: a database is a keyspace with
its own shards and its own janitor, all created before serving starts, and a client may
already be `SELECT`ed into one that a shrink would take away.

## Streams

A stream is an append-only log of field/value entries addressed by `ms-seq` ids, plus the
consumer groups that track who has read what.

```bash
redis-cli -p 6380 xadd events '*' kind login user ada   # 1785915671675-0
redis-cli -p 6380 xadd events 1785915671675-5 kind logout user ada
redis-cli -p 6380 xadd events maxlen '~' 1000 '*' kind ping   # trim as you append
redis-cli -p 6380 xrange events - + count 10
redis-cli -p 6380 xrange events '(1785915671675-0' +    # exclusive start
redis-cli -p 6380 xinfo stream events
```

**Ids.** An id is a millisecond and a sequence number, compared as the pair. `*` generates
one; `<ms>-*` fixes the millisecond and generates the sequence; an explicit id must sort
after every id already in the stream. Generation never trusts the clock to increase, so an
NTP step backwards produces the *same* millisecond with the next sequence rather than an
id that would sort into the middle of the log. Because that makes `*` non-deterministic,
`XADD *` propagates the concrete id it assigned — a replica generating its own would
diverge with nothing to signal it.

**Consumer groups.** A group has a last-delivered id and a pending-entries list (PEL); a
consumer inside it has its own share of that PEL. `XREADGROUP ... >` delivers entries the
group has never seen and records them as pending; `XREADGROUP ... <id>` re-reads that one
consumer's outstanding entries without changing anything. `XACK` clears an entry;
`XCLAIM` and `XAUTOCLAIM` transfer entries idle for long enough to another consumer, which
is how work is recovered from a consumer that died.

```bash
redis-cli -p 6380 xgroup create events workers 0
redis-cli -p 6380 xreadgroup group workers w1 count 10 streams events '>'
redis-cli -p 6380 xpending events workers                    # the summary
redis-cli -p 6380 xpending events workers - + 10 w1          # the extended form
redis-cli -p 6380 xack events workers 1785915671675-0
redis-cli -p 6380 xautoclaim events workers w2 60000 0       # take over w1's stale work
redis-cli -p 6380 xinfo consumers events workers
```

**Blocking.** `XREAD BLOCK ms` and `XREADGROUP BLOCK ms` use the same machinery `BLPOP`
does, so they inherit its guarantees: a blocked client holds no lock, wakeup is FIFO per
key and comes from an exact signal on the write path rather than from polling, and inside
`MULTI` they take their non-blocking behaviour. `XREAD`'s `$` means "entries added after
this call", and it is resolved *once*, on arrival — re-resolving it on each wakeup would
move the goalpost every time and the command could never return. `XREAD` is the one
blocking command here that is not a write, so it is served by a replica; `XREADGROUP` is a
write, and propagates the pending entries it created plus the group's resulting position.

**Durability.** A snapshot (`Dump`, used by an AOF rewrite and a replica seed)
reconstructs the entries, the id counters, *and* every group with its last-delivered id and
its pending-entry list. A stream whose groups vanished on restart would silently
re-deliver acknowledged work; see the design notes for the exact command sequence.

**Trimming.** `MAXLEN` and `MINID` both remove a prefix. `~` is exact here rather than
approximate, because there are no macro-nodes to leave partially filled — which still
satisfies what `~` promises, and additionally makes a trim deterministic. `LIMIT` is
accepted and has no effect.

## HyperLogLog

`PFADD`/`PFCOUNT`/`PFMERGE` estimate the cardinality of a set in a fixed 12 KB, to about
0.81 % standard error. The implementation is the real algorithm — 16384 six-bit registers,
both the sparse and the dense encoding, MurmurHash64A with Redis's seed, and Ertl's
tau/sigma estimator — and the stored value is **byte-for-byte a Redis HLL**, so a sketch
can be moved between this server and real Redis with `GET`/`SET`:

```bash
redis-cli -p 6380 pfadd visitors:mon alice bob carol
redis-cli -p 6380 pfadd visitors:tue carol dave
redis-cli -p 6380 pfcount visitors:mon visitors:tue   # 4 — the union, without merging
redis-cli -p 6380 pfmerge visitors:week visitors:mon visitors:tue
redis-cli -p 6380 pfdebug encoding visitors:week      # sparse, until the sketch grows
redis-cli -p 6380 pfselftest                          # OK
```

Measured worst-case relative error over 200 cardinalities from 1 000 to 200 000 distinct
elements: **1.42 %**. Under a few thousand the count is essentially exact, which is what
the sigma correction buys. Two deviations from Redis's *implementation* — a canonical
sparse re-encode instead of in-place patching, and a cache field that is always written
stale — are documented in the design notes; neither changes the format, and both were
verified by round-tripping sketches through `redis:7-alpine` in both directions.

## Geospatial

A geo set is a sorted set whose score is a 52-bit geohash, exactly as in Redis, so every
sorted-set command works on it:

```bash
redis-cli -p 6380 geoadd Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
redis-cli -p 6380 geodist Sicily Palermo Catania km          # 166.2742
redis-cli -p 6380 geohash Sicily Palermo                     # sqc8b49rny0
redis-cli -p 6380 geopos Sicily Palermo
redis-cli -p 6380 geosearch Sicily fromlonlat 15 37 byradius 200 km asc withdist withcoord
redis-cli -p 6380 geosearch Sicily frommember Palermo bybox 400 400 km asc
redis-cli -p 6380 geosearchstore nearby Sicily fromlonlat 15 37 byradius 200 km asc
redis-cli -p 6380 zcard Sicily                               # 2 — it is a sorted set
```

`GEODIST` is the great-circle distance on a sphere of Redis's radius (6 372 797.560856 m),
which is why Redis documents up to 0.5 % error against a geodesic; the value this server
reports agrees with Redis's to the four decimal places both print. `GEOSEARCH` resolves its
circle or box to nine geohash cells, queries them as score ranges under one lock, and then
filters by real distance, so a `BYRADIUS` result is a circle and not the rectangle the
cells describe.

`GEOSEARCHSTORE` stores geohashes by default — so the destination is itself a geo set that
can be searched again — or distances with `STOREDIST`, which makes it an ordinary sorted
set ordered by proximity.

`GEORADIUS`, `GEORADIUSBYMEMBER` and their `_RO` forms are the pre-6.2 spelling of the same
search, and they are here because a decade of client code sends them. They are the same
search over a different argument layout, sharing one implementation with `GEOSEARCH` rather
than duplicating it — with one difference worth naming: their `STORE` and `STOREDIST` each
take a *key operand*, where `GEOSEARCHSTORE`'s `STOREDIST` is a bare flag. The `_RO` forms
are the same commands with both refused, which is what lets a client send a radius search to
a replica.

```bash
redis-cli -p 6380 georadius Sicily 15 37 200 km withdist asc
redis-cli -p 6380 georadiusbymember Sicily Palermo 200 km count 1
redis-cli -p 6380 georadius Sicily 15 37 200 km storedist by_distance
```

## Observability

```bash
# the slow log: microseconds, negative disables, 0 logs everything
redis-cli -p 6380 config set slowlog-log-slower-than 5000
redis-cli -p 6380 config set slowlog-max-len 256
redis-cli -p 6380 slowlog get 10        # id, timestamp, duration, args, addr, name
redis-cli -p 6380 slowlog len
redis-cli -p 6380 slowlog reset

# the latency monitor: milliseconds, 0 disables
redis-cli -p 6380 config set latency-monitor-threshold 100
redis-cli -p 6380 latency latest        # event, last timestamp, last ms, max ms
redis-cli -p 6380 latency history command
redis-cli -p 6380 latency histogram get set   # per-command cumulative distribution
redis-cli -p 6380 latency reset

# per-command statistics and approximate percentiles, both excluded from a bare INFO
redis-cli -p 6380 info commandstats
redis-cli -p 6380 info latencystats
redis-cli -p 6380 config resetstat

# the cache hit rate, and one key's footprint
redis-cli -p 6380 info stats | grep keyspace_
redis-cli -p 6380 memory usage mykey
redis-cli -p 6380 memory doctor

# the keys a command would touch, for the ones whose keys are not at a fixed position
redis-cli -p 6380 command getkeys xread count 2 streams s1 s2 0 0   # s1 s2
redis-cli -p 6380 command getkeys bitop and dst a b                 # dst a b

# every command every client runs, live
redis-cli -p 6380 monitor
```

Four things are worth knowing about this surface:

- **It is free when unused.** Per-command statistics are three atomic adds on the command
  table entry (no map lookup, no lock, no allocation); the slow log, the latency monitor
  and the `MONITOR` feed are each one atomic load away from doing nothing at all. An
  `AllocsPerRun` test pins it.
- **A monitor never sees a credential.** `AUTH`'s arguments, `HELLO ... AUTH`'s and
  `CONFIG SET requirepass|masterauth`'s are replaced with `(redacted)`.
- **A slow monitor is dropped, not waited for.** Each monitor has a bounded queue drained
  by its own goroutine; one that overflows is disconnected, the same contract the replica
  feeds and Pub/Sub subscribers use.
- **Blocked time is not slow time.** A `BLPOP` that waited five seconds for a push does
  not enter the slow log: the wait is subtracted before the threshold is tested, because
  waiting for a client is not work the server did slowly.
- **`LATENCY HISTOGRAM` and `INFO latencystats` read the same measurements.** There is one
  histogram per command — 64 power-of-two buckets on the table entry — and the two commands
  are two views of it: percentiles for `INFO`, the cumulative distribution for `LATENCY
  HISTOGRAM`. Redis's buckets are `hdr_histogram`'s and so are finer; both are
  approximations, and reporting this one's real boundaries beats interpolating onto Redis's
  and implying a precision the data does not have.

`MEMORY USAGE` is an estimate and is documented as one — Go offers no portable way to ask
the allocator how large a live object graph is — but it is exact about the payload, which
is the part that dominates and the part an operator is looking for when hunting the key
that grew. `MEMORY DOCTOR` reports what this server can actually diagnose (key counts, the dataset's
size, the budget and policy in force, and — when writes are being refused — *why*, which
under a `volatile-*` policy with no TTLs anywhere is the difference between a diagnosis and a
mystery) and says plainly which of Redis's checks it cannot perform, rather than inventing
findings from no evidence.

`MEMORY STATS` follows the same rule, and the interesting part of it is what is *missing*.
It reports the twelve fields this server can state truthfully, under Redis's names and in
Redis's order: `total.allocated` (the dataset, the same number `INFO`'s `used_memory`
reports — Redis's own `total.allocated` is its `used_memory`, so putting anything else under
the name would be two numbers wearing one label), `replication.backlog` (the bytes the backlog ring retains, as `INFO`'s
`repl_backlog_histlen`), `clients.normal`/`clients.slaves` (the sums of exactly the `tot-mem`
`CLIENT LIST` publishes for those connections), `cluster.links`, `lua.caches` and
`functions.caches` (0 because there is no cluster bus, no Lua and no functions — the
measurement, not a placeholder), a `db.N` sub-map per non-empty database carrying
`overhead.hashtable.main`, then `overhead.total`, `keys.count`, `keys.bytes-per-key` and
`dataset.bytes`, where `dataset.bytes` is the sum of the same per-key estimate
`MEMORY USAGE` reports so the two commands cannot give different answers.

Redis's other seventeen fields are **omitted rather than filled in**. `peak.allocated` and
`startup.allocated` would need a high-water mark of live heap bytes and a heap size recorded
at startup, neither of which Go exposes; `dataset.percentage` and `peak.percentage` are
ratios over those two denominators; `aof.buffer` would need the AOF's unwritten byte count,
where 0 is right under `appendfsync always` and false under `everysec`; and the whole
`allocator.*`, `allocator-fragmentation.*`, `allocator-rss.*`, `rss-overhead.*` and
`fragmentation` group describes a jemalloc this server does not have. A missing field says
"this server cannot tell you". A fabricated `fragmentation` ratio would say something false
about the process, which is the one thing an observability reply must never do.

## Memory and eviction

Every operator sizes a cache in bytes — container limits, alerts and capacity plans are all
expressed that way — so `maxmemory` is a real budget here and `used_memory` is a real number.

```bash
redis-cli -p 6380 CONFIG SET maxmemory 2gb
redis-cli -p 6380 CONFIG SET maxmemory-policy allkeys-lru
redis-cli -p 6380 INFO memory | grep -E 'used_memory:|maxmemory'
redis-cli -p 6380 INFO stats  | grep evicted_keys
```

### What `used_memory` counts

It is a running total, maintained as values are written, of what the dataset occupies: for
every key the keyspace holds, the entry struct and the keyspace map's slot for it, the key's
own bytes, and the value's payload measured exactly as `MEMORY USAGE` measures it. The two
are the same estimator, so `used_memory` equals the sum of `MEMORY USAGE` over every key —
not a second opinion about it.

It does **not** count the Go runtime's own footprint, the allocator's slack, the per-shard
map's spare capacity, the replication backlog, client input/output buffers, the AOF's buffer,
or goroutine stacks. Redis excludes replica output buffers from its own `maxmemory`
arithmetic for the same reason — a budget you cannot meet by evicting a key is not a budget
eviction can meet — and includes the rest because it measures at the allocator, which Go
offers no portable way to do. The Go heap is still reported, under its own name
(`used_memory_go_heap`), because it is a real and useful number; it is just not the dataset,
and a budget compared against it would have eviction chasing the garbage collector.

A key whose deadline has passed but which nothing has reclaimed yet is still counted: the
bytes are still resident. It stops being counted the moment a read, the sweep, or eviction
removes it.

### Why a maintained counter, and how it is kept honest

The size of a value is a property of the value, so learning it means looking at it — which
is what `MEMORY USAGE` and `MEMORY STATS` do, and why both are O(the thing they measure).
A byte budget cannot be enforced that way: "are we over the limit?" is asked before every
write, and answering it with a walk of the keyspace would make every write O(keyspace).

So the total is maintained. The difficulty of a maintained total is that it must be right
across *every* path that changes a value's size — including the ones that change it in place
(`APPEND`, `SETRANGE`, `SETBIT`, `BITFIELD`, the HyperLogLog commands), every collection
insert and removal, and expiry and eviction. A counter updated by hand at each of ninety-odd
mutation sites is a counter that will be wrong the first time a site is added and nobody
notices.

The shape is chosen so "did you remember?" is not a question a reviewer has to ask. A
mutating method does not compute a delta; it declares that it is about to change a key, and
the accounting measures the difference itself:

```go
sh.mu.Lock()
charged := s.charge(sh, key)
defer sh.mu.Unlock()
defer s.settle(sh, key, charged)
```

Insert, overwrite, in-place growth, shrink and delete are therefore one case rather than
five, and a method cannot be half-instrumented: either it declared the key it changes or it
did not. Each container carries its own byte count (`deque.bytes`, `zset.bytes`,
`entry.elemBytes`) so that asking a million-field hash how large it is costs nothing —
otherwise `HSET` would become O(n).

What proves it is `TestMemoryAccountingDoesNotDrift`: twenty thousand randomised mutations
drawn from every mutating store method, interleaved with expiry sweeps and eviction passes,
after which every entry's maintained size is compared against a walk of that entry and the
total against a full recomputation. Per entry, not only in total — a total that agrees by
luck, one key over and another under, is not an accounting anyone can reason about. Measured
drift: **zero bytes**.

**The accounting is off until something asks for it.** A server with no byte budget that
nobody has asked about pays one atomic load per mutation and nothing else: no map lookup, no
arithmetic, no counter. Setting `maxmemory`, or the first read of `used_memory`, switches it
on and derives the totals from the dataset, so the counter never starts from a partial
history. That is invariant 12's rule rather than a shortcut — an observer that is not
watching costs nothing, and whoever asks to watch pays for it.

**One documented gap.** A stream's payload is refreshed by the maintenance pass rather than
at the moment it changes, because the stream mutation paths live in a file this accounting
does not instrument. So `used_memory` lags a stream-only write burst by at most one sweep
interval (`-sweep`, one second by default) and converges exactly; every other type is exact
at every instant. `TestMemoryAccountingConvergesForStreams` pins both halves of that
statement, so closing the gap would fail loudly rather than pass silently.

### The policies

All eight of Redis's, and the difference between the two families is not which comparison
they make but which keys are *candidates*: an `allkeys-*` policy may evict anything, a
`volatile-*` policy may only evict a key carrying a TTL.

| policy | victim |
| --- | --- |
| `noeviction` | nothing; writes that could grow the dataset are refused |
| `allkeys-lru` / `volatile-lru` | the oldest access time among the sample |
| `allkeys-lfu` / `volatile-lfu` | the lowest access frequency among the sample |
| `allkeys-random` / `volatile-random` | any candidate |
| `volatile-ttl` | the nearest deadline among the sample |

Eviction is approximate, as Redis's is: `maxmemory-samples` keys are examined and the best
candidate among them is taken, because keeping a global access order would mean touching
shared state on every read — the exact cost the sharded keyspace exists to avoid. What is
*not* approximate is which keys are eligible. A `volatile-*` policy never takes a key with no
TTL, and "there is nothing to evict" is a fact rather than a failed guess: every shard is
visited, and a per-shard count of volatile keys rules out a hopeless shard in O(1) so the
only shards walked in full are ones that really do hold a candidate. An earlier version drew
random shards instead, which missed a lone volatile key's shard often enough to refuse writes
while a key it was allowed to take was sitting there.

The LFU counter is Redis's: an 8-bit logarithmic counter with a decay, so a key that was hot
an hour ago does not outrank one that is hot now, and a new key starts at 5 rather than 0 —
starting at zero would make every freshly written key the most attractive victim in the
keyspace, so an LFU policy would evict exactly what the workload had just started using.
`OBJECT FREQ` reads the counter back. The growth is deliberately slow: measured on redis 7.2,
100 reads of a key move `OBJECT FREQ` from 5 to 6 and 10 000 reads take it to 19.

### When nothing can be evicted

`noeviction`, and a `volatile-*` policy over a keyspace with no volatile keys, both end in a
refusal rather than a search — the second especially, because "keep looking for a candidate"
is an infinite loop with a client waiting on it. The refusal is Redis's, byte for byte:

```
OOM command not allowed when used memory > 'maxmemory'.
```

Only the commands that could grow the dataset get it. Reads keep working, so the problem
stays visible to whatever is monitoring; and `DEL`, `UNLINK`, `GETDEL`, `EXPIRE`, the pops
and the removals keep working, because deleting something is the operator's only way out of a
full keyspace. Which commands those are is not reconstructed from a rule — the rule has edges
that are not guessable (`LSET` is refused because it can store a longer element, `SMOVE` is
not; `SETEX` is, `GETEX` is not; `SORT` is, `SORT_RO` is not) — so the classification is read
out of `COMMAND INFO` on redis 7.2 per command, and `COMMAND INFO` here reports the same
`denyoom` flag the gate acts on. A write with no classification fails a test rather than
silently escaping the budget.

Inside `MULTI` the gate is stricter, and that is measured too: queuing is itself unbounded
memory growth, so every queued command is refused — a `GET` included — and `EXEC` answers
`EXECABORT`. `DISCARD` always works, so the batch can be abandoned.

Redis evicts what it can and *then* refuses if that was not enough, and so does this: with
300 keys, a 1 kB budget and one key given a TTL under `volatile-lru`, redis 7.2 evicts that
one key, reports `evicted_keys:1`, and still answers the `SET` with `OOM`. Freeing one key
does not bring 300 of them under a kilobyte.

### Eviction is a write

It is ordered, persisted and replicated like any other, and it propagates as the `DEL` of the
key that went rather than as anything about the policy. That is invariant 4: the choice of
victim is not reproducible, so a replica running the same policy over the same keyspace would
sample different keys and the two datasets would diverge while both looked internally
consistent.

A replica therefore does **not** evict on its own account, even with a budget of its own —
its master drives what it holds, and the master's eviction arrives as a `DEL`. Redis draws
the same line (`replica-ignore-maxmemory`, on by default). The enforcement is on the client
path only: an AOF replay and a master's stream apply every write they are given whatever this
server's limit says, the same reasoning invariant 13 applies to cluster redirects.

Because the removal hook takes `propMu` to propagate that `DEL`, eviction runs *before* the
command that made room for it rather than inside `runWrite` — `propMu` is not reentrant, and
a write holds it across both its mutation and its propagation. That ordering is also the one
the stream needs: every `DEL` for an evicted key precedes the command whose arrival caused it.

### `maxkeys` is still here

A byte budget did not retire it. `maxkeys` bounds the *number* of keys, is enforced by the
janitor rather than on the write path, and only ever evicts — it never refuses a command,
because a cap is an instruction to bound the keyspace and answering it with `OOM` errors
would silently retire a documented feature. So a cap evicts by the configured policy when
that policy evicts at all, and by approximate LRU when the policy is `noeviction`. It applies
per database; `maxmemory` is server-wide, summed across every database, because a limit each
of sixteen keyspaces enforced separately would be sixteen times the limit the operator set.

## Pub/Sub

```bash
redis-cli -p 6380 subscribe news              # blocks, receiving messages
redis-cli -p 6380 psubscribe 'news.*' '*.eu'  # glob patterns, same matcher as SCAN
redis-cli -p 6380 publish news.eu hello       # (integer) 2 — one per subscription
redis-cli -p 6380 pubsub numsub news          # news 1
```

A subscribed connection enters RESP2 **subscriber mode**: only the subscribe family
plus `PING`, `QUIT` and `RESET` are accepted, because RESP2 has no separate push type
and a subscribed client cannot tell an ordinary reply apart from a delivered message.
`PUBLISH` counts one receiver per matching *subscription*, so a client subscribed both
directly and by pattern is counted — and delivered to — twice, as in Redis.

**A slow subscriber never blocks a publisher.** Each subscriber owns a bounded queue
drained by its own goroutine. A publisher that finds a queue full disconnects that
subscriber instead of waiting: `PUBLISH` has no acknowledgement, so blocking the
publisher — and with it the connection it runs on, and the write-ordering lock when the
publish is inside a transaction — to serve one client that stopped reading trades a
whole server for a single connection. The connection is closed by the *publisher*
rather than left to the subscriber's pump, because the reason a subscriber falls behind
is a full socket, which is exactly where its pump is blocked and unable to notice a
signal. A closed connection is also the only thing RESP2 can say: there is no way to
tell a client "you missed messages", so a visible disconnect it can reconnect and
resubscribe from beats leaving it subscribed and silently short. `INFO` counts it as
`pubsub_dropped_subscribers`.

**Pub/Sub across replication.** A `PUBLISH` on a master is delivered to its own
subscribers *and* streamed to its replicas, so a client subscribed to a read replica
sees messages published anywhere in the tree — otherwise "subscribe to a replica", the
whole reason to have replicas serve reads, would silently lose events. It is never
written to the AOF: a message has no place in a log whose purpose is to reconstruct the
dataset, and replaying one at startup would deliver it to subscribers that had not yet
subscribed when it was sent. The integer `PUBLISH` returns counts *local* receivers
only, as in Redis: a number including replicas' subscribers is not something the
publisher could act on, since it cannot know when the message got there.

Forwarding is a property of the stream, not of the command: a master enqueues
client-originated publishes, and a replica relays what arrives on its replication link
(so a chain converges) but does not relay a publish a client sent directly to it —
doing both would deliver every message twice downstream. A message is also not part of
any transaction: the applier delivers a `PUBLISH` on arrival rather than buffering it
into an open `MULTI` group, for the same reason it drops the keepalive there.

## Keyspace notifications

```bash
go run ./cmd/shardkv -notify-keyspace-events KEA
redis-cli -p 6380 psubscribe '__keyevent@0__:*'
redis-cli -p 6380 psubscribe '__keyevent@*'      # every database
```

With the flags enabled, every change publishes on two reserved channel families:
`__keyspace@<db>__:<key>` carrying the event name, and `__keyevent@<db>__:<event>`
carrying the key. The database is part of the name because the channels themselves are
global — Pub/Sub has no databases — so a consumer of database 0's events must not be
handed an event about the same key name in database 3. They are the same fact transposed, so a consumer can filter on whichever of
the two it cares about. The flag characters are Redis's — `K` keyspace, `E` keyevent,
`g` generic, `$` string, `l` list, `s` set, `h` hash, `z` sorted set, `t` stream,
`x` expired, `e` evicted, `A` all classes — and an unrecognized character is rejected rather than
ignored, so a typo cannot leave an operator waiting for events that never arrive. A
specification with no `K` or `E` names events with nowhere to send them and therefore
disables notifications, as in Redis. Runtime changes go through
`CONFIG SET notify-keyspace-events`.

The stream events were checked command by command against a live `redis:7-alpine` with
`--notify-keyspace-events KEA` rather than derived from the documentation, and the two
servers emit the identical sequence. That check is what caught the surprising part:
`XREADGROUP`, `XACK`, `XCLAIM` and `XAUTOCLAIM` fire no event of their own — the only
thing a group read or a claim reports is an `xgroup-createconsumer` for a consumer it
created implicitly. An event that existed here and not there is exactly the kind of
difference a consumer of notifications would build on and then be surprised by.

Expirations and evictions come from the store's removal hook, which is the only place
that learns about a key nothing ever read again. Notifications are deliberately local:
each node reports the changes it applies (a replica fires events for the writes it
receives and for its own expirations), so they are neither replicated nor persisted —
shipping them would double every event for a client subscribed to a replica.

**Disabled, it costs nothing.** The check on the write path is one atomic load of a
bitmask, tested before any string is built: with no classes enabled the hook does not
even upper-case the command name. A test asserts zero allocations per call for both the
command hook and the expiry hook.

## Authentication and TLS

```bash
go run ./cmd/shardkv -requirepass s3cret
go run ./cmd/shardkv -replicaof 127.0.0.1:6380 -masterauth s3cret
```

An unauthenticated connection may run only `AUTH`, `HELLO`, `PING`, `QUIT` and
`RESET`; everything else gets `NOAUTH Authentication required.`. `AUTH password` and
`AUTH username password` are both accepted (the only user is `default`, since there is
no ACL system here — any other username is refused rather than silently treated as
`default`). The comparison is constant-time, because a compare that returns early
leaks how much of a guess was right and turns an offline-strength secret into an
online per-byte search. The password is never logged.

`PSYNC` is gated too. It never reaches the normal command path — the connection loop
diverts it into a replication feed — so without an explicit check there, asking for a
snapshot of the entire dataset would have been a way around the password.

A replica presents `-masterauth` on **every** connection, reconnects included: one that
authenticated only the first time would come back after any blip and be refused, then
retry forever while serving data that never advances again.

```bash
# listener TLS, and (independently) TLS for the connection to a master
go run ./cmd/shardkv -tls-cert server.pem -tls-key server-key.pem -tls-ca ca.pem
go run ./cmd/shardkv -replicaof host:6380 -tls-replication \
    -tls-cert client.pem -tls-key client-key.pem -tls-ca ca.pem
```

Plain TCP is the default: the listener is wrapped only when a certificate is supplied.
A certificate without its key (or the reverse) is an error rather than a silent
fallback — an operator who configured half of TLS asked for TLS, and quietly serving
unencrypted traffic on the port they meant to protect is the one outcome they cannot
detect. Client certificates are requested but not required (`-tls-ca` exists so a
replica can verify its master; demanding mutual TLS from every `redis-cli` would make
the option unusable). A replica *always* verifies its master's certificate: one that
accepted any certificate would accept any host claiming to be the master, and then
apply that host's write stream to its dataset.

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

Two edges of that rule are worth stating, because both are places a naive implementation
silently gets wrong:

- **A watched key that merely *expired* also aborts**, and it has to be checked at `EXEC`
  rather than reported by an event. A key past its deadline is already invisible to every
  read, but nothing has removed it yet, so there is no write to invalidate the `WATCH` with
  until a read or the janitor gets to it. `EXEC` therefore re-tests every key that was live
  when it was watched. Redis checks the same thing at the same moment, for the same reason.
- **A flush invalidates only the keys that were there.** `FLUSHALL` and `FLUSHDB` abort a
  transaction watching a key they actually emptied, and leave one watching an absent key
  alone — otherwise `FLUSHALL` would be a way to fail every open transaction on the server
  regardless of what it touched.

## Eviction

```bash
go run ./cmd/shardkv -maxkeys 100000
```

With `-maxkeys` set, a background pass keeps the store near the cap by evicting
approximately-least-recently-used keys: it samples up to 16 keys from a random
shard and removes the oldest, repeating until under the cap. Access time is
tracked only when eviction is enabled, so the default (unbounded) configuration
pays nothing on the read path. `INFO` reports `evicted_keys`, the cap is readable and
writable at runtime with `CONFIG GET|SET maxkeys`, and an eviction publishes an
`evicted` keyspace notification when the `e` class is enabled.

The pass measures how far over the cap it is by summing each shard's map length —
O(shards), not the O(live keys) scan `DBSIZE` does — because it runs on every
janitor tick. The estimate counts keys whose TTL elapsed but which nothing has
reclaimed yet; the same tick sweeps expired keys immediately beforehand, so at the
point it is read the approximation is exact. That keeps the eviction check off the
write path entirely: no per-write counter to maintain for a bound that is
documented as approximate anyway.

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
  when a write actually modified state — propagates the command to the AOF and
  replicas. Durability and replication are therefore a property of the table,
  not scattered through handlers.
- **Three kinds of handler, one table.** A `write`/read handler gets the store; an
  *effect* handler gets to say what it did (below); a *session* handler gets the
  connection. The split is deliberate: `AUTH`, `SUBSCRIBE`, `CLIENT`, `RESET`,
  `SELECT`, `QUIT`, `SHUTDOWN` and the transaction commands act on one socket, and
  threading the session through all ~120 dataset handlers would hand every one of them
  the power to mutate connection state it has no business touching. Registering the
  transaction commands in the table rather than special-casing them in the dispatcher
  is also what makes `COMMAND COUNT`/`INFO` truthful — a client that cannot find
  `MULTI` concludes the server has no transactions.
- **Sharded reads, ordered writes.** Reads scale across shards (per-shard
  `RWMutex`). Writes also take the shard lock, but when propagation is active
  they additionally pass through one ordering lock spanning mutation + AOF append
  + replica enqueue — the single-writer model real systems use to keep copies in
  agreement. That lock is skipped entirely for a pure cache, so the default
  config keeps concurrent-write throughput.
- **Absolute deadlines on the wire.** Relative-TTL writes are rewritten to
  absolute (`PEXPIREAT`, `SET ... PXAT`, `RESTORE ... ABSTTL`) before they reach
  the AOF/replicas, and the master synthesizes a `DEL` when it expires/evicts a
  key, so replicas and replayed AOFs don't drift on TTL boundaries. The rewrite is
  built from the deadline the handler *already resolved and already wrote to
  memory* — one clock reading per command, with no clock anywhere on the
  propagation path — so the two instants are the same value, not two computations
  of it. Reading the same clock twice is not sufficient and was the earlier bug:
  the second reading happened after the handler ran, so a replica's copy of a key
  outlived the master's by however long the write took (74 ms with a large value).
- **Chunked snapshots.** `Dump` emits a large collection as a run of bounded
  commands (the first creates the key, the rest append, the `PEXPIREAT` comes
  last) instead of one command per key. One command per key is not merely
  wasteful: past ~1M elements the RESP array exceeds the protocol's multibulk
  limit and the reader rejects the *whole* stream, so an AOF rewrite or replica
  seed would lose the dataset rather than one key. Real Redis chunks for the same
  reason.
- **Ordered locking for multi-key writes.** `COPY`, `SMOVE`, `LMOVE`,
  `RPOPLPUSH`, `RENAMENX` and the `*STORE` combinators touch two or more keys, so
  they take more than one shard lock. They take them in ascending *shard index*
  order, never in argument order: `COPY a b` and `COPY b a` running concurrently
  would otherwise each hold the lock the other is waiting for. Sorting by shard
  index also handles the cases no ordering over the key names survives — two keys
  hashing to the same shard (one lock, not two) and a destination that is also a
  source. The multi-key reads (`SINTER` and friends) take the same order in shared
  mode, which makes the result a consistent cut of every input rather than a
  stitch of several instants.
- **Propagate the effect when the command isn't replayable.** The `write` flag
  ships a command verbatim, which is right whenever the arguments determine the
  outcome. They don't for `SPOP`, `ZPOPMIN`/`ZPOPMAX` (a tie at the boundary score
  is broken by the index), or the float increments (whose result would depend on
  the replica reproducing the same arithmetic). Those register an *effect*
  handler instead: the table calls it, and what reaches the AOF and the replicas is
  the `SREM`/`ZREM`/`SET` describing what actually happened. The random *reads* —
  `RANDOMKEY`, `SRANDMEMBER`, `HRANDFIELD`, `ZRANDMEMBER` — need nothing, because a
  read propagates nothing at all.
- **Optimistic locking, not a global lock.** `WATCH` registers a key→sessions
  map; any write that modifies an affected key marks watching sessions dirty
  (via an atomic flag), so `EXEC` can detect a conflict without serializing the
  whole keyspace. The key list comes from `affectedKeys`, which every multi-key
  write has to be registered in by name: the default of "the first argument" would
  silently miss a `COPY`'s or an `SMOVE`'s destination, and a transaction guarding
  that key would commit on top of a change it was watching for.
- **Pointer entries for LRU.** Values are stored as `*entry` so a key's access
  time can be updated atomically on the read path, under a shared read lock,
  without upgrading to an exclusive lock. The cost is one allocation per write;
  a `sync.Pool` or arena would remove it if write throughput dominated.
- **Value copies.** Stored bytes are copied in and out so callers can never
  mutate state through an aliased slice.
- **Injectable clock.** The store reads time from a function field, so TTL logic
  is tested deterministically instead of with sleeps. It is the *only* clock in
  the write path: the server reads "now" through `Store.Now` rather than calling
  `time.Now` itself, so an injected clock governs the propagated deadline exactly
  as it governs the stored one. Two clocks in one code path is two answers to the
  same question.
- **One lookup on the read path.** `GET` asks the store a single question —
  "the string at this key, or a type error" — under one acquisition of the shard
  lock, rather than checking `TYPE` and then reading. Two lookups both cost twice
  and, worse, can straddle a concurrent write and answer from two different
  states.
- **Bounded queues everywhere, and a drop rather than a stall.** A replica feed, a
  subscriber queue, and the replication backlog are all bounded, and all three answer an
  overflow the same way: never make the whole server wait for one slow consumer, and
  never silently skip what that consumer missed. A replica is disconnected so it resyncs
  (partially, if the backlog still holds its offset); a subscriber is disconnected so it
  resubscribes. The failure is visible in both cases, which is the property that makes it
  safe.
- **One encoder for the wire, counted once.** The bytes counted into the replication
  offset, stored in the backlog, and appended to the AOF all come from a single size
  function in `internal/resp`. Two independent size formulas would eventually disagree
  with the bytes actually written, and a continuation that resumed one byte off would
  hand a replica a stream it can only reject. A test encodes commands both ways and
  compares.
- **The protocol version lives on the writer, and the fallback lives with it.** A
  connection's RESP version is a property of one socket, so it is stored on that
  socket's writer — an AOF replay, a replica feed and another connection's `CLIENT LIST`
  cannot be affected by what one client negotiated. Each RESP3 type is written by a
  method that falls back to the RESP2 encoding of the *same value*, so there is exactly
  one place that decides, and it is the place that knows the version. A handler states
  what a reply *is* — a map, a set, a run of pairs — and the encoding follows; the
  alternative, a version check at each of the forty-odd places a collection is written,
  is how a RESP2 client ends up receiving a stray `%` after someone adds a command and
  copies the wrong branch. The attribute type is the sole exception, because it has no
  RESP2 encoding at all, so its one caller checks the version itself.
- **Follow the server, not the specification, where they disagree.** RESP3's
  documentation suggests `EXISTS` and `SISMEMBER` would use the boolean type and that a
  float reply would use the double type. Redis 7 replies with integers to the first and
  with a bulk string to `INCRBYFLOAT`, so this server does too: the point of wire
  compatibility is the clients people actually run. Every RESP3 encoding in the tests was
  captured from `redis:7-alpine` rather than derived from the specification.
- **The reply writers report no error.** Writes go into a `bufio.Writer` whose error is
  sticky, so a failure part-way through a reply surfaces at the `Flush` the connection
  loop already checks. A return value that every call site has to ignore is worse than no
  return value; the streaming methods (`WriteCommand`, `WriteRaw`, `Flush`) do return
  errors, because their callers act on them by ending the stream.
- **A blocked client is a parked goroutine that owns no lock.** The wait holds no shard
  lock, not `propMu`, and not the connection's writer lock; wakeup is a non-blocking
  signal from the write path, and only the head of a key's queue is woken, which is
  where FIFO fairness comes from. Fairness by waking only the head is cheaper *and*
  stricter than waking everyone and letting them race. See
  [Blocking commands](#blocking-commands).
- **A database is a keyspace, not a dimension.** Each database has its own shards, so
  databases cannot contend; the store never learns that databases exist. A `Server` value
  pairs the shared state with one database, and a connection resolves to one of a fixed
  set of such views per command, which is why `SELECT` cost no change to 135 handler
  signatures. See [Databases](#databases).
- **The database belongs in the stream, emitted lazily.** A `SELECT n` precedes a write
  only when the stream's position changes, so a single-database workload ships exactly
  the bytes it always did and an older AOF still replays. A snapshot returns the replayer
  to the shared stream's position, because the position is shared by every replica while a
  snapshot goes to one.
- **A configuration surface, not a key/value store.** `CONFIG` covers exactly the
  settings that exist, each read from whatever owns it. Accepting and remembering
  arbitrary parameters would let `CONFIG GET` report settings the server does not
  implement — worse than reporting nothing, because an operator would then tune a number
  that changes no behaviour. Settings fixed at startup are refused by `CONFIG SET`
  rather than accepted and ignored.
- **`SHUTDOWN` is a hook, not an `os.Exit`.** It cancels the serve context, so the
  process unwinds through the same path a signal takes and the deferred close of the AOF
  flushes its buffered tail. Exiting from inside the handler would skip that and discard
  writes a client was told were durable.
- **A stream id is a pair, and it never goes backwards.** An id is `ms-seq`: a
  millisecond and a counter that separates entries added inside the same millisecond.
  Generation never trusts the clock to increase — it takes the clock's millisecond only
  when that is *strictly greater* than the last id's, and otherwise keeps the last id's
  millisecond and increments the sequence. So an NTP step, a restored VM snapshot or an
  operator setting the date cannot produce an id that sorts before an id already in the
  stream. Ids can then lead the wall clock until it catches up, which is the trade Redis
  makes and the only one available: refusing the write would turn a clock correction into
  an outage.
- **Streams keep their entries in a sorted slice, not a tree.** Ids are generated in
  increasing order, so the slice is sorted *by construction* and an append is O(1)
  amortized; a range binary-searches its start and walks (O(log n + k)). `XDEL` is the
  only operation that pays — an O(n) memmove — which is the right way round for a type
  that is appended to constantly and deleted from rarely. It buys a structure with no
  per-entry pointer overhead and no rebalancing, where Redis needs a radix tree of
  listpacks to reach the same asymptotics.
- **A stream's consumer groups are data, so a snapshot carries them.** A stream's
  entries are not the whole of its state: the rest is the id counters no entry records
  and the consumer groups — each group's last-delivered id and each consumer's
  pending-entry list, which is exactly the record of *work in flight*. A snapshot that
  emitted only the entries would silently re-deliver acknowledged work, or silently drop
  outstanding work, on every restart and every replica sync, with no error anywhere. So
  `Dump` emits, per stream: one `XADD` per entry, then `XSETID ... ENTRIESADDED ...
  MAXDELETEDID ...`, then per group an `XGROUP CREATE ... ENTRIESREAD`, its
  `XGROUP CREATECONSUMER`s, and one `XCLAIM ... TIME ... RETRYCOUNT ... FORCE JUSTID`
  per pending entry. One command per entry rather than per 256, because each entry names
  its own id and so cannot share an `XADD` — and every emitted command is therefore far
  inside the protocol's multibulk limit. (A pending entry whose data has since been
  `XDEL`ed is not reconstructed: `XCLAIM` drops an id that is not in the stream. Redis's
  own rewrite has the same property, for the same reason — there is nothing to hand a
  consumer.)
- **Approximate stream trimming is exact here, and that is still `~`.** Redis's `~`
  exists because its entries are packed into macro-nodes it will not split, so it stops
  at a node boundary and can leave more entries than asked. This implementation has no
  macro-nodes, so `~` removes exactly the prefix `=` would. That satisfies the contract
  `~` states ("at least this many are retained") as well as the stronger one `=` states,
  and it means a trim is deterministic — which is why `XADD`'s effect can ship an exact
  `XTRIM` instead of the `~` the client sent. `LIMIT` is accepted and has no effect,
  because it bounds the work an approximate trim will do and an exact prefix removal has
  no unbounded work to bound.
- **HyperLogLog is Redis's format, byte for byte.** The stored string is a real Redis
  HLL: the 16-byte `HYLL` header, 14-bit register addressing (16384 registers), the same
  six-bit dense packing, the same sparse `ZERO`/`XZERO`/`VAL` opcodes, MurmurHash64A with
  Redis's `0xadc83b19` seed, and Ertl's tau/sigma estimator (what Redis has used since
  5.0, and what replaced the old bias-correction lookup table). Portability is the whole
  point: a sketch is the one value a client is likely to want to move between servers, so
  a "Redis-compatible" server whose HLL strings were private would be a trap. Verified by
  building the same sketch on this server and on `redis:7-alpine`, comparing the bytes,
  and then loading each into the other and counting — identical registers and identical
  counts, in both the sparse and the dense encoding. **Two documented deviations, both
  invisible to a client:** (1) a sparse update decodes to a register array and re-encodes
  *canonically* rather than patching the opcode runs in place, so the same additions in
  a different order give byte-identical values here where Redis would agree only on the
  count — the format is unchanged and Redis reads the output, the cost is O(16384) per
  sparse update instead of O(sparse length), which is bounded because sparse is only used
  while the sketch is small; and (2) the cached-cardinality field is always written
  *stale*, because computing it on a write would put an O(16384) pass on `PFADD` and
  computing it on a read would make `PFCOUNT` modify what it is reading — which on this
  server would either write on a replica or leave master and replica holding different
  bytes for the same sketch. Redis's own `PFADD` invalidates the cache the same way, so a
  Redis client reading our string simply recomputes.
- **A geo set is a sorted set, and a search is nine score ranges.** `GEOADD` is a `ZADD`
  whose score is a 52-bit geohash — 26 bits per coordinate, which is exactly the largest
  integer a float64 score holds without loss. Interleaving the two coordinates' bits is
  what makes the score useful: points sharing a long prefix are close in *both*
  dimensions, so a contiguous score range is a rectangular cell. A search picks a cell
  size that comfortably contains the query, takes that cell and its eight neighbours
  (a query smaller than a cell can still straddle a corner), turns each into a score
  range, and filters the candidates by real distance — so the answer is a circle and not
  a rectangle. All nine ranges are answered under **one** acquisition of the shard lock,
  because a concurrent `GEOADD` between two separate range queries could move a member
  from a scanned cell into an unscanned one and the search would miss a member that was
  present throughout.
- **`keyspace_hits`/`keyspace_misses` are counted in the store's *read-only* lookup, not
  in the shared one.** Redis counts them in `lookupKeyRead`; this store's `liveEntry`
  resolves keys for reads *and* writes, so a counter there would report an `LPUSH` that
  created a list as a cache miss and an `HSET` that overwrote a field as a cache hit —
  turning the hit *rate* into a number no operator could act on. The counting therefore
  lives in a second helper, `readEntry`, used only by methods that hold the shard's
  *shared* lock: "this is a read" becomes a structural property rather than a convention,
  because a write cannot reach that function without first taking the exclusive lock the
  read methods do not take. The alternative — counting in each read handler in the server
  — would put the decision a layer away from the map probe that actually makes it, and
  would be silently incomplete for every read command added afterwards. `MEMORY USAGE` is
  deliberately excluded: it inspects a key's footprint rather than reading its value, and
  counting it would inflate the hit rate with introspection traffic. A test asserts that a
  run of pure writes moves neither counter.
- **Observability that costs nothing when nobody is looking.** Per-command statistics
  are always collected, because `INFO commandstats` is always available — so they are
  three atomic adds on the `*command` the dispatcher already holds, with no map lookup, no
  lock and no allocation, and a log2 histogram (512 bytes per command) behind the
  percentiles. The opt-in features are each gated by one atomic load: the slow log by its
  threshold, the latency monitor by its threshold, and `MONITOR` by a count of attached
  monitors — so a server nobody is monitoring never formats a line or copies an argument.
  An `AllocsPerRun` test pins all of it, the same way the keyspace-notification hook is
  pinned. A `MONITOR` feed then uses the bounded-queue-and-drop contract Pub/Sub uses: a
  monitoring client is watching rather than participating, so blocking every other client
  behind one that stopped reading would be the worst possible trade. And time a client
  spent parked in a blocking command is subtracted before the slow log sees it, because a
  `BLPOP` that waited five seconds for a push is not the server being slow.
- **A monitor must never see a credential.** `MONITOR` is a plaintext echo of everything
  clients send, which makes it the one place a password reliably lands in a log file.
  `AUTH`'s arguments are replaced with `(redacted)`, and so are `HELLO ... AUTH`'s and
  `CONFIG SET requirepass|masterauth`'s. The redaction happens in one function, so there
  is exactly one place that decides what a monitor may see.
- **The slot map is copy-on-write, and that is what makes the redirect free.** A slot
  lookup sits on the path of every command in cluster mode, so it has to be one atomic
  load and an array index — no lock, no map. Mutations (`ADDSLOTS`, `SETSLOT`) are
  administrative and rare, so they clone the whole 16384-entry table under a mutex and
  swap the pointer. Cloning ~400 KB to move one slot sounds wasteful until you count the
  other side: the alternative is a lock, or an atomic per slot, on every command every
  node ever serves. It also buys consistency for free exactly where it is hardest to get
  otherwise — a multi-key command resolves *all* of its keys against one immutable
  generation, so a concurrent `SETSLOT` cannot make two keys of the same command
  disagree about who owns their slot and produce a `CROSSSLOT` that was never true.
  Nodes are immutable for the same reason: the table holds them by pointer and is read
  without a lock, so an address edited in place would be a data race. Updating a node
  replaces the value and repoints every slot that referred to the old one.
- **The redirect reads the same key list `WATCH` does.** Which arguments of a command
  are keys is answered once, by `commandKeys` → `affectedKeys` — the extraction
  `COMMAND GETKEYS` reports and invariant 7 requires. Routing deliberately does not get
  a list of its own. Two lists would drift, and the drift is silent in *both*
  directions: a command missing from the routing list is served by the wrong node
  (a second copy of the data, on a node no client looks at again), and a command missing
  from the `WATCH` list commits over a concurrent change. Sharing the extraction means a
  new multi-key command gets both for free, and a mistake in it is caught by either set
  of tests. `MIGRATE` is the case that proves it: its keys are at argument 3 *or* after a
  `KEYS` keyword, and the default of "argument 1 is the key" would have named the
  destination host as a key — routing the command by where it was sending data.
- **The redirect path costs a standalone server one atomic load.** `clusterRedirect` is
  called behind `s.ClusterEnabled()`, so a server started without `-cluster-enabled`
  never computes a slot, never builds a key slice and never touches the slot map. That
  is the discipline invariant 12 imposes on the observability hooks, applied to the one
  gate that could not be opt-in per connection. An `AllocsPerRun` test pins it, and then
  enables cluster mode and checks the same call actually decides something — so the
  measurement is of a disabled path and not a broken one.
- **A replica's stream is never redirected.** The gate sits in `executeCommand`, the
  *client* entry point, and not in `dispatch`, which is what an AOF replay and a master's
  stream go through. A replica must apply every write its master sends whatever its own
  slot map says: the master already decided the write belongs to that slot, and a replica
  that redirected it would silently drop data it is supposed to hold. Redis draws the
  same line (`mustObeyClient`).

## Store microbenchmarks

**These are in-process numbers for the store package, not server throughput.** They
measure a Go function call against a map behind a shard lock, with no socket, no RESP
encoding and no syscall involved. A networked server cannot approach them: the TCP and
protocol path dominates by orders of magnitude. The table is here because it is the
number the data structures are tuned against, not because it describes what a client
would see.

Apple Silicon, `go test -bench`, 8 logical cores:

| Operation         | ns/op | allocs/op | in-process rate |
| ----------------- | ----- | --------- | --------------- |
| `GET` (parallel)  | ~28   | 2         | ~35 M ops/sec   |
| `SET`             | ~115  | 3         | ~9 M ops/sec    |
| `INCR` (parallel) | ~77   | 3         | ~13 M ops/sec   |

```bash
go test -bench=. -benchmem ./internal/store
```

The write-path allocation is the `*entry` that enables in-place LRU bookkeeping
(see design notes).

### End-to-end throughput: no trustworthy numbers yet

There is deliberately **no server-level benchmark table here**, because attempts to
produce one on a development machine were not reproducible: three consecutive identical
`redis-benchmark` runs against the same real Redis on Docker Desktop for macOS measured
78k, 191k and 781k SET/sec — a tenfold spread on the reference implementation itself.
Publishing a comparison from that host would be publishing noise with a favourable
number selected from it.

Doing this properly needs a Linux host, cores pinned, the client on separate hardware
from the server, `memtier_benchmark` rather than a single-threaded driver, and several
runs with the variance reported alongside the median. Until that exists, the honest
statement is that end-to-end performance relative to Redis is **unmeasured**.

The harness for doing it is here, so that the missing number is a matter of hardware
rather than of tooling:

```bash
make bench-vs-redis              # ./test/bench/vs-redis.sh
REPS=5 PROFILES="baseline pipelined" make bench-vs-redis
```

It starts shardkv and a real `redis:7-alpine` on one Docker network, with persistence off
on both — an AOF against an RDB would be comparing two different durability contracts, not
two servers — drives them with `redis-benchmark` out of the same image, and covers six
profiles: a request at a time, deep pipelining (`-P 16`), a fan of 500 connections, 4 KiB
values, the collection types, and `XADD`. Every profile is repeated, and the repetitions
are **interleaved** between the two servers rather than run in blocks, so that load
drifting during the run is shared instead of handed to one of them. It records the CPU,
core count, container runtime and both server versions into the report, because a
benchmark without its hardware is an anecdote.

The part that matters most: it computes a coefficient of variation across the repetitions
of each cell and **refuses to print a ratio above 10%**, reporting the observed spread
instead and exiting non-zero. On the machine this was developed on it refuses nearly
everything, which is the correct answer — the tool declining to compare is the finding,
and it is why there is no table above.

## Compatibility: what is missing

This server speaks the Redis wire protocol and reports `redis_version:7.4.0` for the
feature level it implements, which means client libraries and monitoring agents treat it
as a Redis 7.4 server. The gap between that claim and reality is listed here rather than
left for a caller to discover at runtime.

**No scripting.** `EVAL`, `EVALSHA`, `FCALL` and the rest are absent — there is no Lua
interpreter, and adding one would mean a third-party dependency the project does not
have. `SCRIPT FLUSH`/`EXISTS` and `FUNCTION FLUSH`/`LIST`/`STATS` answer truthfully for a
server that caches nothing (which is what lets Redis's own test suite run against it),
but anything that *executes* a script is refused explicitly rather than faked.

The consequence worth knowing: **library features built on scripts do not work**, and
some of them fail asymmetrically. `redis-py`'s `Lock` acquires successfully and then
cannot release, because release is a registered script — the lock expires by TTL instead.
Verified against redis-py 6.4. The same applies to Redisson, BullMQ, Sidekiq and most
Lua-based rate limiters. If you need those, you need Redis.

**Commands a client may expect and not find**, verified absent:
`WAITAOF`; `ACL`; the sharded Pub/Sub family `SSUBSCRIBE`/`SUNSUBSCRIBE`/`SPUBLISH`;
`CLIENT NO-EVICT`/`NO-TOUCH`/`PAUSE`/`UNPAUSE`; the hash-field expiry family
`HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPERSIST` added in 7.4.

**No RDB, in either direction.** A full resync ships a stream of RESP commands rather
than an RDB payload; `DUMP`/`RESTORE` use a self-describing `SHARDKV1` format; and
`SAVE`/`BGSAVE` write a native snapshot whose header says, in ASCII, that it is not an RDB
(see [Persistence (snapshots)](#persistence-snapshots)). All three reject a real Redis
payload cleanly, and are cleanly rejected by real Redis, rather than being misread. That
choice is deliberate — a subtly wrong listpack encoding would produce a payload real Redis
*accepts and misparses*, which is silent corruption — but it has a real cost: **you
cannot load an existing Redis dataset into this server**, and `redis-cli --rdb`,
`redis-shake`, `redis-check-rdb` and live cutover from Redis do not work.

**`OBJECT ENCODING` is derived, not remembered.** <a id="object-encoding-and-the-representation-thresholds"></a>
The thresholds are real configuration — `CONFIG SET hash-max-listpack-entries 3` genuinely
changes what a four-field hash reports — but the name is computed from the value's *current*
contents and the *current* thresholds, because this store keeps one representation per type
and so has no conversion to remember having done. Redis converts on write and never converts
back, which shows up in two places: raising a threshold here can make a value report
`listpack` again where Redis would still say `hashtable`, and lowering one changes the
answer without any write having happened. Every reading taken at the moment a value is built
agrees with Redis, which is what the `foreach encoding {listpack hashtable}` loops in its
own test suite ask for.

**Two introspection replies differ deliberately.** Both are cosmetic, both would cost
real state or a fiction to "fix", and both are named here rather than left to surprise
someone reading `OBJECT`:

- `OBJECT REFCOUNT` answers 1 for every key. Redis answers 2147483647 for integers in
  0–9999 because they come from a shared object pool; there is no such pool here, and a
  sentinel refcount would describe an implementation that does not exist. Redis's own
  suite uses `assert_refcount_morethan` in a few places, which this cannot satisfy.
- `OBJECT ENCODING` names the representation, not the content. Which of the three names a
  string gets follows *how the value was produced*, which is what Redis's own answer
  follows: a value stored whole by a command that runs `tryObjectEncoding` (`SET`, `MSET`,
  `GETSET`, `INCR`, and `APPEND` when it creates the key) can read `int`; one built whole by
  a command that does not (`INCRBYFLOAT`, whose result reads `embstr` even when it is
  integral) is embstr or raw by length; and one produced by editing a buffer in place
  (`APPEND` onto an existing key, `SETRANGE`, `SETBIT`, `BITFIELD`, `BITOP`, the
  HyperLogLog commands) is always `raw`. `COPY` reports what its source reported, because
  Redis duplicates the object rather than re-storing its bytes. The whole matrix is checked
  against a live redis 7.2.15 by `TestObjectEncodingOrigin`.

  What is *not* modelled is Redis's shared-integer pool, which is what `OBJECT REFCOUNT`
  above is about, and a `RESTORE`d value's original encoding: this `DUMP` payload records
  the bytes and not the encoding, so an appended value that is dumped and restored reads
  `int` again where Redis would still say `raw`.

### How the list above is established

Everything on it was found by running real client libraries against this server — not by
reading the protocol specification, and not with `redis-cli`. The distinction is the whole
point. `redis-cli` prints whatever comes back; a client library *parses* it. It negotiates
with `HELLO`, labels the connection with `CLIENT SETINFO`, decides which exception class to
raise by matching the **text** of an error reply, checks that a RESP3 map arrived as a map
rather than as an array, and in cluster mode builds a slot-to-node table out of
`CLUSTER SLOTS`/`SHARDS` and follows `MOVED` and `ASK` on its own. None of that is
exercised by typing commands at a prompt.

```bash
make compat            # the four client libraries
make compat-tcl        # Redis's own test suite, external mode
```

`test/compat/` runs each library in its own container — `redis-py` (with the stricter
hiredis parser), `ioredis`, `node-redis`, `go-redis`, plus each one's *cluster* client
against a three-node cluster — and then **runs every check a second time against a real
`redis:7-alpine`, under the same code**. That second column is what makes the result
evidence rather than an assertion: a check that fails against both servers is a bug in the
check or a quirk of the library, and only one that passes against Redis and fails here is
an incompatibility. The harness reports the pair, and only the second kind sets its exit
status.

That design earned its keep immediately. Three of the first failures were the harness's
own fault, not the server's — including an expected `CLUSTER KEYSLOT` value taken from the
Redis documentation's example that turned out to be wrong, where shardkv had been right all
along. A suite without a reference column would have recorded that as a server bug.

| library | checks | pass | fails, and why |
| ------- | ------ | ---- | -------------- |
| **node-redis 5** (RESP2 + RESP3) | 35 | **35** | none — identical to real Redis on every check, cluster client included |
| **ioredis 5** | 39 + 1 n/a | **38** | `defineCommand` (Lua). The one skip is RESP3: ioredis 5 does not speak it |
| **redis-py 8** (hiredis parser) | 85 | **78** | `EVAL`; `Lock` (releases via a Lua script); `SORT`; `ZUNIONSTORE`; `ZRANGE BYLEX`; `SCAN TYPE`; `RPUSHX` |
| **go-redis v9** (RESP3 by default) | 66 | **57** | the same set, plus `ROLE`, `EXPIRETIME`, `ZRANGE BYSCORE` — and one cluster-`SCAN` check that has not reproduced (see below) |

Every failure in those last two rows except `EVAL` and `Lock` has since been implemented —
`SORT`, `ZUNIONSTORE`, `ZRANGE BYLEX`/`BYSCORE`, `SCAN TYPE`, `RPUSHX`, `ROLE` and
`EXPIRETIME` are all present now — so the table understates the current state and will be
re-measured with the harness rather than edited by hand. Scripting is the only remaining
cause, and it is not going to change.

Every failure in that table is a command or option this server does not implement, listed
above — not a reply this server gets wrong. The one exception is honest to record: under a
run with three suites competing for the machine, go-redis's cluster `SCAN` found 62 of 64
keys once. It has not reproduced, and a direct check of the same thing — 64 keys across
three nodes, `SCAN` with `COUNT 20` per node — returns 64 of 64 with `DBSIZE` agreeing on
every node. It is recorded here rather than dropped because "it went away" is not a
diagnosis.

The cluster clients are the part worth
singling out: `RedisCluster`, `ioredis.Cluster` and `ClusterClient` all discover the
three-node topology from `CLUSTER SLOTS`/`SHARDS`, route by slot, follow `MOVED`, split a
pipeline per node and stitch the replies back together, and raise `CROSSSLOT` for a
multi-key command that spans slots — against this server exactly as against Redis.

And on Redis's own suite, against this commit: **about 1350 of its assertions pass**, across
the 23 unit files that cover the implemented surface, with 18 of those files now running to
completion. Measured per file, one invocation each:

| file | ok | err | stops early on |
| --- | --- | --- | --- |
| `type/zset` | 315 | 1 | — |
| `type/list` | 247 | 4 | — |
| `type/set` | 112 | 2 | — |
| `type/string` | 79 | 0 | — |
| `type/hash` | 71 | 2 | — |
| `geo` | 58 | 6 | — |
| `expire` | 58 | 0 | — |
| `type/stream` | 57 | 15 | — |
| `bitops` | 49 | 0 | — |
| `keyspace` | 45 | 1 | — |
| `type/stream-cgroups` | 41 | 11 | `XINFO STREAM FULL` |
| `type/incr` | 32 | 0 | — |
| `sort` | 32 | 0 | `EVAL` |
| `pubsub` | 29 | 1 | `EVAL` |
| `multi` | 23 | 0 | `CONFIG SET lua-time-limit` |
| `hyperloglog` | 22 | 3 | — |
| `scan` | 21 | 1 | — |
| `bitfield` | 18 | 0 | — |
| `protocol` | 16 | 3 | — |
| `slowlog` | 11 | 0 | — |
| `dump` | 8 | 1 | — |
| `latency-monitor` | 6 | 0 | `EVAL` |
| `quit` | 3 | 0 | — |

The honest part of that number is what is *still* missing, and the shape of it changed. The
largest single cause used to be the encoding thresholds: Redis's type tests open by setting
one (`CONFIG SET hash-max-listpack-entries`, `set-max-intset-entries`, …) to exercise both of
its internal representations, and with the thresholds hard-coded here `unit/type/hash`,
`list`, `set` and `zset` stopped on their first line and contributed nothing. Those four now
account for 745 of the assertions above. What remains is mostly scripting — four files end at
an `EVAL` or a `lua-time-limit`, and no amount of work short of an interpreter changes that.

The `err` column is worth reading rather than summing, because most of it is one structural
difference repeated:

- `type/stream`'s fifteen are almost all approximate trimming. `XADD … MAXLEN ~` and
  `XTRIM … LIMIT` are defined in terms of Redis's macro-nodes, and a stream stored as a
  sorted slice has none to trim by, so it trims *exactly* — which the tests measure and find
  different.
- The encoding assertions in `type/list`, `type/set` and `type/hash` are the derived-vs-remembered
  difference described below: this server computes the name from the value's current contents,
  so raising a threshold can make a value report `listpack` again where Redis would still say
  `hashtable`.
- `type/hash`'s remaining pair is field ordering, which a Go map does not have.
- One in `type/list` fails on real Redis too, roughly one run in six: it races an `EXEC`
  against an unblocking `LPUSH` on another connection with no synchronisation between them.
  Verified by running the same sequence against `redis:7.2` six times.
- One in `type/list` is a libc difference: Redis parses a timeout with `strtold`, which accepts
  a C hex literal (`0x7FFFFFFFFFFFFF`), and Go's `ParseFloat` requires a `p` exponent for hex.
  It is recorded rather than papered over, because the honest fix is a float parser that
  accepts everything `strtod` does, not a special case in one operand.

Quoting a number without that paragraph would be the same dishonesty as a benchmark table
with the losses removed.

The other half is `test/compat/tcl/`, which points **Redis's own TCL test suite** at this
server in external mode (`--host`/`--port`). This is the highest-authority signal available
to anything claiming Redis compatibility: assertions written by the people who define the
behaviour, checking exact replies and exact error strings, with no idea they are not talking
to Redis. It is pinned to the 7.4 release rather than tracking `unstable`, because measuring
against a moving target reports the distance to it as failure. Each file runs in its own
invocation — the suite aborts a whole file on the first unsupported command, so per-file
runs turn "one missing command" into one lost file instead of one lost run — and the same
files run against a real Redis so the assertion counts have a baseline to sit beside.

It runs on a **nightly schedule** (and on demand) rather than on every push, in its own
workflow. Measured, it takes 194 minutes, and the seven jobs that actually gate a merge finish
in under two — so keeping it in the push path meant a CI run reported `in_progress` for over
three hours after the gate was already green, and the branch badge went on showing whatever
completed before it. That is a cost paid in signal, not in runner minutes: it is why the badge
here once read `failing` from a run cancelled the day before while every gating job was
passing. Bounding it with a timeout was the obvious alternative and is worse — the bound fires
every time, so the assertion count gets truncated somewhere that moves with runner speed, and
a count that cannot be compared with last night's is the one thing this suite exists to
produce.

Four bugs came out of that suite that none of this project's own tests had found, and the
first was the serious one:

- **`INCRBYFLOAT` formatted large results in scientific notation** (`1.71798691855e+10`
  where Redis gives `17179869185.5`). Go's `strconv` `'g'` format switches to an exponent
  far earlier than Redis does. That was not a cosmetic reply difference: `INCRBYFLOAT`
  propagates its *effect* as `SET key <result>`, so the exponent notation was what reached
  the AOF and every replica. Fixed at all four call sites that share the formatter
  (`INCRBYFLOAT`, `HINCRBYFLOAT`, `ZINCRBY`, `ZSCORE`), against output measured from a real
  Redis rather than from memory — including that Redis emits unpadded exponents (`1e-7`)
  where Go emits `1e-07`.
- **`INCRBYFLOAT inf` returned the wrong error.** The operand was being rejected during
  parsing (`ERR value is not a valid float`) when Redis accepts infinite *operands* and
  rejects the infinite *result* (`ERR increment would produce NaN or Infinity`). The error
  now names what actually went wrong.
- **`CLIENT SETINFO` did not exist**, and modern `redis-py`, `node-redis` and `go-redis`
  all send two of them, unprompted, on every connect.
- **`ECHO` did not exist**, which is worth singling out because it is part of
  StackExchange.Redis's handshake — so the entire .NET ecosystem could not connect at all.

Two differences found this way are **deliberately not fixed**, because fixing them would
mean lying about the implementation:

- **`OBJECT REFCOUNT` of a small integer returns 1**, where Redis returns 2147483647
  because integers 0-9999 come from a shared object pool. There is no such pool here, and a
  sentinel refcount would describe an optimisation that does not exist.
- **`OBJECT ENCODING` of a `RESTORE`d value is derived from its bytes.** The `DUMP` payload
  here records a value's contents and not the encoding Redis had chosen for it, so an
  appended value that is dumped and restored reads `int` again where Redis would still say
  `raw`. Recording an encoding in the payload would change the format `DUMP` promises for
  the sake of an introspection reply.

  The rest of the encoding surface *was* fixed rather than documented away: after
  `SET foo 1` then `APPEND foo 2` this server reports `raw` as Redis does, and an integral
  `INCRBYFLOAT` result reports `embstr` rather than `int` — see
  [OBJECT ENCODING](#object-encoding-and-the-representation-thresholds) for the three states
  that decide it.

`OBJECT REFCOUNT` costs assertions in Redis's suite (`assert_refcount_morethan` runs in a
few of its type files), which is the honest price of the choice rather than a reason to hide
it.

## Testing

```bash
go test -race ./...                                   # unit + integration, race on
go test -fuzz=FuzzReadCommand -fuzztime=30s ./internal/resp   # fuzz the protocol parser
go vet ./...
```

The suite includes master/replica convergence, expiry-propagation, transaction
(`MULTI`/`EXEC`/`WATCH`), eviction, `SCAN`, AOF round-trip / torn-tail / rewrite,
RESP adversarial-input, and a skip-list fuzz-against-sort cross-check, plus
concurrency tests that drive shared keys from many goroutines and TCP clients.
Replication is covered in both directions — the keepalive a master sends, the read
deadline that makes a replica abandon a silent one, and the drop-and-resync when a
replica falls too far behind — alongside a chunked-snapshot round-trip (encode,
decode, replay, compare) and an injected-clock check that a propagated deadline
equals the one held in memory.

Every command is exercised on its happy path *and* against a missing key, an empty
collection, the wrong data type, an out-of-range or negative index, and a bad
argument count. On top of that, the properties a single command's test cannot see:

- **Propagated form.** Each command carrying a relative expiry or a conditional
  flag is checked for the exact command it puts on the wire, and that the deadline
  named there is the one in memory (under an injected clock, so it is exact).
- **Replica convergence.** A master is driven through every random and every
  multi-key write, then master and replica are compared key by key — the only way
  to catch a command that ships un-replayably.
- **AOF replay.** The whole widened surface is written through a real log, replayed
  into a fresh server, and compared, so a rewritten command that the reader would
  refuse (or apply differently) fails the build.
- **WATCH on a second key.** Every multi-key write is checked to abort a
  transaction watching its *destination*.
- **Lock ordering.** Two-key writes are hammered from many goroutines with the
  arguments swapped, which deadlocks on the spot if the shards are not locked in
  index order.
- **Table metadata.** The command table is walked to require every write to be
  refused on a replica, which is what catches a missing `write` flag or a wrong
  arity on a newly added command.
- **AOF rewrite under load.** Eight goroutines write while a rewrite runs; the log is
  then replayed into a fresh server and compared against the live dataset. A write
  landing between the snapshot and the file swap would be acknowledged and then
  discarded, and only a comparison like this notices.
- **Partial resync.** A replica's link is dropped, writes (including a `DEL`) continue
  while it is down, and the reconnect has to be served from the backlog — with
  `sync_full` unchanged, `sync_partial_ok` at one, and both datasets and both offsets
  agreeing afterwards. The same test with a 64-byte backlog has to fall back to a full
  resync instead, and the deleted key still has to disappear.
- **A slow subscriber, and a slow replica.** Both are proven not to block the writer:
  the queue fills, the consumer is disconnected, the publisher's own connection is
  untouched, and what was already queued stays in order.
- **Notifications cost nothing when off.** `testing.AllocsPerRun` asserts zero
  allocations per call on the disabled write-path hook and the disabled expiry hook, and
  that the same write starts delivering once the flags are set — so the measurement is
  of a disabled path, not a broken one.
- **Auth, including the bypass.** Every unauthenticated command path, both wrong-password
  wordings, `HELLO ... AUTH`, `RESET` de-authenticating a pooled connection, `PSYNC`
  refused without a password, and a stand-in master that records the first command of
  each connection to prove a replica re-authenticates on every reconnect.
- **TLS with a certificate generated in the test.** `crypto/x509` in `t.TempDir()`, so
  no key material is committed and nothing expires on a date nobody chose: a TLS client
  is served, a plain-TCP client is not, an untrusted root is refused, and replication
  converges over TLS while the replica serves plain TCP.
- **RESP2 replies, byte for byte.** Every reply whose shape RESP3 changes is asserted as
  raw bytes on a RESP2 connection, reading exactly the expected length so a reply one
  byte too long shows up as the next assertion's prefix instead of being tolerated. That
  is the regression risk of adding a second protocol, and a test that parses a reply
  cannot see it. The RESP3 side is pinned the same way against bytes captured from real
  Redis 7.
- **Blocking, with real concurrent TCP clients under `-race`.** Wakeup by every kind of
  write (push, rename, transaction, `SWAPDB`); a fractional timeout that has to have
  actually elapsed; **fairness**, where five clients block in a known order — each sent
  only after `blocked_clients` confirms the previous one is waiting — and five pushes
  must be served in that order; one push of three elements serving three waiters, which
  only works if a served waiter hands the queue on; **no goroutine leak** across twenty
  abandoned waits, ten by timeout and ten by hanging up mid-block, measured by letting
  the goroutine count settle rather than sampling it; a blocking command inside `MULTI`
  proven not to block; the effect (`LPOP`, `ZREM`, `LMOVE`) that reaches the replica feed
  rather than the command; a `WATCH` on a `BLMOVE` destination, which isolates the
  blocked client's own write from the push that woke it; `FLUSHALL`, expiry and a
  demotion to replica while blocked; `CLIENT UNBLOCK` in both modes; and a subscribed
  RESP3 connection receiving a message *while* blocked, which is what proves the writer
  lock was released.
- **Databases, end to end.** Isolation across every type and TTLs; `FLUSHDB` against
  `FLUSHALL`; `SWAPDB`, `MOVE` and `COPY ... DB` including the refusals; `WATCH` and
  blocked clients proven per-database; per-database notification channels including
  `MOVE`'s two events in two databases. The two that matter most: the stream itself is
  asserted to contain **no `SELECT` for a database-0 workload** and exactly one where the
  database changes, and then the loop is closed twice — an AOF written across four
  databases is replayed into a fresh server and compared database by database, and a
  replica takes a full resync of a master holding four databases and is compared the same
  way, with live writes afterwards from a non-zero database.
- **Streams, including the two failures that would be silent.** The whole surface at the
  wire level (generated, `ms-*` and explicit ids; the exclusive range forms; both trim
  strategies and both markers; the full group lifecycle through `XCLAIM`/`XAUTOCLAIM`);
  a **backwards clock jump** under an injected clock, which must still produce an id that
  sorts after the last one; `XREAD BLOCK` and `XREADGROUP BLOCK` woken by another
  connection's `XADD`, with the server proven still writable while a client is parked, and
  `XREADGROUP BLOCK` inside `MULTI` proven not to block; the **propagated form** of every
  clock-dependent command, asserting that `XADD *` ships a concrete id and not a `*`, that
  a trim ships as an exact `XTRIM`, that `XGROUP CREATE ... $` ships the resolved id, and
  that `XREADGROUP` ships one `XCLAIM ... TIME` per delivery plus the group's resulting
  position; **`Dump` round-tripped through a fresh server under a frozen clock** and
  compared by what a client can observe (`XINFO STREAM`, `XINFO GROUPS`,
  `XINFO CONSUMERS`, both `XPENDING` forms), which is the test that would fail if a group,
  its position or its pending list were dropped; and a master/replica convergence check
  driven through `XADD *`, `XREADGROUP`, `XACK` and `XCLAIM` and compared the same way.
- **Bitmaps, and their interaction with the string commands.** Redis's own `BITCOUNT`
  values for `"foobar"` in both `BYTE` and `BIT` ranges; the `BITPOS` rule that an
  unbounded zero-search runs past the end of the value but a bounded one does not; `BITOP`
  over operands of different lengths with the short one zero-padded, and an empty result
  deleting the destination; every `BITFIELD` overflow policy including `FAIL`'s null in
  the failing slot; a `BITFIELD` chain proven to see its own writes; and the shared
  representation checked directly — `SETBIT` then `STRLEN`, `APPEND` then `BITCOUNT`,
  `SETRANGE` then `GETBIT`, and a TTL surviving a `SETBIT`.
- **HyperLogLog, against its own promise and against real Redis.** The measured error
  across 200 cardinalities from 1k to 200k (worst case **1.42 %**, against a 2 % bound
  and a 0.81 % standard error); near-exact counts under a few thousand, which is what
  catches an estimator that skipped the sigma correction; the union property, with
  `PFMERGE` and multi-key `PFCOUNT` required to agree *exactly*; determinism, including
  the same additions in a different order producing byte-identical values, which is what
  makes verbatim propagation safe; the sparse-to-dense promotion asserted lossless
  register by register; the layout pinned field by field (magic, encoding byte, the
  12304-byte dense length, the stale-cache bit); and `PFSELFTEST`, which round-trips every
  register value at every position through the six-bit packing — as a *command*, so it can
  be run against a server that is already running, which is when a packing bug matters.
- **Geo, against known real-world distances.** `GEODIST` between eight real cities
  (Palermo–Catania to the metre against Redis's documented value, plus London–Paris,
  London–New York, New York–Sydney, Tokyo–Sydney, London–Nairobi) inside the 0.5 % Redis
  documents; the `GEOHASH` strings asserted against the values in Redis's own
  documentation; a `GEOPOS` round trip at the coordinate extremes; and a **property test**
  that builds a grid of points and, for six radii spanning four orders of magnitude,
  requires the nine-cell search to return *exactly* what a brute-force distance check
  would — which is the test that catches a wrong neighbour computation, whose only other
  symptom is a member silently missing near a cell boundary.
- **Observability costs nothing when unused.** `testing.AllocsPerRun` asserts zero
  allocations for the always-on per-command statistics, for the whole observation hook
  with every opt-in feature off, and for the `MONITOR` feed with no monitors attached —
  and then enables the slow log and checks an entry appears, so the measurement is of a
  disabled path and not a broken one. Alongside it: a monitor proven to receive another
  client's commands and **never** an `AUTH`, `HELLO ... AUTH` or
  `CONFIG SET requirepass` argument; a monitor that stops reading proven to be dropped
  rather than to stall the command path; a `BLPOP` that waited 50 ms proven *not* to
  reach the slow log; the slow log's argument and argument-count truncation; and
  `keyspace_hits`/`keyspace_misses` checked to move for reads of every type, to count one
  lookup per key for `MGET`, and — the point of the whole design — **not to move at all**
  for a run of pure writes.
- **Hash slots, against a live cluster-enabled `redis:7-alpine`.** Thirty-three key/slot
  pairs read out of `redis-cli CLUSTER KEYSLOT` one key at a time — not derived from the
  documentation and not computed by this implementation — covering tagged keys, untagged
  keys, the empty tag `{}`, an unclosed `{`, a stray `}`, nested braces (`{a{b}c` hashes
  `a{b`, `{{double}}` hashes `{double`), repeated tags, and a tag of one space. Plus the
  published CRC-16/XMODEM check vector, so a failure separates "the checksum is wrong"
  from "the tag parsing is wrong", and a range property over 12288 generated keys.
- **Redirects, driven through the client path.** `MOVED` for a foreign slot and *not* for
  the keyless commands (`PING`, `INFO`, `SCAN`, `PUBLISH`, `CLUSTER KEYSLOT`);
  `CLUSTERDOWN` for an unassigned slot; `CROSSSLOT` over **31 multi-key command shapes**,
  each then shown to be accepted when its keys share a tag; a slot migration's three
  states (served here, `ASK` to the target, `TRYAGAIN` for a command straddling both);
  the one-shot `ASKING` flag proven to expire after exactly one command and not to apply
  to an unrelated slot; a transaction redirected at queue time and proven to abort at
  `EXEC`, and one straddling two slots refused at `EXEC` where the whole batch is first
  visible. Two structural tests sit behind those: one requires the routing decision to
  read exactly what `COMMAND GETKEYS` reports, so the two cannot drift, and one requires
  `dispatch` — the AOF-replay and master-stream path — to be redirected *never*.
- **A slot migration between two real nodes.** Two listening servers, `CLUSTER MEET`
  between them over a socket, then every stored kind (including a stream with a consumer
  group and a live pending-entries list) moved with `MIGRATE` and compared key by key on
  the far side. Alongside it: `COPY` leaving the source intact, `REPLACE` and the
  `BUSYKEY` refusal that must *not* consume the source key, `NOKEY`, a TTL crossing the
  boundary as remaining life rather than as a deadline, and the propagated form — a
  `DEL` of the keys that left, never the `MIGRATE`.
- **`DUMP`/`RESTORE` round-trips for all ten types**, each compared with reads that
  describe the whole of its state: a HyperLogLog's register body byte for byte in both
  the sparse and the dense encoding (not merely its estimate), a geo set's 52-bit scores,
  a bitmap's bit pattern, and a stream's id counters, groups, consumers and pending
  entries. Then the three gates, each defeated in turn: a real Redis `DUMP` payload, a
  flipped body byte, a bad checksum, a truncated body and a future version number must
  each be refused with Redis's own message and must leave no key behind — and a payload
  carrying `FLUSHALL` past a *valid* checksum must be refused by the whitelist with the
  witness key still present, which is what proves the checksum is not being trusted as
  an authorization.
- **Cluster configuration survives a restart.** A node's id, slot map and peer table are
  written to its config file and reloaded by a second server on the same file, with
  `CLUSTER NODES` required to match byte for byte; a malformed file is refused rather
  than half-understood.

## Layout

```text
cmd/shardkv        entrypoint: flags, signal handling, AOF/replication/TLS wiring
internal/store     sharded store, typed values, skip-list sorted set, LRU eviction, snapshot   (+ tests, benchmarks)
  databases.go       the cross-store operations SWAPDB/MOVE/COPY ... DB need
  stream.go          the stream type: ms-seq ids, sorted entries, consumer groups, PELs
  bitmap.go          bit addressing over the string type, BITOP, BITFIELD
  hyperloglog.go     Redis's HYLL format: sparse + dense registers, murmur64a, tau/sigma
  memory.go          the per-key footprint estimate MEMORY USAGE reports
internal/resp      RESP protocol reader/writer, size accounting                                 (+ tests, fuzz)
internal/resp      + the RESP3 types and their RESP2 fallbacks, per-connection version
internal/server    TCP server, command table + handlers, transactions, replication             (+ tests)
  server.go          connection loop, dispatch, the database views, propagation funnels
  session.go         per-connection state: database, transaction, auth, subscriptions
  reply.go           the reply shapes RESP3 changes, written once for both protocols
  blocking.go        wait queues, FIFO wakeup, disconnect watchdog, CLIENT UNBLOCK
  auth.go            requirepass/masterauth, AUTH, the NOAUTH gate
  tls.go             certificate loading, listener wrapping, master dialer
  pubsub.go          subscription registry, fan-out, subscriber mode, message pump
  notify.go          keyspace notification flags and event table
  observability.go   slow log, MONITOR, latency monitor, per-command stats, MEMORY, GETKEYS
  geohash.go         52-bit geohash encode/decode, haversine, the nine-cell search area
  replication.go     PSYNC/REPLCONF/WAIT, the replica's link, offsets
  backlog.go         bounded replication backlog ring, command encoder
  cluster_slots.go   CRC16-XMODEM, hash tags, the key -> slot function
  cluster.go         the copy-on-write slot map, the node table, nodes.conf, CLUSTER MEET
  cluster_redirect.go  MOVED/ASK/CROSSSLOT/TRYAGAIN/CLUSTERDOWN, the ASKING flag
  commands_cluster.go  the CLUSTER subcommands and the NODES/SLOTS/SHARDS formats
  commands_dump.go     DUMP/RESTORE's payload format, and MIGRATE
  aof_rewrite.go     BGREWRITEAOF and the automatic growth policy
  snapshot.go        SAVE/BGSAVE, the save schedule, the startup load, DEBUG RELOAD
  commands_*.go      the command groups, incl. blocking, debug, client/config/admin
internal/aof       append-only-file persistence: append, fsync policy, size, rewrite, replay   (+ tests)
internal/snapshot  the point-in-time snapshot file: header, framing, CRC-64, atomic write   (+ tests)
```

## Roadmap

- `XINFO STREAM FULL`, and `XADD`'s `+` id form from Redis 7.4
- `sync.Pool` for entries to remove the write-path allocation
- `ZRANGEBYLEX`, and `LPUSHX`/`RPUSHX`
- `OBJECT FREQ` and an LFU eviction policy alongside the LRU one, which would also give
  `RESTORE`'s `FREQ` option something to do
- Client-side caching (`CLIENT TRACKING`), which RESP3's invalidation pushes now make
  expressible
- Sharded Pub/Sub (`SPUBLISH`/`SSUBSCRIBE`/`SUNSUBSCRIBE`), which routes a channel by
  slot and is the one part of cluster Pub/Sub that does *not* need the gossip bus
- A `--cluster create`-style helper that assigns slots and runs the `MEET` mesh in one
  command, since without gossip that is currently a shell loop
- The cluster bus itself — failure detection and automatic failover — which is a
  protocol, not a feature, and is deliberately out of scope today (see
  [Cluster](#cluster))
- ACL users beyond `default`, now that `AUTH username password` is parsed
- A fork-and-diff-buffer AOF rewrite, to stop blocking writers for the duration

## License

MIT — see [LICENSE](LICENSE).
