// ioredis against shardkv, and against a real Redis for reference.
//
// ioredis is the client most Node applications actually run, and it is the most
// opinionated of the four: it keeps a command queue per connection, reorders
// nothing, insists on seeing the subscribe confirmation before it will deliver a
// message, and -- in Cluster mode -- refreshes its slot table from CLUSTER SLOTS
// on every MOVED. It speaks RESP2 only (ioredis 5 has no RESP3 support), so the
// RESP3 checks here are deliberately skipped rather than faked.

import Redis from "ioredis";
import { createRequire } from "node:module";

const HOST = process.env.SHARDKV_HOST ?? "127.0.0.1";
const PORT = Number(process.env.SHARDKV_PORT ?? 6380);
const CHOST = process.env.CLUSTER_HOST ?? HOST;
const CPORT = Number(process.env.CLUSTER_PORT ?? 6380);
const TARGET = process.env.TARGET ?? "?";

const version = createRequire(import.meta.url)("ioredis/package.json").version;

function result(feature, status, detail = "") {
  const flat = String(detail).replace(/\s+/g, " ").slice(0, 220);
  process.stdout.write(`::RESULT::${feature}::${status}::${flat}\n`);
}

class Skip extends Error {}

const clients = [];
function open(opts = {}) {
  const c = new Redis({
    host: HOST,
    port: PORT,
    maxRetriesPerRequest: 2,
    retryStrategy: () => null,
    ...opts,
  });
  c.on("error", () => {}); // otherwise a deliberate failure kills the process
  clients.push(c);
  return c;
}

async function check(feature, fn) {
  try {
    await fn();
    result(feature, "PASS");
  } catch (err) {
    if (err instanceof Skip) result(feature, "SKIP", err.message);
    else result(feature, "FAIL", `${err.name}: ${err.message}`);
  }
}

function canon(v) {
  // Key order in a JSON dump is not a property of the reply: HGETALL is a hash,
  // and neither the server nor the library promises an order for its fields. A
  // comparison that depended on it would pass or fail by luck.
  if (Array.isArray(v)) return v.map(canon);
  if (v && typeof v === "object" && !(v instanceof Map)) {
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])]));
  }
  return v;
}

function eq(got, want, what = "") {
  const g = JSON.stringify(canon(got));
  const w = JSON.stringify(canon(want));
  if (g !== w) throw new Error(`${what}got ${g}, want ${w}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function expectError(fn, needle) {
  try {
    await fn();
  } catch (err) {
    if (!err.message.toLowerCase().includes(needle.toLowerCase())) {
      throw new Error(`error was ${JSON.stringify(err.message)}, wanted ${needle}`);
    }
    return;
  }
  throw new Error(`no error raised; wanted ${needle}`);
}

process.stdout.write(`# ioredis ${version} -> ${TARGET} at ${HOST}:${PORT}\n`);
result("library_version", "PASS", `ioredis ${version}`);

const r = open();
await r.flushall();

// ---------------------------------------------------------------------------
// Handshake and connection state.
// ---------------------------------------------------------------------------

await check("handshake_ping_info", async () => {
  eq(await r.ping(), "PONG");
  const info = await r.info();
  if (!info.includes("# Server")) throw new Error("INFO has no # Server section");
});

await check("handshake_connection_name", async () => {
  // ioredis sends CLIENT SETNAME itself when connectionName is set, before it
  // will report the connection ready.
  const named = open({ connectionName: "ioredis-app" });
  eq(await named.client("GETNAME"), "ioredis-app");
});

await check("handshake_db_select", async () => {
  const c = open({ db: 5 });
  await c.set("in-db-5", "yes");
  eq(await c.get("in-db-5"), "yes");
  eq(await r.get("in-db-5"), null);
  await c.flushdb();
});

await check("handshake_resp3", async () => {
  throw new Skip("ioredis 5 speaks RESP2 only");
});

await check("client_setinfo", async () => {
  eq(await r.client("SETINFO", "LIB-NAME", "ioredis-compat"), "OK");
  const info = await r.client("INFO");
  if (!info.includes("lib-name=ioredis-compat")) {
    throw new Error(`CLIENT INFO after SETINFO: ${info}`);
  }
});

await check("command_docs_and_count", async () => {
  const count = await r.command("COUNT");
  if (count < 100) throw new Error(`COMMAND COUNT = ${count}`);
  const docs = await r.command("DOCS", "GET");
  if (!docs || docs.length === 0) throw new Error("COMMAND DOCS GET was empty");
});

await check("config_get", async () => {
  const got = await r.config("GET", "*");
  if (!Array.isArray(got) || got.length === 0) throw new Error(`CONFIG GET * = ${got}`);
});

// ---------------------------------------------------------------------------
// Types. ioredis returns everything as strings unless asked for buffers, so the
// checks below are also checking that the reply *kinds* line up.
// ---------------------------------------------------------------------------

await check("type_string", async () => {
  eq(await r.set("s", "hello"), "OK");
  eq(await r.get("s"), "hello");
  eq(await r.append("s", "!"), 6);
  eq(await r.getrange("s", 0, 3), "hell");
  eq(await r.incrby("counter", 5), 5);
  eq(await r.incrbyfloat("counter", 0.25), "5.25");
  eq(await r.mset("a", "1", "b", "2"), "OK");
  eq(await r.mget("a", "b", "missing"), ["1", "2", null]);
  eq(await r.setex("volatile", 60, "v"), "OK");
  const ttl = await r.ttl("volatile");
  if (ttl < 50 || ttl > 60) throw new Error(`TTL = ${ttl}`);
});

await check("type_binary_safe", async () => {
  // A value with a NUL and a CR in it: the length prefix is the only thing that
  // can carry it, so this is really a test of the writer's framing.
  const payload = Buffer.from([0, 13, 10, 255, 65, 0]);
  await r.set("bin", payload);
  const got = await r.getBuffer("bin");
  if (Buffer.compare(got, payload) !== 0) {
    throw new Error(`binary round-trip: got ${got.toString("hex")}`);
  }
  eq(await r.strlen("bin"), payload.length);
});

await check("type_list_hash_set_zset", async () => {
  await r.del("l", "h", "st", "z");
  eq(await r.rpush("l", "a", "b", "c"), 3);
  eq(await r.lrange("l", 0, -1), ["a", "b", "c"]);
  eq(await r.hset("h", "f", "v", "g", "w"), 2);
  eq(await r.hgetall("h"), { f: "v", g: "w" });
  eq(await r.sadd("st", "x", "y"), 2);
  eq((await r.smembers("st")).sort(), ["x", "y"]);
  eq(await r.zadd("z", 1, "a", 2, "b"), 2);
  eq(await r.zrange("z", 0, -1, "WITHSCORES"), ["a", "1", "b", "2"]);
  eq(await r.zscore("z", "b"), "2");
  eq(await r.type("z"), "zset");
});

await check("type_stream_and_hll_and_geo", async () => {
  await r.del("stx", "hll", "geo");
  const id = await r.xadd("stx", "*", "field", "value");
  if (!/^\d+-\d+$/.test(id)) throw new Error(`XADD returned ${id}`);
  eq(await r.xlen("stx"), 1);
  eq(await r.pfadd("hll", "a", "b"), 1);
  eq(await r.pfcount("hll"), 2);
  eq(await r.geoadd("geo", 13.361389, 38.115556, "Palermo"), 1);
  eq(await r.geohash("geo", "Palermo"), ["sqc8b49rny0"]);
});

// ---------------------------------------------------------------------------
// Pipelines and transactions. ioredis pipelines by default in `pipeline()` and
// wraps in MULTI/EXEC in `multi()`, and reports per-command errors as the first
// element of each result pair -- so a wrong error shape is visible here.
// ---------------------------------------------------------------------------

await check("pipeline", async () => {
  await r.del("p1");
  const got = await r.pipeline().set("p1", "v").get("p1").llen("p1").exec();
  eq(got[0], [null, "OK"]);
  eq(got[1], [null, "v"]);
  if (!got[2][0] || !got[2][0].message.includes("WRONGTYPE")) {
    throw new Error(`pipelined LLEN on a string: ${JSON.stringify(got[2])}`);
  }
});

await check("auto_pipelining", async () => {
  // enableAutoPipelining batches whatever the event loop produced in one tick
  // into a single write. It is the setting most throughput-sensitive Node
  // applications turn on, and it depends on replies coming back strictly in
  // order for a batch the server never saw framed as a unit.
  const c = open({ enableAutoPipelining: true });
  await c.del("auto");
  const results = await Promise.all(
    Array.from({ length: 200 }, (_, i) => c.hset("auto", `f${i}`, i)),
  );
  eq(results.filter((n) => n === 1).length, 200);
  eq(await c.hlen("auto"), 200);
});

await check("multi_exec", async () => {
  await r.del("t");
  const got = await r.multi().rpush("t", "a").rpush("t", "b").llen("t").exec();
  eq(got.map((pair) => pair[1]), [1, 2, 2]);
});

await check("multi_discard", async () => {
  await r.del("td");
  const m = r.multi();
  m.set("td", "1");
  await m.discard();
  eq(await r.get("td"), null);
});

await check("watch_aborts_on_conflict", async () => {
  const c = open();
  await c.set("w", "0");
  await c.watch("w");
  await open().set("w", "changed");
  const got = await c.multi().set("w", "mine").exec();
  // ioredis reports an aborted transaction as a null result set.
  if (got !== null) throw new Error(`EXEC after a conflicting write: ${JSON.stringify(got)}`);
  eq(await r.get("w"), "changed");
});

await check("watch_commits_when_clean", async () => {
  const c = open();
  await c.set("w2", "0");
  await c.watch("w2");
  const got = await c.multi().incr("w2").exec();
  eq(got, [[null, 1]]);
});

// ---------------------------------------------------------------------------
// Pub/Sub. ioredis puts a connection into subscriber mode and refuses ordinary
// commands on it locally, which makes the server's own refusal invisible -- so
// this checks delivery, patterns and introspection instead.
// ---------------------------------------------------------------------------

await check("pubsub_message", async () => {
  const sub = open();
  const got = new Promise((resolve) => sub.on("message", (ch, msg) => resolve([ch, msg])));
  eq(await sub.subscribe("news"), 1);
  eq(await r.publish("news", "hello"), 1);
  eq(await got, ["news", "hello"]);
});

await check("pubsub_pattern", async () => {
  const sub = open();
  const got = new Promise((resolve) =>
    sub.on("pmessage", (pat, ch, msg) => resolve([pat, ch, msg])),
  );
  eq(await sub.psubscribe("news.*"), 1);
  await r.publish("news.sport", "goal");
  eq(await got, ["news.*", "news.sport", "goal"]);
});

await check("pubsub_introspection", async () => {
  const sub = open();
  await sub.subscribe("introspect");
  const channels = await r.pubsub("CHANNELS");
  if (!channels.includes("introspect")) throw new Error(`PUBSUB CHANNELS = ${channels}`);
  eq(await r.pubsub("NUMSUB", "introspect"), ["introspect", 1]);
});

await check("keyspace_notifications", async () => {
  const sub = open();
  const got = new Promise((resolve) => sub.on("pmessage", (_p, ch, msg) => resolve([ch, msg])));
  await sub.psubscribe("__keyevent@0__:set");
  await sleep(100);
  await r.set("notified", "1");
  const [ch, msg] = await got;
  if (!ch.endsWith(":set")) throw new Error(`channel was ${ch}`);
  eq(msg, "notified");
});

// ---------------------------------------------------------------------------
// Blocking commands and streams.
// ---------------------------------------------------------------------------

await check("blocking_blpop", async () => {
  const c = open();
  await c.del("queue");
  const waiting = c.blpop("queue", 5);
  await sleep(200);
  await r.rpush("queue", "job");
  eq(await waiting, ["queue", "job"]);
});

await check("blocking_timeout", async () => {
  const c = open();
  eq(await c.blpop("never", 1), null);
});

await check("streams_consumer_group", async () => {
  await r.del("orders");
  await r.xadd("orders", "*", "item", "a");
  await r.xadd("orders", "*", "item", "b");
  eq(await r.xgroup("CREATE", "orders", "g", "0"), "OK");
  const got = await r.xreadgroup("GROUP", "g", "worker", "COUNT", "1", "STREAMS", "orders", ">");
  eq(got.length, 1);
  eq(got[0][0], "orders");
  const id = got[0][1][0][0];
  eq(got[0][1][0][1], ["item", "a"]);
  const pending = await r.xpending("orders", "g");
  eq(pending[0], 1);
  eq(await r.xack("orders", "g", id), 1);
});

await check("streams_xread_block", async () => {
  const c = open();
  await r.del("live");
  await r.xadd("live", "*", "seed", "1");
  const waiting = c.xread("BLOCK", 5000, "COUNT", 1, "STREAMS", "live", "$");
  await sleep(200);
  await r.xadd("live", "*", "late", "1");
  const got = await waiting;
  if (!got) throw new Error("XREAD BLOCK $ was not woken");
});

// ---------------------------------------------------------------------------
// SCAN, as a stream: ioredis's scanStream is the idiom, and it drives the
// cursor itself until the server says 0.
// ---------------------------------------------------------------------------

await check("scan_stream", async () => {
  await r.flushdb();
  const pipe = r.pipeline();
  for (let i = 0; i < 400; i++) pipe.set(`scan:${i}`, i);
  await pipe.exec();
  const seen = new Set();
  await new Promise((resolve, reject) => {
    const stream = r.scanStream({ match: "scan:*", count: 17 });
    stream.on("data", (keys) => keys.forEach((k) => seen.add(k)));
    stream.on("end", resolve);
    stream.on("error", reject);
  });
  eq(seen.size, 400);
});

await check("hscan_stream", async () => {
  await r.del("bigh");
  const pipe = r.pipeline();
  for (let i = 0; i < 300; i++) pipe.hset("bigh", `f${i}`, i);
  await pipe.exec();
  let n = 0;
  await new Promise((resolve, reject) => {
    const stream = r.hscanStream("bigh", { count: 11 });
    stream.on("data", (kv) => (n += kv.length / 2));
    stream.on("end", resolve);
    stream.on("error", reject);
  });
  eq(n, 300);
});

// ---------------------------------------------------------------------------
// Error text and Lua.
// ---------------------------------------------------------------------------

await check("error_wrongtype", async () => {
  await r.set("etype", "v");
  await expectError(() => r.lpush("etype", "x"), "WRONGTYPE");
});

await check("error_unknown_command", async () => {
  await expectError(() => r.call("NOSUCHTHING"), "unknown command");
});

await check("error_arity", async () => {
  await expectError(() => r.call("GET"), "wrong number of arguments");
});

await check("error_not_integer", async () => {
  await r.set("nan", "abc");
  await expectError(() => r.incr("nan"), "not an integer");
});

await check("define_command_lua", async () => {
  // ioredis's defineCommand is how a Node application ships a server-side
  // script; it EVALs by SHA and falls back to EVAL.
  const c = open();
  c.defineCommand("setandget", {
    numberOfKeys: 1,
    lua: "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
  });
  eq(await c.setandget("luakey", "luaval"), "luaval");
});

// ---------------------------------------------------------------------------
// Cluster. ioredis's Cluster builds its slot table from CLUSTER SLOTS, refreshes
// it on MOVED, and follows ASK with an ASKING prelude -- on its own, with no
// help from the application.
// ---------------------------------------------------------------------------

let cluster = null;
try {
  cluster = new Redis.Cluster([{ host: CHOST, port: CPORT }], {
    clusterRetryStrategy: (times) => (times > 3 ? null : 200),
    redisOptions: { maxRetriesPerRequest: 2 },
  });
  cluster.on("error", () => {});
  await cluster.ping();
  result("cluster_connect", "PASS");
} catch (err) {
  result("cluster_connect", "FAIL", `${err.name}: ${err.message}`);
  cluster = null;
}

function needCluster() {
  if (!cluster) throw new Skip("no cluster client");
  return cluster;
}

await check("cluster_slots_discovered", async () => {
  const c = needCluster();
  const nodes = c.nodes("master");
  if (nodes.length !== 3) throw new Error(`discovered ${nodes.length} masters, want 3`);
});

await check("cluster_routed_set_get", async () => {
  const c = needCluster();
  for (let i = 0; i < 64; i++) await c.set(`icl:${i}`, i);
  for (let i = 0; i < 64; i++) eq(await c.get(`icl:${i}`), String(i));
});

await check("cluster_hashtag_multikey", async () => {
  const c = needCluster();
  await c.mset("{tag}.a", "1", "{tag}.b", "2");
  eq(await c.mget("{tag}.a", "{tag}.b"), ["1", "2"]);
});

await check("cluster_crossslot_error", async () => {
  const c = needCluster();
  await expectError(() => c.mget("plain-a", "plain-b"), "CROSSSLOT");
});

await check("cluster_pipeline", async () => {
  const c = needCluster();
  const pipe = c.pipeline();
  for (let i = 0; i < 32; i++) pipe.set(`{icp}.${i}`, i);
  const got = await pipe.exec();
  eq(got.length, 32);
  eq(await c.get("{icp}.31"), "31");
});

await check("cluster_scan_per_node", async () => {
  const c = needCluster();
  let total = 0;
  for (const node of c.nodes("master")) {
    let cursor = "0";
    do {
      const [next, keys] = await node.scan(cursor, "MATCH", "icl:*", "COUNT", 20);
      cursor = next;
      total += keys.length;
    } while (cursor !== "0");
  }
  if (total < 64) throw new Error(`SCAN across the cluster found ${total} of 64`);
});

process.stdout.write("# done\n");
for (const c of clients) c.disconnect();
if (cluster) cluster.disconnect();
process.exit(0);
