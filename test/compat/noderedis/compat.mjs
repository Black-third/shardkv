// node-redis (the official Node client) against shardkv, and against a real
// Redis for reference.
//
// node-redis is the strictest of the four on the handshake: it sends
// CLIENT SETINFO on every connect unprompted, and it is the only one of the four
// that speaks RESP3 as a first-class mode rather than an option. It also builds
// its own command surface from a generated table, so a reply of the wrong *type*
// (an integer where a bulk string belongs) throws inside the client rather than
// producing a wrong value.

import { createClient, createCluster } from "redis";
import { createRequire } from "node:module";

const HOST = process.env.SHARDKV_HOST ?? "127.0.0.1";
const PORT = Number(process.env.SHARDKV_PORT ?? 6380);
const CHOST = process.env.CLUSTER_HOST ?? HOST;
const CPORT = Number(process.env.CLUSTER_PORT ?? 6380);
const TARGET = process.env.TARGET ?? "?";

const version = createRequire(import.meta.url)("redis/package.json").version;

function result(feature, status, detail = "") {
  const flat = String(detail).replace(/\s+/g, " ").slice(0, 220);
  process.stdout.write(`::RESULT::${feature}::${status}::${flat}\n`);
}

class Skip extends Error {}

const opened = [];
async function open(extra = {}) {
  const c = createClient({ socket: { host: HOST, port: PORT }, ...extra });
  c.on("error", () => {});
  await c.connect();
  opened.push(c);
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
    if (!String(err.message).toLowerCase().includes(needle.toLowerCase())) {
      throw new Error(`error was ${JSON.stringify(err.message)}, wanted ${needle}`);
    }
    return;
  }
  throw new Error(`no error raised; wanted ${needle}`);
}

process.stdout.write(`# node-redis ${version} -> ${TARGET} at ${HOST}:${PORT}\n`);
result("library_version", "PASS", `node-redis ${version}`);

// ---------------------------------------------------------------------------
// The handshake, first, because node-redis does more of it than the others and
// a rejection here means the client never becomes usable.
// ---------------------------------------------------------------------------

let r = null;
await check("handshake_default_connect", async () => {
  // Defaults only. node-redis sends CLIENT SETINFO LIB-NAME and LIB-VER here
  // without being asked; if the server's answer upsets it, connect() rejects
  // and nothing else in this file can run.
  r = await open();
  eq(await r.ping(), "PONG");
});

if (r === null) {
  // Fall back to a client with the library-identification step disabled, so the
  // rest of the matrix still has data even when the handshake is the bug.
  await check("handshake_without_client_info", async () => {
    r = await open({ disableClientInfo: true });
    eq(await r.ping(), "PONG");
  });
}
if (r === null) {
  result("handshake_fatal", "FAIL", "no usable connection; skipping the rest");
  process.stdout.write("# aborted\n");
  process.exit(0);
}

await r.flushAll();

await check("handshake_resp3", async () => {
  const c = await open({ RESP: 3 });
  eq(await c.ping(), "PONG");
  const hello = await c.sendCommand(["HELLO", "3"]);
  // In RESP3 node-redis surfaces a map as a JS Map or a plain object depending
  // on its typeMapping; either way it must not be a flat array.
  if (Array.isArray(hello)) throw new Error("HELLO 3 replied with a flat array");
});

await check("handshake_client_name", async () => {
  const c = await open({ name: "node-redis-app" });
  eq(await c.clientGetName(), "node-redis-app");
});

await check("client_setinfo", async () => {
  eq(await r.sendCommand(["CLIENT", "SETINFO", "LIB-NAME", "node-redis-compat"]), "OK");
  const info = await r.sendCommand(["CLIENT", "INFO"]);
  if (!String(info).includes("lib-name=node-redis-compat")) {
    throw new Error(`CLIENT INFO after SETINFO: ${info}`);
  }
});

await check("command_docs", async () => {
  const docs = await r.sendCommand(["COMMAND", "DOCS", "GET"]);
  if (!docs || (Array.isArray(docs) && docs.length === 0)) {
    throw new Error("COMMAND DOCS GET was empty");
  }
});

await check("config_get", async () => {
  const got = await r.configGet("*");
  if (!got || Object.keys(got).length === 0) throw new Error("CONFIG GET * was empty");
});

await check("info_sections", async () => {
  const info = await r.info();
  for (const section of ["# Server", "# Clients", "# Stats", "# Replication"]) {
    if (!info.includes(section)) throw new Error(`INFO has no ${section}`);
  }
});

// ---------------------------------------------------------------------------
// Types.
// ---------------------------------------------------------------------------

await check("type_string", async () => {
  eq(await r.set("s", "hello"), "OK");
  eq(await r.get("s"), "hello");
  eq(await r.append("s", "!"), 6);
  eq(await r.getRange("s", 0, 3), "hell");
  eq(await r.incrBy("counter", 5), 5);
  eq(await r.mSet({ a: "1", b: "2" }), "OK");
  eq(await r.mGet(["a", "b", "missing"]), ["1", "2", null]);
  eq(await r.exists(["a", "b", "missing"]), 2);
});

await check("type_expiry", async () => {
  await r.set("e", "v", { EX: 100 });
  const ttl = await r.ttl("e");
  if (ttl < 90 || ttl > 100) throw new Error(`TTL = ${ttl}`);
  // node-redis 5 hands back the integer the server sent; 4 coerced it to a
  // boolean. Either is fine -- the server's reply is :1 in both cases.
  const persisted = await r.persist("e");
  if (persisted !== true && persisted !== 1) throw new Error(`PERSIST = ${persisted}`);
  eq(await r.ttl("e"), -1);
  eq(await r.ttl("nope"), -2);
});

await check("type_list_hash_set_zset", async () => {
  await r.del(["l", "h", "st", "z"]);
  eq(await r.rPush("l", ["a", "b", "c"]), 3);
  eq(await r.lRange("l", 0, -1), ["a", "b", "c"]);
  eq(await r.lPop("l"), "a");
  eq(await r.hSet("h", { f: "v", g: "w" }), 2);
  eq(await r.hGetAll("h"), { f: "v", g: "w" });
  eq(await r.sAdd("st", ["x", "y"]), 2);
  eq((await r.sMembers("st")).sort(), ["x", "y"]);
  eq(await r.zAdd("z", [{ score: 1, value: "a" }, { score: 2, value: "b" }]), 2);
  eq(await r.zRange("z", 0, -1), ["a", "b"]);
  eq(await r.zScore("z", "b"), 2);
  const withScores = await r.zRangeWithScores("z", 0, -1);
  eq(withScores, [{ value: "a", score: 1 }, { value: "b", score: 2 }]);
});

await check("type_binary_safe", async () => {
  // A value containing NUL, CR and LF: only the length prefix can carry it, so
  // this is really a test of the writer's framing. The bytes are read back with
  // GETBIT rather than as a Buffer, which keeps the check independent of how
  // this version of node-redis spells "return binary".
  const payload = Buffer.from([0, 13, 10, 255, 65, 0]);
  await r.set("bin", payload);
  eq(await r.strLen("bin"), payload.length);
  eq(await r.getBit("bin", 0), 0);        // first byte is NUL
  for (let bit = 24; bit < 32; bit++) {   // fourth byte is 0xFF
    eq(await r.getBit("bin", bit), 1, `bit ${bit}: `);
  }
  eq(await r.getRange("bin", 4, 4), "A");
});

// ---------------------------------------------------------------------------
// Pipelines and transactions.
// ---------------------------------------------------------------------------

await check("multi_exec", async () => {
  await r.del("t");
  const got = await r.multi().rPush("t", "a").rPush("t", "b").lLen("t").exec();
  eq(got, [1, 2, 2]);
});

await check("multi_exec_partial_error", async () => {
  await r.del("te");
  await r.set("te", "str");
  // node-redis 5 rejects exec() when any queued command came back an error,
  // and carries the per-command replies on the rejection. What is being checked
  // is the server's side: EXEC ran, the INCR failed with the right text, and the
  // SET that followed it still applied.
  let replies;
  try {
    replies = await r.multi().incr("te").set("te2", "ok").exec();
  } catch (err) {
    replies = err.replies;
    if (!replies) throw err;
  }
  const first = replies[0];
  const text = first instanceof Error ? first.message : String(first);
  if (!text.includes("not an integer")) {
    throw new Error(`INCR on a string inside EXEC: ${JSON.stringify(text)}`);
  }
  eq(replies[1], "OK");
  eq(await r.get("te2"), "ok");
});

await check("watch_aborts_on_conflict", async () => {
  const c = await open();
  await c.set("w", "0");
  await c.watch("w");
  await r.set("w", "changed");
  try {
    await c.multi().set("w", "mine").exec();
  } catch (err) {
    // node-redis raises WatchError when EXEC comes back nil.
    if (!/watch/i.test(err.name + err.message)) throw err;
    eq(await r.get("w"), "changed");
    return;
  }
  throw new Error("EXEC succeeded after a watched key changed");
});

await check("unbuffered_pipeline_concurrency", async () => {
  // node-redis batches whatever is issued in one tick onto the socket without
  // MULTI. 500 concurrent INCRs must all land, in order, on one connection.
  const c = await open();
  await c.del("pipelined");
  const got = await Promise.all(Array.from({ length: 500 }, () => c.incr("pipelined")));
  eq(got.length, 500);
  eq(await c.get("pipelined"), "500");
});

// ---------------------------------------------------------------------------
// Pub/Sub, including the RESP3 case where the subscriber can still run ordinary
// commands on the same connection.
// ---------------------------------------------------------------------------

await check("pubsub_message", async () => {
  const sub = r.duplicate();
  sub.on("error", () => {});
  await sub.connect();
  const got = new Promise((resolve) => {
    sub.subscribe("news", (message, channel) => resolve([channel, message]));
  });
  await sleep(150);
  eq(await r.publish("news", "hello"), 1);
  eq(await got, ["news", "hello"]);
  await sub.quit();
});

await check("pubsub_pattern", async () => {
  const sub = r.duplicate();
  sub.on("error", () => {});
  await sub.connect();
  const got = new Promise((resolve) => {
    sub.pSubscribe("news.*", (message, channel) => resolve([channel, message]));
  });
  await sleep(150);
  await r.publish("news.sport", "goal");
  eq(await got, ["news.sport", "goal"]);
  await sub.quit();
});

await check("pubsub_resp3_commands_while_subscribed", async () => {
  // RESP3 types pushes separately from replies, so one connection can do both.
  // node-redis allows it only on a RESP3 connection, which makes this a direct
  // test of the server tagging its pushes correctly.
  const c = await open({ RESP: 3 });
  await c.set("readable-while-subscribed", "yes");
  const got = new Promise((resolve) => {
    c.subscribe("resp3", (message) => resolve(message));
  });
  await sleep(150);
  eq(await c.get("readable-while-subscribed"), "yes");
  await r.publish("resp3", "pushed");
  eq(await got, "pushed");
  await c.unsubscribe("resp3");
});

await check("keyspace_notifications", async () => {
  const sub = r.duplicate();
  sub.on("error", () => {});
  await sub.connect();
  const got = new Promise((resolve) => {
    sub.pSubscribe("__keyevent@0__:set", (message, channel) => resolve([channel, message]));
  });
  await sleep(150);
  await r.set("notified", "1");
  const [channel, message] = await got;
  if (!channel.endsWith(":set")) throw new Error(`channel was ${channel}`);
  eq(message, "notified");
  await sub.quit();
});

// ---------------------------------------------------------------------------
// Blocking commands, on their own connection as an application would.
// ---------------------------------------------------------------------------

await check("blocking_blpop", async () => {
  const iso = await open();
  await iso.del("queue");
  const waiting = iso.blPop("queue", 5);
  await sleep(200);
  await r.rPush("queue", "job");
  const got = await waiting;
  eq(got, { key: "queue", element: "job" });
});

await check("blocking_timeout", async () => {
  const iso = await open();
  eq(await iso.blPop("never-pushed", 1), null);
});

// ---------------------------------------------------------------------------
// Streams.
// ---------------------------------------------------------------------------

await check("streams_consumer_group", async () => {
  await r.del("orders");
  await r.xAdd("orders", "*", { item: "a" });
  await r.xAdd("orders", "*", { item: "b" });
  const created = await r.xGroupCreate("orders", "g", "0");
  if (created !== true && created !== "OK") throw new Error(`XGROUP CREATE = ${created}`);
  const got = await r.xReadGroup("g", "worker", { key: "orders", id: ">" }, { COUNT: 1 });
  if (!got || got.length !== 1) throw new Error(`XREADGROUP: ${JSON.stringify(got)}`);
  const id = got[0].messages[0].id;
  eq(got[0].messages[0].message, { item: "a" });
  eq(await r.xAck("orders", "g", id), 1);
  const pending = await r.xPending("orders", "g");
  eq(pending.pending, 0);
});

await check("streams_xinfo", async () => {
  await r.del("xi");
  await r.xAdd("xi", "*", { a: "1" });
  await r.xGroupCreate("xi", "g", "0");
  const info = await r.xInfoStream("xi");
  eq(info.length, 1);
  const groups = await r.xInfoGroups("xi");
  eq(groups[0].name, "g");
});

// ---------------------------------------------------------------------------
// SCAN, through node-redis's async iterator.
// ---------------------------------------------------------------------------

await check("scan_iterator", async () => {
  await r.flushDb();
  const multi = r.multi();
  for (let i = 0; i < 400; i++) multi.set(`scan:${i}`, String(i));
  await multi.exec();
  const seen = new Set();
  for await (const batch of r.scanIterator({ MATCH: "scan:*", COUNT: 17 })) {
    // v4 yields one key, v5 yields an array of keys.
    if (Array.isArray(batch)) batch.forEach((k) => seen.add(k));
    else seen.add(batch);
  }
  eq(seen.size, 400);
});

await check("hscan_iterator", async () => {
  await r.del("bigh");
  const multi = r.multi();
  for (let i = 0; i < 300; i++) multi.hSet("bigh", `f${i}`, String(i));
  await multi.exec();
  let n = 0;
  for await (const batch of r.hScanIterator("bigh", { COUNT: 11 })) {
    n += Array.isArray(batch) ? batch.length : 1;
  }
  eq(n, 300);
});

// ---------------------------------------------------------------------------
// Error text.
// ---------------------------------------------------------------------------

await check("error_wrongtype", async () => {
  await r.set("etype", "v");
  await expectError(() => r.lPush("etype", "x"), "WRONGTYPE");
});

await check("error_unknown_command", async () => {
  await expectError(() => r.sendCommand(["NOSUCHTHING"]), "unknown command");
});

await check("error_arity", async () => {
  await expectError(() => r.sendCommand(["GET"]), "wrong number of arguments");
});

await check("error_not_integer", async () => {
  await r.set("nan", "abc");
  await expectError(() => r.incr("nan"), "not an integer");
});

// ---------------------------------------------------------------------------
// Cluster.
// ---------------------------------------------------------------------------

let cluster = null;
try {
  cluster = createCluster({
    rootNodes: [{ socket: { host: CHOST, port: CPORT } }],
    defaults: { socket: { reconnectStrategy: false } },
  });
  cluster.on("error", () => {});
  await cluster.connect();
  result("cluster_connect", "PASS");
} catch (err) {
  result("cluster_connect", "FAIL", `${err.name}: ${err.message}`);
  cluster = null;
}

function needCluster() {
  if (!cluster) throw new Skip("no cluster client");
  return cluster;
}

await check("cluster_routed_set_get", async () => {
  const c = needCluster();
  for (let i = 0; i < 64; i++) await c.set(`ncl:${i}`, String(i));
  for (let i = 0; i < 64; i++) eq(await c.get(`ncl:${i}`), String(i));
});

await check("cluster_hashtag_multikey", async () => {
  const c = needCluster();
  await c.mSet({ "{ntag}.a": "1", "{ntag}.b": "2" });
  eq(await c.mGet(["{ntag}.a", "{ntag}.b"]), ["1", "2"]);
});

await check("cluster_crossslot_error", async () => {
  const c = needCluster();
  await expectError(() => c.mGet(["plain-a", "plain-b"]), "CROSSSLOT");
});

await check("cluster_multi_one_slot", async () => {
  const c = needCluster();
  const got = await c.multi().set("{ntx}.a", "1").set("{ntx}.b", "2").exec();
  eq(got, ["OK", "OK"]);
});

process.stdout.write("# done\n");
for (const c of opened) {
  try {
    await c.quit();
  } catch {
    /* already gone */
  }
}
if (cluster) await cluster.close().catch(() => cluster.quit().catch(() => {}));
process.exit(0);
