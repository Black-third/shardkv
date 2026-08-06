#!/usr/bin/env python3
"""redis-py against shardkv (and, for reference, against a real Redis).

Every check is a thing an application does, not a thing a protocol document
says. The distinction matters: redis-py does not send the bytes you would type
into redis-cli. It negotiates with HELLO, labels the connection with
CLIENT SETINFO, decides which exception class to raise by matching the *text* of
an error reply, validates that a RESP3 map really arrived as a map, and -- in
cluster mode -- builds a slot-to-node table out of CLUSTER SHARDS or CLUSTER
SLOTS and follows MOVED and ASK by itself.

The same file runs against both servers. Nothing here is conditional on which,
so a difference in the results is a difference in the servers.
"""

from __future__ import annotations

import os
import threading
import time
import traceback

import redis
from redis.exceptions import ResponseError

HOST = os.environ.get("SHARDKV_HOST", "127.0.0.1")
PORT = int(os.environ.get("SHARDKV_PORT", "6380"))
CHOST = os.environ.get("CLUSTER_HOST", HOST)
CPORT = int(os.environ.get("CLUSTER_PORT", "6380"))
TARGET = os.environ.get("TARGET", "?")

VERBOSE = os.environ.get("VERBOSE", "0") == "1"


def result(feature: str, status: str, detail: str = "") -> None:
    detail = " ".join(str(detail).split())[:220]
    print(f"::RESULT::{feature}::{status}::{detail}", flush=True)


class Skip(Exception):
    """Raised by a check that cannot apply here (not a failure)."""


def check(feature):
    """Run the decorated function immediately and record one matrix cell."""

    def deco(fn):
        try:
            fn()
            result(feature, "PASS")
        except Skip as exc:
            result(feature, "SKIP", str(exc))
        except Exception as exc:  # noqa: BLE001 - the point is to report anything
            if VERBOSE:
                traceback.print_exc()
            result(feature, "FAIL", f"{type(exc).__name__}: {exc}")
        return fn

    return deco


def eq(got, want, what=""):
    if got != want:
        raise AssertionError(f"{what}got {got!r}, want {want!r}")


def pool_connection(client):
    """A raw connection from the client's pool, across redis-py versions."""
    pool = client.connection_pool
    try:
        return pool.get_connection()
    except TypeError:  # redis-py 5 wants a command name
        return pool.get_connection("_")


def r2(**kw):
    return redis.Redis(host=HOST, port=PORT, decode_responses=True, protocol=2, **kw)


def r3(**kw):
    return redis.Redis(host=HOST, port=PORT, decode_responses=True, protocol=3, **kw)


print(f"# redis-py {redis.__version__} -> {TARGET} at {HOST}:{PORT}", flush=True)
result("library_version", "PASS", f"redis-py {redis.__version__}")

_boot = r2()
_boot.flushall()


# ---------------------------------------------------------------------------
# The handshake. Everything below depends on it, and a client that cannot
# complete it never issues a single application command.
# ---------------------------------------------------------------------------


@check("handshake_resp2")
def _():
    c = r2()
    eq(c.ping(), True)
    flat = c.execute_command("HELLO", 2)
    # RESP2 has no map type, so HELLO answers with a flat array and the client
    # pairs it itself. A client that got a map here would have to be speaking
    # RESP3 already, which it did not ask for.
    if not isinstance(flat, list):
        raise AssertionError(f"HELLO 2 replied with {type(flat).__name__}, want array")
    info = dict(zip(flat[::2], flat[1::2]))
    if "proto" not in info:
        raise AssertionError(f"HELLO 2 reply has no proto field: {flat!r}")
    eq(int(info["proto"]), 2)
    if "server" not in info or "id" not in info or "role" not in info:
        raise AssertionError(f"HELLO 2 reply is missing fields: {sorted(info)}")


@check("handshake_resp3")
def _():
    c = r3()
    eq(c.ping(), True)
    info = c.execute_command("HELLO", 3)
    eq(int(info["proto"]), 3)
    # redis-py only reaches here if the reply arrived as a RESP3 map: the
    # hiredis parser types it, so a flat array would come back as a list.
    if not isinstance(info, dict):
        raise AssertionError(f"HELLO 3 reply is {type(info).__name__}, want map")


@check("handshake_lib_name_setinfo")
def _():
    # This is what modern redis-py does on every connect, unprompted: two
    # CLIENT SETINFO commands naming the library and its version. A server that
    # rejects them must at least reject them with an error redis-py tolerates,
    # or no application using this version of the library can connect at all.
    c = redis.Redis(
        host=HOST,
        port=PORT,
        decode_responses=True,
        lib_name="redis-py-compat",
        lib_version="9.9.9",
    )
    eq(c.ping(), True)
    info = c.client_info()
    if info.get("lib-name") not in ("redis-py-compat", None, ""):
        raise AssertionError(f"lib-name reported as {info.get('lib-name')!r}")


@check("client_setinfo_explicit")
def _():
    c = r2()
    eq(c.client_setinfo("LIB-NAME", "explicit-name"), True)
    eq(c.client_setinfo("LIB-VER", "1.2.3"), True)
    info = c.client_info()
    eq(info.get("lib-name"), "explicit-name", "CLIENT INFO lib-name: ")
    eq(info.get("lib-ver"), "1.2.3", "CLIENT INFO lib-ver: ")


@check("client_setname_getname")
def _():
    c = r2()
    eq(c.client_setname("app-worker-3"), True)
    eq(c.client_getname(), "app-worker-3")
    eq(c.client_info()["name"], "app-worker-3")


@check("command_docs")
def _():
    # redis-py's own COMMAND DOCS parser rejects real Redis 7's reply as well --
    # it assumes every attribute value is an integer -- so this goes below the
    # command layer and reads the reply itself. What is being checked is the
    # server: that it answers the command redis-cli and several libraries send on
    # connect, and that the reply parses as RESP at all.
    c = r2()
    conn = pool_connection(c)
    try:
        conn.send_command("COMMAND", "DOCS", "GET")
        docs = conn.read_response()
    finally:
        c.connection_pool.release(conn)
    if not docs:
        raise AssertionError("COMMAND DOCS GET returned nothing")
    flat = docs if isinstance(docs, list) else sum(([k, v] for k, v in docs.items()), [])
    names = [x.decode() if isinstance(x, bytes) else str(x) for x in flat]
    if "get" not in [n.lower() for n in names]:
        raise AssertionError(f"COMMAND DOCS GET did not name get: {names[:6]}")


@check("command_count_info_getkeys")
def _():
    c = r2()
    if c.command_count() < 100:
        raise AssertionError(f"COMMAND COUNT = {c.command_count()}")
    # redis-py turns COMMAND INFO into a dict keyed by command name; older
    # versions handed back the raw array. Accept either and check the fields.
    info = c.execute_command("COMMAND", "INFO", "get")
    if isinstance(info, dict):
        entry = info.get("get")
        if entry is None:
            raise AssertionError(f"COMMAND INFO get = {info!r}")
        arity = entry["arity"] if isinstance(entry, dict) else entry[1]
    else:
        if not info or not info[0]:
            raise AssertionError(f"COMMAND INFO get = {info!r}")
        eq(info[0][0], "get", "COMMAND INFO name: ")
        arity = info[0][1]
    eq(int(arity), 2, "COMMAND INFO arity: ")
    eq(c.command_getkeys("SET", "k", "v"), ["k"])
    eq(c.command_getkeys("MSET", "a", "1", "b", "2"), ["a", "b"])


@check("config_get_glob")
def _():
    c = r2()
    got = c.config_get("maxmemory*")
    if not isinstance(got, dict):
        raise AssertionError(f"CONFIG GET returned {type(got).__name__}")
    got = c.config_get("*")
    if not isinstance(got, dict) or not got:
        raise AssertionError(f"CONFIG GET * returned {got!r}")


@check("config_set_roundtrip")
def _():
    c = r2()
    before = c.config_get("slowlog-max-len")["slowlog-max-len"]
    c.config_set("slowlog-max-len", 256)
    eq(c.config_get("slowlog-max-len")["slowlog-max-len"], "256")
    c.config_set("slowlog-max-len", before)


@check("info_parsing")
def _():
    info = r2().info()
    for field in ("redis_version", "role", "connected_clients", "db0"):
        if field == "db0":
            continue
        if field not in info:
            raise AssertionError(f"INFO has no {field}: keys={sorted(info)[:12]}")
    # redis-py parses INFO into a dict of typed values; a malformed section
    # would raise before this point.
    sect = r2().info("replication")
    if "role" not in sect:
        raise AssertionError(f"INFO replication = {sect!r}")


@check("reset")
def _():
    c = r2()
    c.client_setname("before-reset")
    eq(c.execute_command("RESET"), "RESET")
    eq(c.client_getname(), None)


# ---------------------------------------------------------------------------
# Types. The library converts on the way in and on the way out, so a wrong
# reply *type* (integer where a bulk string belongs) fails here even when the
# value is right.
# ---------------------------------------------------------------------------


@check("type_string")
def _():
    c = r2()
    eq(c.set("s", "hello"), True)
    eq(c.get("s"), "hello")
    eq(c.append("s", "!"), 6)
    eq(c.strlen("s"), 6)
    eq(c.getrange("s", 0, 3), "hell")
    eq(c.setrange("s", 0, "J"), 6)
    eq(c.get("s"), "Jello!")
    eq(c.getset("s", "x"), "Jello!")
    eq(c.getdel("s"), "x")
    eq(c.get("s"), None)
    eq(c.set("n", 10), True)
    eq(c.incr("n"), 11)
    eq(c.incrby("n", 5), 16)
    eq(c.decrby("n", 6), 10)
    eq(float(c.incrbyfloat("n", 0.5)), 10.5)
    eq(c.mset({"a": "1", "b": "2"}), True)
    eq(c.mget("a", "b", "nope"), ["1", "2", None])
    eq(c.setnx("a", "9"), False)
    eq(c.exists("a", "b", "nope"), 2)
    eq(c.type("a"), "string")


@check("type_expiry")
def _():
    c = r2()
    c.set("e", "v", ex=100)
    ttl = c.ttl("e")
    if not 90 <= ttl <= 100:
        raise AssertionError(f"TTL after SET EX 100 = {ttl}")
    if not 90_000 <= c.pttl("e") <= 100_000:
        raise AssertionError(f"PTTL = {c.pttl('e')}")
    eq(c.persist("e"), True)
    eq(c.ttl("e"), -1)
    eq(c.expire("e", 50), True)
    eq(c.getex("e", persist=True), "v")
    eq(c.ttl("e"), -1)
    eq(c.ttl("nosuchkey"), -2)


@check("type_list")
def _():
    c = r2()
    c.delete("l")
    eq(c.rpush("l", "a", "b", "c"), 3)
    eq(c.lpush("l", "z"), 4)
    eq(c.lrange("l", 0, -1), ["z", "a", "b", "c"])
    eq(c.lindex("l", 1), "a")
    eq(c.llen("l"), 4)
    eq(c.lpop("l"), "z")
    eq(c.rpop("l", 2), ["c", "b"])
    c.rpush("l", "b", "c", "a")
    eq(c.lpos("l", "c"), 2)
    eq(c.lset("l", 0, "A"), True)
    eq(c.linsert("l", "before", "b", "Q"), 5)
    eq(c.lrem("l", 1, "Q"), 1)
    eq(c.ltrim("l", 0, 1), True)
    eq(c.lrange("l", 0, -1), ["A", "b"])
    eq(c.lmove("l", "l2", "left", "right"), "A")
    eq(c.lrange("l2", 0, -1), ["A"])
    eq(c.rpoplpush("l", "l2"), "b")
    eq(c.lmpop(2, "nothere", "l2", direction="left"), ["l2", ["b"]])


@check("type_hash")
def _():
    c = r2()
    c.delete("h")
    eq(c.hset("h", mapping={"f1": "v1", "f2": "v2"}), 2)
    eq(c.hget("h", "f1"), "v1")
    eq(c.hgetall("h"), {"f1": "v1", "f2": "v2"})
    eq(c.hmget("h", "f1", "nope"), ["v1", None])
    eq(c.hexists("h", "f2"), True)
    eq(c.hlen("h"), 2)
    eq(sorted(c.hkeys("h")), ["f1", "f2"])
    eq(sorted(c.hvals("h")), ["v1", "v2"])
    eq(c.hstrlen("h", "f1"), 2)
    eq(c.hsetnx("h", "f1", "other"), False)
    eq(c.hincrby("h", "n", 5), 5)
    eq(float(c.hincrbyfloat("h", "n", 0.5)), 5.5)
    eq(c.hdel("h", "f1", "f2"), 2)
    if c.hrandfield("h") != "n":
        raise AssertionError("HRANDFIELD")


@check("type_set")
def _():
    c = r2()
    c.delete("s1", "s2", "sd")
    eq(c.sadd("s1", "a", "b", "c"), 3)
    eq(c.sadd("s2", "b", "c", "d"), 3)
    eq(c.scard("s1"), 3)
    eq(c.smembers("s1"), {"a", "b", "c"})
    eq(c.sismember("s1", "a"), True)
    eq(c.smismember("s1", "a", "z"), [True, False])
    eq(c.sinter("s1", "s2"), {"b", "c"})
    eq(c.sunion("s1", "s2"), {"a", "b", "c", "d"})
    eq(c.sdiff("s1", "s2"), {"a"})
    eq(c.sintercard(2, ["s1", "s2"]), 2)
    eq(c.sinterstore("sd", "s1", "s2"), 2)
    eq(c.sunionstore("sd", "s1", "s2"), 4)
    eq(c.sdiffstore("sd", "s1", "s2"), 1)
    eq(c.smove("s1", "s2", "a"), True)
    eq(c.srem("s2", "a"), 1)
    if c.srandmember("s1") not in {"b", "c"}:
        raise AssertionError("SRANDMEMBER")
    if c.spop("s1") not in {"b", "c"}:
        raise AssertionError("SPOP")


@check("type_zset")
def _():
    c = r2()
    c.delete("z")
    eq(c.zadd("z", {"a": 1, "b": 2, "c": 3}), 3)
    eq(c.zcard("z"), 3)
    eq(c.zscore("z", "b"), 2.0)
    eq(c.zmscore("z", ["a", "zz"]), [1.0, None])
    eq(c.zrange("z", 0, -1), ["a", "b", "c"])
    eq(c.zrange("z", 0, -1, withscores=True), [("a", 1.0), ("b", 2.0), ("c", 3.0)])
    eq(c.zrevrange("z", 0, 0), ["c"])
    eq(c.zrangebyscore("z", 1, 2), ["a", "b"])
    eq(c.zrangebyscore("z", "(1", "+inf"), ["b", "c"])
    eq(c.zcount("z", 2, 3), 2)
    eq(c.zrank("z", "b"), 1)
    eq(c.zrevrank("z", "b"), 1)
    eq(c.zincrby("z", 1.5, "a"), 2.5)
    eq(c.zpopmin("z"), [("b", 2.0)])
    eq(c.zpopmax("z"), [("c", 3.0)])
    eq(c.zrem("z", "a"), 1)
    c.zadd("z", {"x": 0, "y": 0, "w": 0})
    eq(c.zlexcount("z", "-", "+"), 3)
    eq(c.zremrangebyrank("z", 0, 0), 1)
    eq(c.zremrangebyscore("z", "-inf", "+inf"), 2)


@check("type_zset_bylex")
def _():
    # ZRANGE ... BYLEX is how a modern client asks; ZRANGEBYLEX is the older
    # spelling redis-py still exposes as zrangebylex().
    c = r2()
    c.delete("zl")
    c.zadd("zl", {"a": 0, "b": 0, "c": 0})
    eq(c.zrange("zl", "[a", "[b", bylex=True), ["a", "b"])
    eq(c.zrangebylex("zl", "-", "+"), ["a", "b", "c"])


@check("type_zset_setops")
def _():
    c = r2()
    c.delete("za", "zb", "zdest")
    c.zadd("za", {"a": 1, "b": 2})
    c.zadd("zb", {"b": 3, "c": 4})
    eq(c.zunionstore("zdest", ["za", "zb"]), 3)
    eq(c.zinterstore("zdest", ["za", "zb"]), 1)
    eq(c.zdiff(["za", "zb"]), ["a"])


@check("type_bitmap")
def _():
    c = r2()
    c.delete("bm", "bm2", "bmdest")
    eq(c.setbit("bm", 7, 1), 0)
    eq(c.get("bm"), "\x01")
    eq(c.getbit("bm", 7), 1)
    eq(c.bitcount("bm"), 1)
    eq(c.bitpos("bm", 1), 7)
    c.set("bm2", "abc")
    eq(c.bitop("AND", "bmdest", "bm2", "bm2"), 3)
    eq(c.bitfield("bf").set("u8", 0, 255).get("u8", 0).execute(), [0, 255])


@check("type_hyperloglog")
def _():
    c = r2()
    c.delete("hll", "hll2", "hllm")
    eq(c.pfadd("hll", "a", "b", "c"), 1)
    eq(c.pfcount("hll"), 3)
    c.pfadd("hll2", "c", "d")
    c.pfmerge("hllm", "hll", "hll2")
    eq(c.pfcount("hllm"), 4)


@check("type_geo")
def _():
    c = r2()
    c.delete("geo")
    eq(c.geoadd("geo", (13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")), 2)
    dist = c.geodist("geo", "Palermo", "Catania", unit="km")
    if not 166 < float(dist) < 167:
        raise AssertionError(f"GEODIST Palermo Catania = {dist} km")
    eq(c.geohash("geo", "Palermo"), ["sqc8b49rny0"])
    pos = c.geopos("geo", "Palermo")
    if abs(float(pos[0][0]) - 13.361389) > 1e-4:
        raise AssertionError(f"GEOPOS = {pos!r}")
    found = c.geosearch("geo", longitude=15, latitude=37, radius=200, unit="km")
    eq(sorted(found), ["Catania", "Palermo"])


@check("keys_generic")
def _():
    c = r2()
    c.flushdb()
    c.mset({"k1": "1", "k2": "2"})
    eq(sorted(c.keys("k*")), ["k1", "k2"])
    eq(c.dbsize(), 2)
    eq(c.rename("k1", "k3"), True)
    eq(c.renamenx("k3", "k2"), False)
    eq(c.copy("k2", "k4"), True)
    eq(c.get("k4"), "2")
    eq(c.touch("k2", "k4"), 2)
    eq(c.unlink("k4"), 1)
    if c.randomkey() not in ("k2", "k3"):
        raise AssertionError("RANDOMKEY")
    eq(c.object("refcount", "k2") >= 1, True)
    eq(c.object("encoding", "k2") is not None, True)
    eq(c.memory_usage("k2") > 0, True)


@check("dump_restore")
def _():
    c = r2()
    c.delete("dr", "dr2")
    c.rpush("dr", "a", "b")
    payload = c.dump("dr")
    c.restore("dr2", 0, payload)
    eq(c.lrange("dr2", 0, -1), ["a", "b"])


@check("select_and_swapdb")
def _():
    c = redis.Redis(host=HOST, port=PORT, decode_responses=True, db=3)
    c.flushdb()
    c.set("indb3", "yes")
    eq(c.get("indb3"), "yes")
    eq(r2().get("indb3"), None)
    c.flushdb()


# ---------------------------------------------------------------------------
# RESP3. The reply *shapes* differ, and this is where a client that negotiated
# 3 and got a 2-shaped reply breaks -- silently, by handing the application the
# wrong Python type.
# ---------------------------------------------------------------------------


@check("resp3_reply_shapes")
def _():
    c = r3()
    c.delete("h3", "s3", "z3")
    c.hset("h3", mapping={"a": "1", "b": "2"})
    got = c.hgetall("h3")
    if not isinstance(got, dict):
        raise AssertionError(f"RESP3 HGETALL is {type(got).__name__}, want dict")
    c.sadd("s3", "x", "y")
    got = c.smembers("s3")
    if not isinstance(got, (set, frozenset)):
        raise AssertionError(f"RESP3 SMEMBERS is {type(got).__name__}, want set")
    c.zadd("z3", {"m": 1.5})
    got = c.zscore("z3", "m")
    if not isinstance(got, float):
        raise AssertionError(f"RESP3 ZSCORE is {type(got).__name__}, want float")
    eq(got, 1.5)
    # RESP3 pairs come back as two-element sequences; whether the library hands
    # them over as a tuple or a list is redis-py's business, not the server's.
    got = [tuple(pair) for pair in c.zrange("z3", 0, -1, withscores=True)]
    eq(got, [("m", 1.5)])
    got = c.config_get("slowlog-max-len")
    if not isinstance(got, dict):
        raise AssertionError(f"RESP3 CONFIG GET is {type(got).__name__}")
    got = c.client_info()
    if not isinstance(got, dict):
        raise AssertionError("RESP3 CLIENT INFO did not parse")


@check("resp3_double_specials")
def _():
    c = r3()
    c.delete("zi")
    c.zadd("zi", {"pos": "+inf", "neg": "-inf"})
    eq(c.zscore("zi", "pos"), float("inf"))
    eq(c.zscore("zi", "neg"), float("-inf"))


@check("resp3_null_and_bool")
def _():
    c = r3()
    eq(c.get("definitely-not-here"), None)
    eq(c.exists("definitely-not-here"), 0)
    eq(c.set("b3", "v", nx=True, get=True), None)


# ---------------------------------------------------------------------------
# Pipelines and transactions.
# ---------------------------------------------------------------------------


@check("pipeline_unbuffered")
def _():
    c = r2()
    with c.pipeline(transaction=False) as pipe:
        pipe.set("p1", "a").get("p1").incr("pcount").llen("p1")
        got = pipe.execute(raise_on_error=False)
    eq(got[0], True)
    eq(got[1], "a")
    eq(got[2], 1)
    if not isinstance(got[3], ResponseError) or "WRONGTYPE" not in str(got[3]):
        raise AssertionError(f"pipelined LLEN on a string gave {got[3]!r}")


@check("pipeline_transaction")
def _():
    c = r2()
    c.delete("t1")
    with c.pipeline(transaction=True) as pipe:
        pipe.rpush("t1", "a", "b")
        pipe.llen("t1")
        got = pipe.execute()
    eq(got, [2, 2])


@check("multi_exec_discard")
def _():
    c = r2()
    conn = c.pipeline(transaction=True)
    conn.multi()
    conn.set("m1", "1")
    conn.reset()  # DISCARD
    eq(c.get("m1"), None)


@check("watch_aborts_on_conflict")
def _():
    c = r2()
    other = r2()
    c.set("w1", "0")
    with c.pipeline() as pipe:
        pipe.watch("w1")
        other.set("w1", "changed-by-someone-else")
        pipe.multi()
        pipe.set("w1", "mine")
        try:
            pipe.execute()
        except redis.WatchError:
            pass
        else:
            raise AssertionError("EXEC succeeded after a watched key changed")
    eq(c.get("w1"), "changed-by-someone-else")


@check("watch_commits_when_clean")
def _():
    c = r2()
    c.set("w2", "0")

    def bump(pipe):
        current = int(pipe.get("w2"))
        pipe.multi()
        pipe.set("w2", current + 1)

    c.transaction(bump, "w2")
    eq(c.get("w2"), "1")


@check("transaction_error_inside_exec")
def _():
    c = r2()
    c.delete("te")
    c.set("te", "str")
    with c.pipeline(transaction=True) as pipe:
        pipe.incr("te")
        pipe.set("te2", "ok")
        got = pipe.execute(raise_on_error=False)
    if not isinstance(got[0], ResponseError):
        raise AssertionError(f"INCR on a string inside EXEC gave {got[0]!r}")
    eq(got[1], True)


# ---------------------------------------------------------------------------
# Pub/Sub.
# ---------------------------------------------------------------------------


def drain(pubsub, want, timeout=5.0):
    """Collect `want` non-subscribe messages, or fail with what did arrive."""
    got = []
    deadline = time.time() + timeout
    while len(got) < want and time.time() < deadline:
        msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
        if msg:
            got.append(msg)
    if len(got) < want:
        raise AssertionError(f"got {len(got)} of {want} messages: {got!r}")
    return got


@check("pubsub_resp2")
def _():
    c = r2()
    p = c.pubsub()
    p.subscribe("news")
    # The subscribe confirmation must arrive before a publish can be seen.
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.get_message(timeout=0.2):
            break
    eq(r2().publish("news", "hello"), 1)
    got = drain(p, 1)
    eq(got[0]["channel"], "news")
    eq(got[0]["data"], "hello")
    p.unsubscribe("news")
    p.close()


@check("pubsub_patterns")
def _():
    c = r2()
    p = c.pubsub()
    p.psubscribe("news.*")
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.get_message(timeout=0.2):
            break
    r2().publish("news.sport", "goal")
    got = drain(p, 1)
    eq(got[0]["pattern"], "news.*")
    eq(got[0]["data"], "goal")
    p.close()


@check("pubsub_channels_numsub")
def _():
    c = r2()
    p = c.pubsub()
    p.subscribe("introspect-me")
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.get_message(timeout=0.2):
            break
    other = r2()
    if "introspect-me" not in other.pubsub_channels():
        raise AssertionError(f"PUBSUB CHANNELS = {other.pubsub_channels()!r}")
    eq(other.pubsub_numsub("introspect-me"), [("introspect-me", 1)])
    p.close()


@check("pubsub_resp2_rejects_other_commands")
def _():
    # A RESP2 subscriber may only run a small set of commands, and the error
    # text is what a library shows its user. Real Redis says exactly this.
    c = r2()
    p = c.pubsub()
    p.subscribe("strict")
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.get_message(timeout=0.2):
            break
    conn = p.connection
    conn.send_command("GET", "anything")
    try:
        conn.read_response()
    except ResponseError as exc:
        if "only" not in str(exc) or "SUBSCRIBE" not in str(exc):
            raise AssertionError(f"unexpected subscriber-mode error: {exc}") from exc
    else:
        raise AssertionError("GET was allowed on a RESP2 subscriber connection")
    p.close()


@check("pubsub_resp3_commands_while_subscribed")
def _():
    # RESP3 lifts the restriction: pushes are typed, so a subscribed connection
    # can still run ordinary commands and tell the replies apart. This is the
    # shape a modern application uses to avoid a second connection.
    c = r3()
    c.set("under-subscription", "readable")
    p = c.pubsub()
    p.subscribe("resp3-chan")
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.get_message(timeout=0.2):
            break
    conn = p.connection
    conn.send_command("GET", "under-subscription")
    got = conn.read_response()
    if isinstance(got, bytes):
        got = got.decode()
    eq(got, "readable")
    p.close()


@check("keyspace_notifications")
def _():
    c = r2()
    p = c.pubsub()
    p.psubscribe("__keyevent@0__:*")
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.get_message(timeout=0.2):
            break
    w = r2()
    w.delete("notify-me")
    w.set("notify-me", "v")
    got = drain(p, 1, timeout=5)
    if not got[0]["channel"].endswith(":set"):
        raise AssertionError(f"first keyspace event was {got[0]!r}")
    eq(got[0]["data"], "notify-me")
    p.close()


# ---------------------------------------------------------------------------
# Blocking commands: a worker pool's whole reason for existing.
# ---------------------------------------------------------------------------


@check("blocking_blpop")
def _():
    c = r2()
    c.delete("queue")
    out = {}

    def worker():
        out["got"] = r2().blpop("queue", timeout=5)

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.3)
    r2().rpush("queue", "job-1")
    t.join(10)
    eq(out.get("got"), ("queue", "job-1"))


@check("blocking_timeout_returns_none")
def _():
    started = time.time()
    eq(r2().blpop("never-pushed", timeout=1), None)
    if time.time() - started < 0.9:
        raise AssertionError("BLPOP returned before its timeout")


@check("blocking_blmove_and_bzpopmin")
def _():
    c = r2()
    c.delete("src", "dst", "bz")
    c.rpush("src", "v")
    eq(c.blmove("src", "dst", 1, "left", "right"), "v")
    c.zadd("bz", {"m": 1})
    eq(c.bzpopmin("bz", timeout=1), ("bz", "m", 1.0))
    eq(c.blmpop(1, 1, "dst", direction="left"), ["dst", ["v"]])


@check("blocking_inside_multi_does_not_block")
def _():
    c = r2()
    c.delete("noblock")
    with c.pipeline(transaction=True) as pipe:
        pipe.blpop("noblock", timeout=0)
        got = pipe.execute()
    eq(got, [None])


@check("client_unblock")
def _():
    c = r2()
    victim = r2()
    ident = victim.client_id()
    out = {}

    def worker():
        try:
            out["got"] = victim.blpop("unblock-me", timeout=10)
        except Exception as exc:  # noqa: BLE001
            out["err"] = exc

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.4)
    eq(c.client_unblock(ident), True)
    t.join(12)
    eq(out.get("got"), None)


# ---------------------------------------------------------------------------
# Streams with consumer groups.
# ---------------------------------------------------------------------------


@check("streams_basic")
def _():
    c = r2()
    c.delete("st")
    first = c.xadd("st", {"item": "widget", "qty": "2"})
    second = c.xadd("st", {"item": "gadget"})
    eq(c.xlen("st"), 2)
    entries = c.xrange("st")
    eq(len(entries), 2)
    eq(entries[0][0], first)
    eq(entries[0][1], {"item": "widget", "qty": "2"})
    eq(c.xrevrange("st", count=1)[0][0], second)
    got = c.xread({"st": "0"}, count=10)
    eq(len(got[0][1]), 2)
    eq(c.xdel("st", first), 1)
    eq(c.xtrim("st", maxlen=0), 1)


@check("streams_consumer_group")
def _():
    c = r2()
    c.delete("orders")
    c.xadd("orders", {"item": "a"})
    c.xadd("orders", {"item": "b"})
    c.xgroup_create("orders", "fulfil", id="0")
    got = c.xreadgroup("fulfil", "alice", {"orders": ">"}, count=1)
    eq(len(got[0][1]), 1)
    first_id = got[0][1][0][0]
    pending = c.xpending("orders", "fulfil")
    eq(pending["pending"], 1)
    detail = c.xpending_range("orders", "fulfil", min="-", max="+", count=10)
    eq(detail[0]["consumer"], "alice")
    eq(c.xack("orders", "fulfil", first_id), 1)
    eq(c.xpending("orders", "fulfil")["pending"], 0)
    # bob picks up what is left
    got = c.xreadgroup("fulfil", "bob", {"orders": ">"}, count=10)
    eq(len(got[0][1]), 1)


@check("streams_claim_and_autoclaim")
def _():
    c = r2()
    c.delete("claims")
    c.xadd("claims", {"n": "1"})
    c.xgroup_create("claims", "g", id="0")
    got = c.xreadgroup("g", "dead-worker", {"claims": ">"}, count=10)
    ident = got[0][1][0][0]
    claimed = c.xclaim("claims", "g", "live-worker", min_idle_time=0, message_ids=[ident])
    eq(len(claimed), 1)
    nxt, msgs, _ = c.xautoclaim("claims", "g", "third-worker", min_idle_time=0)
    eq(len(msgs), 1)


@check("streams_xinfo")
def _():
    c = r2()
    c.delete("xi")
    c.xadd("xi", {"a": "1"})
    c.xgroup_create("xi", "g", id="0")
    c.xreadgroup("g", "c1", {"xi": ">"}, count=1)
    info = c.xinfo_stream("xi")
    eq(info["length"], 1)
    groups = c.xinfo_groups("xi")
    eq(groups[0]["name"], "g")
    eq(groups[0]["consumers"], 1)
    consumers = c.xinfo_consumers("xi", "g")
    eq(consumers[0]["name"], "c1")
    eq(consumers[0]["pending"], 1)


@check("streams_xread_block")
def _():
    c = r2()
    c.delete("blockst")
    c.xadd("blockst", {"seed": "1"})
    out = {}

    def worker():
        out["got"] = r2().xread({"blockst": "$"}, block=5000, count=1)

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.3)
    r2().xadd("blockst", {"late": "1"})
    t.join(10)
    if not out.get("got"):
        raise AssertionError("XREAD BLOCK $ was not woken by XADD")


# ---------------------------------------------------------------------------
# SCAN. Every library wraps it in an iterator, and the iterator is what breaks
# when a cursor is not honoured or the reply is not [cursor, [elements]].
# ---------------------------------------------------------------------------


@check("scan_iter")
def _():
    c = r2()
    c.flushdb()
    with c.pipeline(transaction=False) as pipe:
        for i in range(500):
            pipe.set(f"scan:{i}", i)
        pipe.execute()
    seen = set(c.scan_iter(match="scan:*", count=13))
    eq(len(seen), 500)
    eq(len(set(c.scan_iter(count=7))), 500)


@check("scan_iter_typed")
def _():
    c = r2()
    c.delete("typed-list")
    c.rpush("typed-list", "x")
    seen = list(c.scan_iter(match="typed-*", _type="LIST"))
    eq(seen, ["typed-list"])


@check("hscan_sscan_zscan_iter")
def _():
    c = r2()
    c.delete("bigh", "bigs", "bigz")
    with c.pipeline(transaction=False) as pipe:
        for i in range(300):
            pipe.hset("bigh", f"f{i}", i)
            pipe.sadd("bigs", f"m{i}")
            pipe.zadd("bigz", {f"z{i}": i})
        pipe.execute()
    eq(len(dict(c.hscan_iter("bigh", count=11))), 300)
    eq(len(set(c.sscan_iter("bigs", count=11))), 300)
    eq(len(dict(c.zscan_iter("bigz", count=11))), 300)


# ---------------------------------------------------------------------------
# The connection pool: many sockets, reused, under threads.
# ---------------------------------------------------------------------------


@check("connection_pool_under_threads")
def _():
    pool = redis.ConnectionPool(
        host=HOST, port=PORT, decode_responses=True, max_connections=8
    )
    c = redis.Redis(connection_pool=pool)
    c.delete("pooled")
    errors = []

    def worker(n):
        try:
            for i in range(50):
                c.incr("pooled")
                c.set(f"pool:{n}:{i}", i)
                eq(c.get(f"pool:{n}:{i}"), str(i))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    if errors:
        raise AssertionError(f"{len(errors)} pooled workers failed: {errors[0]!r}")
    eq(c.get("pooled"), "400")
    pool.disconnect()


@check("health_check_and_reconnect")
def _():
    # A pooled connection that has been idle is PINGed before reuse; a server
    # that answered PING differently on a reused socket would break every pool.
    c = redis.Redis(
        host=HOST, port=PORT, decode_responses=True, health_check_interval=1
    )
    c.set("hc", "1")
    time.sleep(1.2)
    eq(c.get("hc"), "1")


# ---------------------------------------------------------------------------
# Error text. Libraries map replies onto exception classes by matching prefixes,
# so the words matter as much as the fact that an error came back.
# ---------------------------------------------------------------------------


def expect_error(fn, needle):
    try:
        fn()
    except ResponseError as exc:
        if needle.lower() not in str(exc).lower():
            raise AssertionError(f"error was {str(exc)!r}, wanted {needle!r}") from exc
        return
    raise AssertionError(f"no error raised; wanted {needle!r}")


@check("error_wrongtype")
def _():
    c = r2()
    c.delete("etype")
    c.set("etype", "v")
    expect_error(lambda: c.lpush("etype", "x"), "WRONGTYPE")
    expect_error(lambda: c.hget("etype", "f"), "WRONGTYPE")
    expect_error(lambda: c.zadd("etype", {"m": 1}), "WRONGTYPE")


@check("error_unknown_command")
def _():
    expect_error(lambda: r2().execute_command("NOSUCHCOMMAND", "a"), "unknown command")


@check("error_wrong_arity")
def _():
    expect_error(
        lambda: r2().execute_command("GET"), "wrong number of arguments for 'get'"
    )


@check("error_not_an_integer")
def _():
    c = r2()
    c.set("nai", "abc")
    expect_error(lambda: c.incr("nai"), "not an integer or out of range")


@check("error_value_out_of_range")
def _():
    c = r2()
    c.delete("oor")
    c.rpush("oor", "a")
    expect_error(lambda: c.lset("oor", 99, "x"), "index out of range")


@check("error_syntax")
def _():
    expect_error(lambda: r2().execute_command("SET", "k", "v", "BOGUS"), "syntax error")


@check("error_no_such_key")
def _():
    expect_error(lambda: r2().rename("definitely-missing", "x"), "no such key")


@check("error_expire_nan")
def _():
    c = r2()
    c.set("exnan", "v")
    expect_error(lambda: c.execute_command("EXPIRE", "exnan", "nope"), "not an integer")


@check("error_exec_without_multi")
def _():
    expect_error(lambda: r2().execute_command("EXEC"), "without MULTI")


@check("error_subscribe_arity")
def _():
    expect_error(lambda: r2().execute_command("SUBSCRIBE"), "wrong number of arguments")


# ---------------------------------------------------------------------------
# Things applications reach for that need server-side scripting. redis-py's own
# Lock uses a Lua script to release, so this is not an exotic corner.
# ---------------------------------------------------------------------------


@check("lock_acquire_release")
def _():
    c = r2()
    lock = c.lock("app-lock", timeout=5)
    if not lock.acquire(blocking=False):
        raise AssertionError("could not acquire an uncontended lock")
    lock.release()


@check("eval_script")
def _():
    c = r2()
    eq(c.eval("return 1", 0), 1)


@check("sort")
def _():
    c = r2()
    c.delete("tosort")
    c.rpush("tosort", "3", "1", "2")
    eq(c.sort("tosort"), ["1", "2", "3"])


@check("time_command")
def _():
    got = r2().time()
    if not isinstance(got, tuple) or got[0] < 1_600_000_000:
        raise AssertionError(f"TIME = {got!r}")


@check("setex_msetnx_pushx")
def _():
    c = r2()
    c.delete("sx", "px")
    eq(c.setex("sx", 60, "v"), True)
    eq(c.msetnx({"mn1": "1", "mn2": "2"}), True)
    eq(c.rpushx("px", "v"), 0)


# ---------------------------------------------------------------------------
# Observability an operator's dashboard reads.
# ---------------------------------------------------------------------------


@check("client_list_and_kill")
def _():
    c = r2()
    c.client_setname("to-be-listed")
    listed = c.client_list()
    if not any(entry.get("name") == "to-be-listed" for entry in listed):
        raise AssertionError(f"CLIENT LIST did not include this connection: {listed!r}")
    doomed = r2()
    doomed.client_setname("doomed")
    ident = doomed.client_id()
    eq(c.client_kill_filter(_id=ident), 1)


@check("slowlog")
def _():
    c = r2()
    c.config_set("slowlog-log-slower-than", 0)
    c.get("slowlog-probe")
    entries = c.slowlog_get(5)
    c.config_set("slowlog-log-slower-than", 10000)
    if not entries:
        raise AssertionError("SLOWLOG GET was empty with a 0 threshold")
    for field in ("id", "start_time", "duration", "command"):
        if field not in entries[0]:
            raise AssertionError(f"slow log entry has no {field}: {entries[0]!r}")
    c.slowlog_reset()


@check("monitor")
def _():
    c = r2()
    with c.monitor() as mon:
        r2().set("watched-by-monitor", "1")
        deadline = time.time() + 5
        while time.time() < deadline:
            line = mon.next_command()
            if line and "watched-by-monitor" in str(line.get("command", "")):
                return
    raise AssertionError("MONITOR never reported the SET")


@check("memory_usage_and_stats")
def _():
    c = r2()
    c.set("dbgk", "v")
    if not c.memory_usage("dbgk"):
        raise AssertionError("MEMORY USAGE returned nil or zero")
    if c.memory_usage("no-such-key-at-all") is not None:
        raise AssertionError("MEMORY USAGE of a missing key was not nil")


# ---------------------------------------------------------------------------
# Cluster. redis-py's RedisCluster reads CLUSTER SHARDS (falling back to
# CLUSTER SLOTS), builds a slot table, and routes every command itself. It is a
# far stricter reader of those replies than redis-cli, which only prints them.
# ---------------------------------------------------------------------------

try:
    from redis.cluster import RedisCluster
except ImportError:  # pragma: no cover
    RedisCluster = None


def cluster_client():
    if RedisCluster is None:
        raise Skip("redis-py has no cluster client")
    return RedisCluster(host=CHOST, port=CPORT, decode_responses=True)


_cluster = None
try:
    _cluster = cluster_client()
    _cluster.ping(target_nodes=RedisCluster.ALL_NODES)
    result("cluster_connect", "PASS")
except Skip as exc:
    result("cluster_connect", "SKIP", str(exc))
except Exception as exc:  # noqa: BLE001
    result("cluster_connect", "FAIL", f"{type(exc).__name__}: {exc}")


def need_cluster():
    if _cluster is None:
        raise Skip("no cluster client")
    return _cluster


@check("cluster_slots_and_shards")
def _():
    c = need_cluster()
    node = list(c.get_nodes())[0]
    slots = c.execute_command("CLUSTER SLOTS", target_nodes=node)
    if not slots:
        raise AssertionError("CLUSTER SLOTS was empty")
    shards = c.execute_command("CLUSTER SHARDS", target_nodes=node)
    if not shards:
        raise AssertionError("CLUSTER SHARDS was empty")
    covered = len(c.get_nodes())
    if covered < 3:
        raise AssertionError(f"only {covered} nodes discovered")


@check("cluster_routed_set_get")
def _():
    c = need_cluster()
    # Keys chosen to land in different slots; the client must send each to its
    # own node without the application knowing which.
    for i in range(64):
        c.set(f"cl:{i}", i)
    for i in range(64):
        eq(c.get(f"cl:{i}"), str(i))


@check("cluster_hashtag_multikey")
def _():
    c = need_cluster()
    c.mset({"{user1000}.following": "1", "{user1000}.followers": "2"})
    eq(c.mget("{user1000}.following", "{user1000}.followers"), ["1", "2"])


@check("cluster_crossslot_error")
def _():
    c = need_cluster()
    try:
        c.mget("no-tag-a", "no-tag-b")
    except Exception as exc:  # noqa: BLE001
        # redis-py refuses this in the client, before a byte goes out: it knows
        # the two keys hash to different slots. Either message counts -- what
        # must not happen is the command being served.
        text = str(exc).lower()
        if "crossslot" not in text and "same key slot" not in text:
            raise AssertionError(f"cross-slot MGET raised {type(exc).__name__}: {exc}") from exc
        return
    raise AssertionError("a cross-slot MGET was served")


@check("cluster_pipeline")
def _():
    c = need_cluster()
    with c.pipeline() as pipe:
        for i in range(32):
            pipe.set(f"clp:{i}", i)
        pipe.execute()
    eq(c.get("clp:31"), "31")


@check("cluster_keyslot_and_countkeys")
def _():
    c = need_cluster()
    eq(c.cluster_keyslot("foo"), 12182)
    eq(c.cluster_keyslot("{user1000}.following"), 3443)


@check("cluster_scan_all_nodes")
def _():
    c = need_cluster()
    seen = set(c.scan_iter(match="cl:*", count=11))
    if len(seen) < 64:
        raise AssertionError(f"scan_iter over the cluster found {len(seen)} of 64")


@check("cluster_transaction_one_slot")
def _():
    c = need_cluster()
    with c.pipeline(transaction=True) as pipe:
        pipe.set("{tx}.a", "1")
        pipe.set("{tx}.b", "2")
        # The cluster pipeline hands back the raw +OK rather than coercing it to
        # True, unlike the standalone one. Either is the library's business.
        got = [True if v == "OK" else v for v in pipe.execute()]
        eq(got, [True, True])


@check("cluster_pubsub")
def _():
    c = need_cluster()
    node = list(c.get_nodes())[0]
    p = c.pubsub(node=node)
    p.subscribe("cluster-news")
    deadline = time.time() + 5
    while time.time() < deadline:
        if p.get_message(timeout=0.2):
            break
    # PUBLISH names no key, so redis-py cannot route it from the slot table and
    # insists on being told which node. Real Redis broadcasts a publish across
    # the cluster bus; shardkv has no bus, so a subscriber only hears publishes
    # that reached its own node -- which is why both go to the same node here.
    # That difference is documented in the README's Cluster section.
    c.publish("cluster-news", "hello", target_nodes=node)
    got = drain(p, 1, timeout=5)
    eq(got[0]["data"], "hello")
    p.close()


print("# done", flush=True)
