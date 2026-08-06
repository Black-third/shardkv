// go-redis against shardkv, and against a real Redis for reference.
//
// go-redis is the strictest of the four clients on reply *types*, because every
// command has a typed accessor: a sorted-set range with scores is decoded into
// []redis.Z, XINFO STREAM into a struct with named fields, CLUSTER SLOTS into a
// slot table. A reply that is one element short, or an integer where a bulk
// string belongs, fails inside the library rather than reaching the caller as a
// wrong value -- which is exactly the class of bug a hand-driven redis-cli
// session cannot see.
//
// It also defaults to RESP3, so it exercises the protocol that most changes
// reply shapes, and it sends CLIENT SETINFO on every connect unprompted.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	host     = env("SHARDKV_HOST", "127.0.0.1")
	port     = env("SHARDKV_PORT", "6380")
	chost    = env("CLUSTER_HOST", "127.0.0.1")
	cport    = env("CLUSTER_PORT", "6380")
	target   = env("TARGET", "?")
	ctx      = context.Background()
	addr     = host + ":" + port
	caddr    = chost + ":" + cport
	clients  []*redis.Client
	clientMu sync.Mutex
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// errSkip marks a check that cannot apply here; it is not a failure. Wrap it
// with %w to report a skip from a check body.
var errSkip = errors.New("skip")

func result(feature, status, detail string) {
	detail = strings.Join(strings.Fields(detail), " ")
	if len(detail) > 220 {
		detail = detail[:220]
	}
	fmt.Printf("::RESULT::%s::%s::%s\n", feature, status, detail)
}

func check(feature string, fn func() error) {
	defer func() {
		if p := recover(); p != nil {
			result(feature, "FAIL", fmt.Sprintf("panic: %v", p))
		}
	}()
	switch err := fn(); {
	case err == nil:
		result(feature, "PASS", "")
	case errors.Is(err, errSkip):
		result(feature, "SKIP", err.Error())
	default:
		result(feature, "FAIL", err.Error())
	}
}

func open(opts *redis.Options) *redis.Client {
	if opts == nil {
		opts = &redis.Options{}
	}
	opts.Addr = addr
	c := redis.NewClient(opts)
	clientMu.Lock()
	clients = append(clients, c)
	clientMu.Unlock()
	return c
}

func eq[T comparable](got, want T, what string) error {
	if got != want {
		return fmt.Errorf("%sgot %v, want %v", what, got, want)
	}
	return nil
}

func expectError(err error, needle string) error {
	if err == nil {
		return fmt.Errorf("no error raised; wanted %q", needle)
	}
	if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(needle)) {
		return fmt.Errorf("error was %q, wanted %q", err.Error(), needle)
	}
	return nil
}

func goRedisVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if strings.HasPrefix(dep.Path, "github.com/redis/go-redis") {
			return dep.Version
		}
	}
	return "unknown"
}

func main() {
	version := goRedisVersion()
	fmt.Printf("# go-redis %s -> %s at %s\n", version, target, addr)
	result("library_version", "PASS", "go-redis "+version)

	r := open(nil)
	if err := r.FlushAll(ctx).Err(); err != nil {
		result("handshake_fatal", "FAIL", err.Error())
		fmt.Println("# aborted")
		return
	}

	handshake(r)
	types(r)
	transactions(r)
	pubsub(r)
	blocking(r)
	streams(r)
	scanning(r)
	pooling()
	errorText(r)
	gaps(r)
	cluster()

	fmt.Println("# done")
	clientMu.Lock()
	for _, c := range clients {
		_ = c.Close()
	}
	clientMu.Unlock()
}

// ---------------------------------------------------------------------------
// Handshake. go-redis negotiates RESP3 by default and identifies itself with
// CLIENT SETINFO; both happen before the application's first command.
// ---------------------------------------------------------------------------

func handshake(r *redis.Client) {
	check("handshake_resp3_default", func() error {
		// The default client sends HELLO 3 and two CLIENT SETINFO commands. If
		// any of that upsets the server, this is where it shows.
		return eq(r.Ping(ctx).Val(), "PONG", "PING: ")
	})

	check("handshake_resp2", func() error {
		c := open(&redis.Options{Protocol: 2})
		if err := c.Ping(ctx).Err(); err != nil {
			return err
		}
		return eq(c.Set(ctx, "resp2-key", "v", 0).Val(), "OK", "SET over RESP2: ")
	})

	check("handshake_client_name", func() error {
		c := open(&redis.Options{ClientName: "go-redis-app"})
		return eq(c.ClientGetName(ctx).Val(), "go-redis-app", "CLIENT GETNAME: ")
	})

	check("client_setinfo", func() error {
		if err := r.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "go-redis-compat").Err(); err != nil {
			return err
		}
		info, err := r.ClientInfo(ctx).Result()
		if err != nil {
			return err
		}
		if info.LibName != "go-redis-compat" {
			return fmt.Errorf("CLIENT INFO lib-name = %q", info.LibName)
		}
		return nil
	})

	check("client_info_parses", func() error {
		// go-redis decodes the CLIENT INFO line into a struct, so every field it
		// expects must be present and in the form it expects.
		info, err := r.ClientInfo(ctx).Result()
		if err != nil {
			return err
		}
		if info.ID == 0 {
			return fmt.Errorf("CLIENT INFO id = 0 in %+v", info)
		}
		return nil
	})

	check("command_docs", func() error {
		res, err := r.Do(ctx, "COMMAND", "DOCS", "GET").Result()
		if err != nil {
			return err
		}
		if res == nil {
			return errors.New("COMMAND DOCS GET was nil")
		}
		return nil
	})

	check("config_get", func() error {
		got, err := r.ConfigGet(ctx, "*").Result()
		if err != nil {
			return err
		}
		if len(got) == 0 {
			return errors.New("CONFIG GET * was empty")
		}
		return nil
	})

	check("info_redis_version", func() error {
		// go-redis itself does not need this, but nearly every layer above it
		// does: a version string is how a library decides which commands exist.
		info, err := r.Info(ctx, "server").Result()
		if err != nil {
			return err
		}
		if !strings.Contains(info, "redis_version:") {
			return fmt.Errorf("INFO server has no redis_version: %q", firstLines(info, 6))
		}
		return nil
	})

	check("info_memory_section", func() error {
		info, err := r.Info(ctx, "memory").Result()
		if err != nil {
			return err
		}
		if !strings.Contains(info, "used_memory:") {
			return fmt.Errorf("INFO memory has no used_memory: %q", firstLines(info, 6))
		}
		return nil
	})

	check("echo", func() error {
		// Trivial, and load-bearing: several clients use ECHO as their
		// connection liveness probe instead of PING.
		got, err := r.Echo(ctx, "round trip").Result()
		if err != nil {
			return err
		}
		return eq(got, "round trip", "ECHO: ")
	})
}

func firstLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, " / ")
}

// ---------------------------------------------------------------------------
// Types, through go-redis's typed accessors.
// ---------------------------------------------------------------------------

func types(r *redis.Client) {
	check("type_string", func() error {
		if err := r.Set(ctx, "s", "hello", 0).Err(); err != nil {
			return err
		}
		if err := eq(r.Get(ctx, "s").Val(), "hello", "GET: "); err != nil {
			return err
		}
		if err := eq(r.Append(ctx, "s", "!").Val(), int64(6), "APPEND: "); err != nil {
			return err
		}
		if err := eq(r.GetRange(ctx, "s", 0, 3).Val(), "hell", "GETRANGE: "); err != nil {
			return err
		}
		if err := eq(r.IncrBy(ctx, "counter", 5).Val(), int64(5), "INCRBY: "); err != nil {
			return err
		}
		if err := eq(r.IncrByFloat(ctx, "counter", 0.25).Val(), 5.25, "INCRBYFLOAT: "); err != nil {
			return err
		}
		if err := r.MSet(ctx, "a", "1", "b", "2").Err(); err != nil {
			return err
		}
		got := r.MGet(ctx, "a", "b", "missing").Val()
		if len(got) != 3 || got[0] != "1" || got[2] != nil {
			return fmt.Errorf("MGET = %#v", got)
		}
		return nil
	})

	check("type_nil_is_redis_nil", func() error {
		// The single most load-bearing convention in go-redis: a missing key is
		// the sentinel redis.Nil, not an empty string. It depends on the server
		// sending a null bulk string rather than an empty one.
		_, err := r.Get(ctx, "definitely-not-here").Result()
		if !errors.Is(err, redis.Nil) {
			return fmt.Errorf("GET of a missing key gave %v, want redis.Nil", err)
		}
		return nil
	})

	check("type_expiry", func() error {
		if err := r.Set(ctx, "e", "v", 100*time.Second).Err(); err != nil {
			return err
		}
		ttl := r.TTL(ctx, "e").Val()
		if ttl < 90*time.Second || ttl > 100*time.Second {
			return fmt.Errorf("TTL = %v", ttl)
		}
		if err := eq(r.Persist(ctx, "e").Val(), true, "PERSIST: "); err != nil {
			return err
		}
		if err := eq(r.TTL(ctx, "e").Val(), time.Duration(-1), "TTL after PERSIST: "); err != nil {
			return err
		}
		return nil
	})

	check("type_list", func() error {
		r.Del(ctx, "l")
		if err := eq(r.RPush(ctx, "l", "a", "b", "c").Val(), int64(3), "RPUSH: "); err != nil {
			return err
		}
		if got := r.LRange(ctx, "l", 0, -1).Val(); strings.Join(got, ",") != "a,b,c" {
			return fmt.Errorf("LRANGE = %v", got)
		}
		if err := eq(r.LPop(ctx, "l").Val(), "a", "LPOP: "); err != nil {
			return err
		}
		if err := eq(r.LPos(ctx, "l", "c", redis.LPosArgs{}).Val(), int64(1), "LPOS: "); err != nil {
			return err
		}
		return nil
	})

	check("type_hash", func() error {
		r.Del(ctx, "h")
		if err := r.HSet(ctx, "h", "f", "v", "g", "w").Err(); err != nil {
			return err
		}
		got := r.HGetAll(ctx, "h").Val()
		if got["f"] != "v" || got["g"] != "w" || len(got) != 2 {
			return fmt.Errorf("HGETALL = %v", got)
		}
		if err := eq(r.HLen(ctx, "h").Val(), int64(2), "HLEN: "); err != nil {
			return err
		}
		vals := r.HMGet(ctx, "h", "f", "nope").Val()
		if len(vals) != 2 || vals[0] != "v" || vals[1] != nil {
			return fmt.Errorf("HMGET = %#v", vals)
		}
		return nil
	})

	check("type_set", func() error {
		r.Del(ctx, "s1", "s2")
		r.SAdd(ctx, "s1", "a", "b", "c")
		r.SAdd(ctx, "s2", "b", "c", "d")
		members := r.SMembers(ctx, "s1").Val()
		sort.Strings(members)
		if strings.Join(members, ",") != "a,b,c" {
			return fmt.Errorf("SMEMBERS = %v", members)
		}
		inter := r.SInter(ctx, "s1", "s2").Val()
		sort.Strings(inter)
		if strings.Join(inter, ",") != "b,c" {
			return fmt.Errorf("SINTER = %v", inter)
		}
		return eq(r.SIsMember(ctx, "s1", "a").Val(), true, "SISMEMBER: ")
	})

	check("type_zset_withscores", func() error {
		// []redis.Z is decoded element by element: in RESP2 from a flat array,
		// in RESP3 from an array of pairs. Getting either shape wrong fails here.
		r.Del(ctx, "z")
		if err := r.ZAdd(ctx, "z",
			redis.Z{Score: 1, Member: "a"},
			redis.Z{Score: 2, Member: "b"}).Err(); err != nil {
			return err
		}
		got := r.ZRangeWithScores(ctx, "z", 0, -1).Val()
		if len(got) != 2 || got[0].Member != "a" || got[0].Score != 1 || got[1].Score != 2 {
			return fmt.Errorf("ZRANGE WITHSCORES = %#v", got)
		}
		if err := eq(r.ZScore(ctx, "z", "b").Val(), 2.0, "ZSCORE: "); err != nil {
			return err
		}
		if err := eq(r.ZCard(ctx, "z").Val(), int64(2), "ZCARD: "); err != nil {
			return err
		}
		if err := eq(r.ZRank(ctx, "z", "b").Val(), int64(1), "ZRANK: "); err != nil {
			return err
		}
		return nil
	})

	check("type_zset_rangeargs_byscore", func() error {
		// ZRangeArgs is what go-redis emits for any range that is not by rank,
		// and it always sends the Redis 6.2 option form: ZRANGE key min max
		// BYSCORE [REV] [LIMIT ...].
		r.Del(ctx, "zr")
		r.ZAdd(ctx, "zr", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"},
			redis.Z{Score: 3, Member: "c"})
		got, err := r.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key: "zr", Start: 1, Stop: 2, ByScore: true,
		}).Result()
		if err != nil {
			return err
		}
		if strings.Join(got, ",") != "a,b" {
			return fmt.Errorf("ZRANGE BYSCORE = %v", got)
		}
		got, err = r.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key: "zr", Start: "+inf", Stop: "-inf", ByScore: true, Rev: true, Offset: 0, Count: 2,
		}).Result()
		if err != nil {
			return err
		}
		if strings.Join(got, ",") != "c,b" {
			return fmt.Errorf("ZRANGE BYSCORE REV LIMIT = %v", got)
		}
		return nil
	})

	check("type_zset_rangeargs_bylex", func() error {
		r.Del(ctx, "zl")
		r.ZAdd(ctx, "zl", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"},
			redis.Z{Score: 0, Member: "c"})
		got, err := r.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key: "zl", Start: "[a", Stop: "[b", ByLex: true,
		}).Result()
		if err != nil {
			return err
		}
		if strings.Join(got, ",") != "a,b" {
			return fmt.Errorf("ZRANGE BYLEX = %v", got)
		}
		return nil
	})

	check("type_bitmap_hll_geo", func() error {
		r.Del(ctx, "bm", "hll", "geo")
		if err := eq(r.SetBit(ctx, "bm", 7, 1).Val(), int64(0), "SETBIT: "); err != nil {
			return err
		}
		if err := eq(r.BitCount(ctx, "bm", nil).Val(), int64(1), "BITCOUNT: "); err != nil {
			return err
		}
		if err := eq(r.PFAdd(ctx, "hll", "a", "b", "c").Val(), int64(1), "PFADD: "); err != nil {
			return err
		}
		if err := eq(r.PFCount(ctx, "hll").Val(), int64(3), "PFCOUNT: "); err != nil {
			return err
		}
		if err := r.GeoAdd(ctx, "geo", &redis.GeoLocation{
			Longitude: 13.361389, Latitude: 38.115556, Name: "Palermo",
		}).Err(); err != nil {
			return err
		}
		hash := r.GeoHash(ctx, "geo", "Palermo").Val()
		if len(hash) != 1 || hash[0] != "sqc8b49rny0" {
			return fmt.Errorf("GEOHASH = %v", hash)
		}
		return nil
	})

	check("object_encoding_and_type", func() error {
		r.Del(ctx, "oe")
		r.RPush(ctx, "oe", "x")
		if err := eq(r.Type(ctx, "oe").Val(), "list", "TYPE: "); err != nil {
			return err
		}
		enc, err := r.ObjectEncoding(ctx, "oe").Result()
		if err != nil {
			return err
		}
		if enc == "" {
			return errors.New("OBJECT ENCODING was empty")
		}
		return nil
	})

	check("dump_restore", func() error {
		r.Del(ctx, "dr", "dr2")
		r.RPush(ctx, "dr", "a", "b")
		payload, err := r.Dump(ctx, "dr").Result()
		if err != nil {
			return err
		}
		if err := r.Restore(ctx, "dr2", 0, payload).Err(); err != nil {
			return err
		}
		if got := r.LRange(ctx, "dr2", 0, -1).Val(); strings.Join(got, ",") != "a,b" {
			return fmt.Errorf("restored list = %v", got)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Pipelines and transactions.
// ---------------------------------------------------------------------------

func transactions(r *redis.Client) {
	check("pipeline", func() error {
		r.Del(ctx, "p")
		cmds, err := r.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "p", "v", 0)
			pipe.Get(ctx, "p")
			pipe.LLen(ctx, "p")
			return nil
		})
		// The third command fails; Pipelined reports the first error but every
		// command still carries its own result.
		if err == nil {
			return errors.New("pipeline did not report the WRONGTYPE")
		}
		if len(cmds) != 3 {
			return fmt.Errorf("pipeline returned %d commands", len(cmds))
		}
		if err := expectError(cmds[2].Err(), "WRONGTYPE"); err != nil {
			return err
		}
		return eq(cmds[1].(*redis.StringCmd).Val(), "v", "pipelined GET: ")
	})

	check("tx_pipeline", func() error {
		r.Del(ctx, "t")
		cmds, err := r.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.RPush(ctx, "t", "a")
			pipe.RPush(ctx, "t", "b")
			pipe.LLen(ctx, "t")
			return nil
		})
		if err != nil {
			return err
		}
		return eq(cmds[2].(*redis.IntCmd).Val(), int64(2), "LLEN inside MULTI: ")
	})

	check("watch_aborts_on_conflict", func() error {
		r.Set(ctx, "w", "0", 0)
		other := open(nil)
		err := r.Watch(ctx, func(tx *redis.Tx) error {
			if _, err := tx.Get(ctx, "w").Result(); err != nil {
				return err
			}
			// Somebody else writes the watched key while the transaction is open.
			if err := other.Set(ctx, "w", "changed", 0).Err(); err != nil {
				return err
			}
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "w", "mine", 0)
				return nil
			})
			return err
		}, "w")
		if !errors.Is(err, redis.TxFailedErr) {
			return fmt.Errorf("EXEC after a conflicting write gave %v, want TxFailedErr", err)
		}
		return eq(r.Get(ctx, "w").Val(), "changed", "value after the abort: ")
	})

	check("watch_retry_loop_commits", func() error {
		// The canonical go-redis optimistic-lock loop.
		r.Set(ctx, "w2", "0", 0)
		increment := func(tx *redis.Tx) error {
			n, err := tx.Get(ctx, "w2").Int()
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "w2", n+1, 0)
				return nil
			})
			return err
		}
		for i := 0; i < 10; i++ {
			err := r.Watch(ctx, increment, "w2")
			if err == nil {
				return eq(r.Get(ctx, "w2").Val(), "1", "value after the commit: ")
			}
			if !errors.Is(err, redis.TxFailedErr) {
				return err
			}
		}
		return errors.New("the retry loop never committed")
	})
}

// ---------------------------------------------------------------------------
// Pub/Sub. go-redis waits for the subscribe confirmation before it will deliver
// anything, so a missing or misshapen confirmation hangs the application.
// ---------------------------------------------------------------------------

func pubsub(r *redis.Client) {
	check("pubsub_message", func() error {
		sub := r.Subscribe(ctx, "news")
		defer sub.Close()
		if _, err := sub.Receive(ctx); err != nil { // the confirmation
			return err
		}
		if err := r.Publish(ctx, "news", "hello").Err(); err != nil {
			return err
		}
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		if err := eq(msg.Channel, "news", "channel: "); err != nil {
			return err
		}
		return eq(msg.Payload, "hello", "payload: ")
	})

	check("pubsub_pattern", func() error {
		sub := r.PSubscribe(ctx, "news.*")
		defer sub.Close()
		if _, err := sub.Receive(ctx); err != nil {
			return err
		}
		if err := r.Publish(ctx, "news.sport", "goal").Err(); err != nil {
			return err
		}
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		if err := eq(msg.Pattern, "news.*", "pattern: "); err != nil {
			return err
		}
		return eq(msg.Payload, "goal", "payload: ")
	})

	check("pubsub_introspection", func() error {
		sub := r.Subscribe(ctx, "introspect")
		defer sub.Close()
		if _, err := sub.Receive(ctx); err != nil {
			return err
		}
		channels, err := r.PubSubChannels(ctx, "*").Result()
		if err != nil {
			return err
		}
		for _, ch := range channels {
			if ch == "introspect" {
				counts, err := r.PubSubNumSub(ctx, "introspect").Result()
				if err != nil {
					return err
				}
				return eq(counts["introspect"], int64(1), "PUBSUB NUMSUB: ")
			}
		}
		return fmt.Errorf("PUBSUB CHANNELS = %v", channels)
	})

	check("keyspace_notifications", func() error {
		sub := r.PSubscribe(ctx, "__keyevent@0__:set")
		defer sub.Close()
		if _, err := sub.Receive(ctx); err != nil {
			return err
		}
		if err := r.Set(ctx, "notified", "1", 0).Err(); err != nil {
			return err
		}
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		if !strings.HasSuffix(msg.Channel, ":set") {
			return fmt.Errorf("channel was %q", msg.Channel)
		}
		return eq(msg.Payload, "notified", "notified key: ")
	})
}

// ---------------------------------------------------------------------------
// Blocking commands.
// ---------------------------------------------------------------------------

func blocking(r *redis.Client) {
	check("blocking_blpop", func() error {
		r.Del(ctx, "queue")
		done := make(chan []string, 1)
		errs := make(chan error, 1)
		go func() {
			got, err := open(nil).BLPop(ctx, 5*time.Second, "queue").Result()
			if err != nil {
				errs <- err
				return
			}
			done <- got
		}()
		time.Sleep(300 * time.Millisecond)
		if err := r.RPush(ctx, "queue", "job").Err(); err != nil {
			return err
		}
		select {
		case got := <-done:
			if len(got) != 2 || got[0] != "queue" || got[1] != "job" {
				return fmt.Errorf("BLPOP = %v", got)
			}
			return nil
		case err := <-errs:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("BLPOP never returned")
		}
	})

	check("blocking_timeout_is_redis_nil", func() error {
		_, err := r.BLPop(ctx, time.Second, "never-pushed").Result()
		if !errors.Is(err, redis.Nil) {
			return fmt.Errorf("a timed-out BLPOP gave %v, want redis.Nil", err)
		}
		return nil
	})

	check("blocking_bzpopmin_and_blmove", func() error {
		r.Del(ctx, "bz", "src", "dst")
		r.ZAdd(ctx, "bz", redis.Z{Score: 1, Member: "m"})
		got, err := r.BZPopMin(ctx, time.Second, "bz").Result()
		if err != nil {
			return err
		}
		if got.Key != "bz" || got.Member != "m" || got.Score != 1 {
			return fmt.Errorf("BZPOPMIN = %#v", got)
		}
		r.RPush(ctx, "src", "v")
		moved, err := r.BLMove(ctx, "src", "dst", "left", "right", time.Second).Result()
		if err != nil {
			return err
		}
		return eq(moved, "v", "BLMOVE: ")
	})
}

// ---------------------------------------------------------------------------
// Streams, decoded into go-redis's structs.
// ---------------------------------------------------------------------------

func streams(r *redis.Client) {
	check("streams_basic", func() error {
		r.Del(ctx, "st")
		id, err := r.XAdd(ctx, &redis.XAddArgs{
			Stream: "st", Values: map[string]any{"item": "widget"},
		}).Result()
		if err != nil {
			return err
		}
		if !strings.Contains(id, "-") {
			return fmt.Errorf("XADD returned %q", id)
		}
		if err := eq(r.XLen(ctx, "st").Val(), int64(1), "XLEN: "); err != nil {
			return err
		}
		msgs, err := r.XRange(ctx, "st", "-", "+").Result()
		if err != nil {
			return err
		}
		if len(msgs) != 1 || msgs[0].Values["item"] != "widget" {
			return fmt.Errorf("XRANGE = %#v", msgs)
		}
		return nil
	})

	check("streams_consumer_group", func() error {
		r.Del(ctx, "orders")
		r.XAdd(ctx, &redis.XAddArgs{Stream: "orders", Values: map[string]any{"item": "a"}})
		r.XAdd(ctx, &redis.XAddArgs{Stream: "orders", Values: map[string]any{"item": "b"}})
		if err := r.XGroupCreate(ctx, "orders", "g", "0").Err(); err != nil {
			return err
		}
		streams, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: "g", Consumer: "worker", Streams: []string{"orders", ">"}, Count: 1,
		}).Result()
		if err != nil {
			return err
		}
		if len(streams) != 1 || len(streams[0].Messages) != 1 {
			return fmt.Errorf("XREADGROUP = %#v", streams)
		}
		id := streams[0].Messages[0].ID
		pending, err := r.XPending(ctx, "orders", "g").Result()
		if err != nil {
			return err
		}
		if pending.Count != 1 {
			return fmt.Errorf("XPENDING count = %d", pending.Count)
		}
		if err := eq(r.XAck(ctx, "orders", "g", id).Val(), int64(1), "XACK: "); err != nil {
			return err
		}
		ext, err := r.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: "orders", Group: "g", Start: "-", End: "+", Count: 10,
		}).Result()
		if err != nil {
			return err
		}
		return eq(len(ext), 0, "pending entries after the ack: ")
	})

	check("streams_autoclaim", func() error {
		r.Del(ctx, "claims")
		r.XAdd(ctx, &redis.XAddArgs{Stream: "claims", Values: map[string]any{"n": "1"}})
		if err := r.XGroupCreate(ctx, "claims", "g", "0").Err(); err != nil {
			return err
		}
		if _, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: "g", Consumer: "dead", Streams: []string{"claims", ">"},
		}).Result(); err != nil {
			return err
		}
		msgs, _, err := r.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: "claims", Group: "g", Consumer: "live", MinIdle: 0, Start: "0",
		}).Result()
		if err != nil {
			return err
		}
		return eq(len(msgs), 1, "reclaimed messages: ")
	})

	check("streams_xinfo_structs", func() error {
		// XINFO STREAM and XINFO GROUPS are decoded field by field into structs;
		// a missing field is a decode error rather than a zero value.
		r.Del(ctx, "xi")
		r.XAdd(ctx, &redis.XAddArgs{Stream: "xi", Values: map[string]any{"a": "1"}})
		if err := r.XGroupCreate(ctx, "xi", "g", "0").Err(); err != nil {
			return err
		}
		info, err := r.XInfoStream(ctx, "xi").Result()
		if err != nil {
			return err
		}
		if info.Length != 1 {
			return fmt.Errorf("XINFO STREAM length = %d", info.Length)
		}
		groups, err := r.XInfoGroups(ctx, "xi").Result()
		if err != nil {
			return err
		}
		if len(groups) != 1 || groups[0].Name != "g" {
			return fmt.Errorf("XINFO GROUPS = %#v", groups)
		}
		return nil
	})

	check("streams_xread_block", func() error {
		r.Del(ctx, "live")
		r.XAdd(ctx, &redis.XAddArgs{Stream: "live", Values: map[string]any{"seed": "1"}})
		done := make(chan error, 1)
		go func() {
			_, err := open(nil).XRead(ctx, &redis.XReadArgs{
				Streams: []string{"live", "$"}, Block: 5 * time.Second, Count: 1,
			}).Result()
			done <- err
		}()
		time.Sleep(300 * time.Millisecond)
		r.XAdd(ctx, &redis.XAddArgs{Stream: "live", Values: map[string]any{"late": "1"}})
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("XREAD BLOCK $ was never woken")
		}
	})
}

// ---------------------------------------------------------------------------
// SCAN, through go-redis's iterator.
// ---------------------------------------------------------------------------

func scanning(r *redis.Client) {
	check("scan_iterator", func() error {
		if err := r.FlushDB(ctx).Err(); err != nil {
			return err
		}
		pipe := r.Pipeline()
		for i := 0; i < 400; i++ {
			pipe.Set(ctx, "scan:"+strconv.Itoa(i), i, 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		seen := map[string]bool{}
		iter := r.Scan(ctx, 0, "scan:*", 17).Iterator()
		for iter.Next(ctx) {
			seen[iter.Val()] = true
		}
		if err := iter.Err(); err != nil {
			return err
		}
		return eq(len(seen), 400, "keys seen by the SCAN iterator: ")
	})

	check("scan_type_filter", func() error {
		// ScanType sends SCAN ... TYPE <t>, which several application frameworks
		// use to enumerate one kind of key without reading every value.
		r.Del(ctx, "typed-list")
		r.RPush(ctx, "typed-list", "x")
		r.Set(ctx, "typed-string", "y", 0)
		seen := map[string]bool{}
		iter := r.ScanType(ctx, 0, "typed-*", 10, "list").Iterator()
		for iter.Next(ctx) {
			seen[iter.Val()] = true
		}
		if err := iter.Err(); err != nil {
			return err
		}
		if len(seen) != 1 || !seen["typed-list"] {
			return fmt.Errorf("SCAN TYPE list found %v", seen)
		}
		return nil
	})

	check("hscan_sscan_zscan_iterators", func() error {
		r.Del(ctx, "bigh", "bigs", "bigz")
		pipe := r.Pipeline()
		for i := 0; i < 300; i++ {
			pipe.HSet(ctx, "bigh", "f"+strconv.Itoa(i), i)
			pipe.SAdd(ctx, "bigs", "m"+strconv.Itoa(i))
			pipe.ZAdd(ctx, "bigz", redis.Z{Score: float64(i), Member: "z" + strconv.Itoa(i)})
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		count := func(iter *redis.ScanIterator) (int, error) {
			n := 0
			for iter.Next(ctx) {
				n++
			}
			return n, iter.Err()
		}
		// HSCAN and ZSCAN yield field and value alternately, so 300 entries is
		// 600 iterator steps.
		if n, err := count(r.HScan(ctx, "bigh", 0, "", 11).Iterator()); err != nil || n != 600 {
			return fmt.Errorf("HSCAN yielded %d (err %v), want 600", n, err)
		}
		if n, err := count(r.SScan(ctx, "bigs", 0, "", 11).Iterator()); err != nil || n != 300 {
			return fmt.Errorf("SSCAN yielded %d (err %v), want 300", n, err)
		}
		if n, err := count(r.ZScan(ctx, "bigz", 0, "", 11).Iterator()); err != nil || n != 600 {
			return fmt.Errorf("ZSCAN yielded %d (err %v), want 600", n, err)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// The connection pool under concurrency: many goroutines, few sockets.
// ---------------------------------------------------------------------------

func pooling() {
	check("connection_pool_under_goroutines", func() error {
		c := open(&redis.Options{PoolSize: 8, MinIdleConns: 2})
		if err := c.Del(ctx, "pooled").Err(); err != nil {
			return err
		}
		var wg sync.WaitGroup
		failures := make(chan error, 64)
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					if err := c.Incr(ctx, "pooled").Err(); err != nil {
						failures <- err
						return
					}
					key := fmt.Sprintf("pool:%d:%d", worker, i)
					if err := c.Set(ctx, key, i, 0).Err(); err != nil {
						failures <- err
						return
					}
					if got := c.Get(ctx, key).Val(); got != strconv.Itoa(i) {
						failures <- fmt.Errorf("%s = %q", key, got)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(failures)
		if err := <-failures; err != nil {
			return err
		}
		return eq(c.Get(ctx, "pooled").Val(), "800", "counter after 16x50 INCRs: ")
	})
}

// ---------------------------------------------------------------------------
// Error text.
// ---------------------------------------------------------------------------

func errorText(r *redis.Client) {
	check("error_wrongtype", func() error {
		r.Del(ctx, "etype")
		r.Set(ctx, "etype", "v", 0)
		return expectError(r.LPush(ctx, "etype", "x").Err(), "WRONGTYPE")
	})

	check("error_unknown_command", func() error {
		return expectError(r.Do(ctx, "NOSUCHTHING").Err(), "unknown command")
	})

	check("error_arity", func() error {
		return expectError(r.Do(ctx, "GET").Err(), "wrong number of arguments")
	})

	check("error_not_integer", func() error {
		r.Set(ctx, "nan", "abc", 0)
		return expectError(r.Incr(ctx, "nan").Err(), "not an integer")
	})

	check("error_syntax", func() error {
		return expectError(r.Do(ctx, "SET", "k", "v", "BOGUS").Err(), "syntax error")
	})

	check("error_no_such_key", func() error {
		return expectError(r.Rename(ctx, "definitely-missing", "x").Err(), "no such key")
	})
}

// ---------------------------------------------------------------------------
// Commands an application reaches for that this server may not have. Each is
// its own cell so the matrix records the gap rather than one lumped failure.
// ---------------------------------------------------------------------------

func gaps(r *redis.Client) {
	check("eval_lua", func() error {
		return r.Eval(ctx, "return 1", nil).Err()
	})

	check("sort", func() error {
		r.Del(ctx, "tosort")
		r.RPush(ctx, "tosort", "3", "1", "2")
		got, err := r.Sort(ctx, "tosort", &redis.Sort{}).Result()
		if err != nil {
			return err
		}
		if strings.Join(got, ",") != "1,2,3" {
			return fmt.Errorf("SORT = %v", got)
		}
		return nil
	})

	check("time", func() error {
		got, err := r.Time(ctx).Result()
		if err != nil {
			return err
		}
		if got.Year() < 2020 {
			return fmt.Errorf("TIME = %v", got)
		}
		return nil
	})

	check("zunionstore", func() error {
		r.Del(ctx, "za", "zb", "zdest")
		r.ZAdd(ctx, "za", redis.Z{Score: 1, Member: "a"})
		r.ZAdd(ctx, "zb", redis.Z{Score: 2, Member: "b"})
		n, err := r.ZUnionStore(ctx, "zdest", &redis.ZStore{Keys: []string{"za", "zb"}}).Result()
		if err != nil {
			return err
		}
		return eq(n, int64(2), "ZUNIONSTORE: ")
	})

	check("role", func() error {
		return r.Do(ctx, "ROLE").Err()
	})

	check("expiretime", func() error {
		r.Set(ctx, "et", "v", time.Hour)
		return r.ExpireTime(ctx, "et").Err()
	})
}

// ---------------------------------------------------------------------------
// Cluster. ClusterClient reads CLUSTER SLOTS (or CLUSTER SHARDS), keeps a slot
// table, and follows MOVED and ASK itself. Its state check also parses
// CLUSTER INFO. Nothing here is done by the application.
// ---------------------------------------------------------------------------

func cluster() {
	cc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        []string{caddr},
		MaxRedirects: 8,
	})
	defer func() { _ = cc.Close() }()

	if err := cc.Ping(ctx).Err(); err != nil {
		result("cluster_connect", "FAIL", err.Error())
		return
	}
	result("cluster_connect", "PASS", "")

	check("cluster_slots_reply", func() error {
		slots, err := cc.ClusterSlots(ctx).Result()
		if err != nil {
			return err
		}
		covered := 0
		for _, s := range slots {
			covered += s.End - s.Start + 1
			if len(s.Nodes) == 0 {
				return fmt.Errorf("a slot range with no nodes: %#v", s)
			}
			if s.Nodes[0].Addr == "" || s.Nodes[0].ID == "" {
				return fmt.Errorf("a node with no address or id: %#v", s.Nodes[0])
			}
		}
		return eq(covered, 16384, "slots covered by CLUSTER SLOTS: ")
	})

	check("cluster_shards_reply", func() error {
		if err := cc.ClusterShards(ctx).Err(); err != nil {
			return err
		}
		return nil
	})

	check("cluster_topology_discovered", func() error {
		n := 0
		if err := cc.ForEachMaster(ctx, func(_ context.Context, _ *redis.Client) error {
			n++
			return nil
		}); err != nil {
			return err
		}
		return eq(n, 3, "masters discovered: ")
	})

	check("cluster_routed_set_get", func() error {
		for i := 0; i < 64; i++ {
			if err := cc.Set(ctx, "gcl:"+strconv.Itoa(i), i, 0).Err(); err != nil {
				return err
			}
		}
		for i := 0; i < 64; i++ {
			if got := cc.Get(ctx, "gcl:"+strconv.Itoa(i)).Val(); got != strconv.Itoa(i) {
				return fmt.Errorf("gcl:%d = %q", i, got)
			}
		}
		return nil
	})

	check("cluster_hashtag_multikey", func() error {
		if err := cc.MSet(ctx, "{gtag}.a", "1", "{gtag}.b", "2").Err(); err != nil {
			return err
		}
		got := cc.MGet(ctx, "{gtag}.a", "{gtag}.b").Val()
		if len(got) != 2 || got[0] != "1" || got[1] != "2" {
			return fmt.Errorf("MGET = %#v", got)
		}
		return nil
	})

	check("cluster_crossslot_error", func() error {
		return expectError(cc.MGet(ctx, "plain-a", "plain-b").Err(), "CROSSSLOT")
	})

	check("cluster_pipeline_split_by_node", func() error {
		// go-redis splits a pipeline into one batch per node and stitches the
		// replies back into the caller's order.
		cmds, err := cc.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for i := 0; i < 48; i++ {
				pipe.Set(ctx, "gcp:"+strconv.Itoa(i), i, 0)
			}
			return nil
		})
		if err != nil {
			return err
		}
		return eq(len(cmds), 48, "pipelined commands: ")
	})

	check("cluster_tx_one_slot", func() error {
		_, err := cc.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "{gtx}.a", "1", 0)
			pipe.Set(ctx, "{gtx}.b", "2", 0)
			return nil
		})
		return err
	})

	check("cluster_scan_all_masters", func() error {
		total := 0
		if err := cc.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			iter := node.Scan(ctx, 0, "gcl:*", 20).Iterator()
			for iter.Next(ctx) {
				total++
			}
			return iter.Err()
		}); err != nil {
			return err
		}
		if total < 64 {
			return fmt.Errorf("SCAN across the masters found %d of 64", total)
		}
		return nil
	})

	check("cluster_keyslot", func() error {
		if err := eq(cc.ClusterKeySlot(ctx, "foo").Val(), int64(12182), "KEYSLOT foo: "); err != nil {
			return err
		}
		return eq(cc.ClusterKeySlot(ctx, "{user1000}.following").Val(), int64(3443),
			"KEYSLOT with a hash tag: ")
	})
}
