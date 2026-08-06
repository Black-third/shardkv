package server

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/store"
)

// TestReplicaConvergesOnNonDeterministicWrites drives a master through every write
// whose result is drawn at random or decided by iteration order, then requires the
// replica to hold the identical dataset.
//
// This is the end-to-end form of the effect-rewrite decision: if SPOP or ZPOPMIN
// shipped verbatim, the replica would run its own draw, remove different members,
// and stay diverged forever with nothing to signal it -- the failure mode a
// convergence check is the only way to catch.
func TestReplicaConvergesOnNonDeterministicWrites(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	mc := dialTx(t, masterAddr)
	defer mc.close()

	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	rc := dialTx(t, replicaAddr)
	defer rc.close()

	mc.cmd("SET sentinel 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET sentinel") == "1" })

	// A set big enough that a second random draw would almost certainly differ.
	for i := 0; i < 20; i++ {
		mc.cmd("SADD set m" + strconv.Itoa(i))
	}
	mc.cmd("SPOP set 12")
	mc.cmd("SPOP set")
	mc.cmd("SRANDMEMBER set 5") // a read: must change nothing anywhere

	// Ties at the boundary score make ZPOPMIN's choice depend on the index order.
	mc.cmd("ZADD z 1 a 1 b 1 c 1 d 2 e")
	mc.cmd("ZPOPMIN z 2")
	mc.cmd("ZPOPMAX z")
	mc.cmd("ZRANDMEMBER z 3")

	// The float increments propagate their result, TTL included.
	mc.cmd("SETEX f 100 10.5")
	mc.cmd("INCRBYFLOAT f 0.1")
	mc.cmd("HSET h f 1.5")
	mc.cmd("HINCRBYFLOAT h f 0.25")
	mc.cmd("HRANDFIELD h 2")
	mc.cmd("RANDOMKEY")

	mc.cmd("SET fence done")
	waitFor(t, "the write stream to drain", func() bool { return rc.cmd("GET fence") == "done" })

	for _, cmd := range []string{
		"SCARD set", "ZCARD z", "ZRANGE z 0 -1 WITHSCORES", "GET f", "HGET h f",
	} {
		if m, r := mc.cmd(cmd), rc.cmd(cmd); m != r {
			t.Errorf("%q: master %q != replica %q", cmd, m, r)
		}
	}
	if m, r := arrayFields(mc.cmd("SMEMBERS set")), arrayFields(rc.cmd("SMEMBERS set")); m != r {
		t.Errorf("surviving set members diverged: master %q, replica %q", m, r)
	}
	// The KEEPTTL in the propagated form is what keeps the replica's key volatile.
	if got := rc.cmd("TTL f"); got == ":-1" {
		t.Error("INCRBYFLOAT cleared the replica's TTL; the propagated SET needs KEEPTTL")
	}
}

// TestReplicaConvergesOnMultiKeyWrites covers the cross-key writes, which propagate
// verbatim: their effect is fully determined by their arguments, so the check is
// that both copies agree on both keys.
func TestReplicaConvergesOnMultiKeyWrites(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	mc := dialTx(t, masterAddr)
	defer mc.close()

	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	rc := dialTx(t, replicaAddr)
	defer rc.close()

	mc.cmd("SET sentinel 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET sentinel") == "1" })

	mc.cmd("SETEX src 100 v")
	mc.cmd("COPY src dst")
	mc.cmd("SADD sa x y")
	mc.cmd("SADD sb y z")
	mc.cmd("SMOVE sa sb x")
	mc.cmd("SINTERSTORE inter sa sb")
	mc.cmd("SUNIONSTORE union sa sb")
	mc.cmd("SDIFFSTORE diff sb sa")
	mc.cmd("RPUSH la a b c")
	mc.cmd("LMOVE la lb LEFT RIGHT")
	mc.cmd("RPOPLPUSH la lb")
	mc.cmd("SET ren v")
	mc.cmd("RENAMENX ren renamed")
	mc.cmd("MSET u1 a u2 b")
	mc.cmd("UNLINK u1 u2")

	mc.cmd("SET fence done")
	waitFor(t, "the write stream to drain", func() bool { return rc.cmd("GET fence") == "done" })

	for _, cmd := range []string{
		"GET dst", "SCARD sa", "SCARD sb", "SCARD inter", "SCARD union", "SCARD diff",
		"LRANGE la 0 -1", "LRANGE lb 0 -1", "GET renamed", "EXISTS ren",
		"EXISTS u1 u2", "DBSIZE",
	} {
		if m, r := mc.cmd(cmd), rc.cmd(cmd); m != r {
			t.Errorf("%q: master %q != replica %q", cmd, m, r)
		}
	}
	for _, key := range []string{"sa", "sb", "inter", "union", "diff"} {
		if m, r := arrayFields(mc.cmd("SMEMBERS "+key)), arrayFields(rc.cmd("SMEMBERS "+key)); m != r {
			t.Errorf("set %q diverged: master %q, replica %q", key, m, r)
		}
	}
	// A copied TTL has to arrive as a TTL, not as a permanent key.
	if got := rc.cmd("TTL dst"); got == ":-1" || got == ":-2" {
		t.Errorf("replica TTL for the copied key = %q; want a live expiry", got)
	}
}

// TestReplicaRejectsEveryWriteCommand walks the command table and requires each write
// to be refused on a replica. It is the check that the table's metadata is right for
// every new command at once: dispatch rejects a write on a replica only if the
// command declared itself one, and it only gets that far if the arity it declared
// admits the call.
func TestReplicaRejectsEveryWriteCommand(t *testing.T) {
	replica, addr, stop := startServer(t, store.New(8))
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, "127.0.0.1:1") // nothing listening: only the role matters

	c := dialTx(t, addr)
	defer c.close()
	waitFor(t, "the replica role", func() bool { return contains(c.cmd("INFO"), "role:replica") })

	names := make([]string, 0, len(commandTable))
	for name, cmd := range commandTable {
		if cmd.write {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) < 40 {
		t.Fatalf("only %d write commands registered; the table looks incomplete", len(names))
	}
	for _, name := range names {
		// The fewest arguments this command's arity admits, so dispatch reaches the
		// replica check rather than replying with an arity error.
		n := commandTable[name].arity
		if n < 0 {
			n = -n
		}
		args := make([]string, 0, n)
		args = append(args, name)
		for i := 1; i < n; i++ {
			args = append(args, "x")
		}
		if got := c.cmd(strings.Join(args, " ")); got != "-READONLY You can't write against a read only replica." {
			t.Errorf("%s on a replica = %q; want the READONLY error", name, got)
		}
	}
}

// TestNewWritesConvergeOnReplica covers the writes added with the sorted-set algebra,
// SORT ... STORE and the radius searches' STORE forms.
//
// Every one of them is propagated *verbatim*, which is a claim rather than an assumption:
// each is a pure function of the dataset the replica already has, because the orderings
// they produce break ties by the element itself rather than by a hash table's iteration
// (see commands_sort.go and store.sortedMembers). This is the test that the claim holds --
// a replica that recomputed a different order would show up here as a different key.
func TestNewWritesConvergeOnReplica(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	master := dialTx(t, masterAddr)
	defer master.close()

	replicaSrv, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replicaSrv.ReplicaOf(ctx, masterAddr)

	replica := dialTx(t, replicaAddr)
	defer replica.close()

	master.cmd("SET sentinel 1")
	waitFor(t, "the replica to attach", func() bool { return replica.cmd("GET sentinel") == "1" })

	for _, cmd := range []string{
		"ZADD z1 1 a 2 b 3 c",
		"ZADD z2 1 a 5 d",
		"SADD plain a e",
		"ZUNIONSTORE u 3 z1 z2 plain WEIGHTS 1 2 3 AGGREGATE MAX",
		"ZINTERSTORE i 2 z1 z2",
		"ZDIFFSTORE df 2 z1 z2",
		"ZRANGESTORE rs z1 1 2",
		"ZREMRANGEBYLEX z2 [a [a",
		"RPUSH ml 3 1 2",
		"MSET weight_1 10 weight_2 5 weight_3 1",
		"SORT ml BY weight_* STORE sorted",
		"SADD unordered 3 1 2",
		"SORT unordered BY nosort STORE forced",
		"GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania",
		"GEORADIUS Sicily 15 37 200 km STORE geostore",
		"GEORADIUS Sicily 15 37 200 km STOREDIST geodist",
		"LPUSHX ml 9",
		"RPUSHX nolist 9",
		"HMSET h f v g w",
		"SETBIT bits 100 1",
		"BITFIELD counters SET u8 0 250 INCRBY u8 0 10",
	} {
		if reply := master.cmd(cmd); strings.HasPrefix(reply, "-") {
			t.Fatalf("%q on the master -> %s", cmd, reply)
		}
	}

	// Every key the sequence produced has to read identically on both sides. The zero-score
	// and tie cases are the interesting ones: "forced" comes from a *set*, whose iteration
	// order is random per walk, so it only matches if the alphabetic ordering was applied.
	checks := []string{
		"ZRANGE u 0 -1 WITHSCORES",
		"ZRANGE i 0 -1 WITHSCORES",
		"ZRANGE df 0 -1 WITHSCORES",
		"ZRANGE rs 0 -1 WITHSCORES",
		"ZRANGE z2 0 -1 WITHSCORES",
		"LRANGE sorted 0 -1",
		"LRANGE forced 0 -1",
		"ZRANGE geostore 0 -1 WITHSCORES",
		"ZRANGE geodist 0 -1 WITHSCORES",
		"LRANGE ml 0 -1",
		"EXISTS nolist",
		"HGETALL h",
		"GET bits",
		"BITFIELD_RO counters GET u8 0",
	}
	waitFor(t, "the replica to catch up", func() bool {
		return replica.cmd("BITFIELD_RO counters GET u8 0") == master.cmd("BITFIELD_RO counters GET u8 0")
	})
	for _, q := range checks {
		want := master.cmd(q)
		got := replica.cmd(q)
		// A hash and a set have no order, and Go randomises map iteration, so comparing
		// the two replies as strings is a coin flip that says "diverged" when the copies
		// agree perfectly. Convergence here means the same *elements*; the commands whose
		// order is part of the answer (ZRANGE, LRANGE) are compared verbatim above and
		// below by falling through to the string compare.
		if unorderedReply(q) {
			if !sameElements(got, want) {
				t.Errorf("%q: replica %q, master %q (elements differ)", q, got, want)
			}
			continue
		}
		if got != want {
			t.Errorf("%q: replica %q, master %q", q, got, want)
		}
	}
}

// unorderedReply reports whether a command's reply is a collection with no defined
// order, so two agreeing copies may legitimately spell it differently.
func unorderedReply(q string) bool {
	switch strings.Fields(strings.ToUpper(q))[0] {
	case "HGETALL", "HKEYS", "HVALS", "SMEMBERS", "SINTER", "SUNION", "SDIFF", "CONFIG":
		return true
	}
	return false
}

// sameElements compares two flattened "[a b c]" array replies as multisets.
func sameElements(a, b string) bool {
	fa, fb := splitSpace(strings.Trim(a, "[]")), splitSpace(strings.Trim(b, "[]"))
	if len(fa) != len(fb) {
		return false
	}
	slices.Sort(fa)
	slices.Sort(fb)
	return slices.Equal(fa, fb)
}
