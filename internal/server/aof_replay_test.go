package server

import (
	"bufio"
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// directClient runs commands against a Server in-process and returns the reply in the
// same flattened form readReply gives a socket client, so a live server and one rebuilt
// from its own log can be compared command for command without a connection each.
type directClient struct {
	t *testing.T
	s *Server
}

func (c *directClient) cmd(cmd string) string {
	c.t.Helper()
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	c.s.dispatch(w, cmdArgs(strings.Fields(cmd)...))
	if err := w.Flush(); err != nil {
		c.t.Fatalf("flush: %v", err)
	}
	return readReply(c.t, bufio.NewReader(&buf))
}

// TestAOFReplaysPropagatedForms drives the widened command surface through a server
// with persistence on, then replays the log it wrote into a fresh server and requires
// the two datasets to match.
//
// It closes the loop on the propagation rewrites: a rewritten command has to be one
// the reader can apply again. A form the handler would reject (an absolute deadline of
// zero, a flag whose condition no longer holds, a random draw re-run) would replay as
// an error or as different data, and only a replay comparison catches that.
func TestAOFReplaysPropagatedForms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay.aof")
	logf, err := aof.Open(path, aof.SyncAlways)
	if err != nil {
		t.Fatal(err)
	}

	live := New(store.New(8))
	live.AttachAOF(logf)

	writes := []string{
		"SET plain v",
		"SET nx v NX",
		"SET withttl v EX 300",
		"SET keepttl v EX 300",
		"SET keepttl v2 KEEPTTL",
		"SETEX setex 300 v",
		"PSETEX psetex 300000 v",
		"SET getex v",
		"GETEX getex EX 300",
		"SET persisted v EX 300",
		"GETEX persisted PERSIST",
		"SET counter 10.5",
		"INCRBYFLOAT counter 0.25",
		"SETRANGE range 2 hi",
		"SET expnx v",
		"EXPIRE expnx 300 NX",
		"SET expgt v EX 100",
		"PEXPIRE expgt 300000 XX GT",
		"RPUSH list a b c d",
		"LSET list 0 A",
		"LINSERT list BEFORE c X",
		"LTRIM list 0 3",
		"LPOP list 1",
		"RPOPLPUSH list moved",
		"LMOVE list moved LEFT RIGHT",
		"HSET hash f1 v1 f2 v2",
		"HSETNX hash f3 v3",
		"HINCRBY hash n 5",
		"HINCRBYFLOAT hash fl 1.25",
		"HDEL hash f2",
		"SADD set a b c d e",
		"SPOP set 2",
		"SMOVE set otherset a",
		"SADD third c d z",
		"SINTERSTORE inter set third",
		"SUNIONSTORE union set third",
		"SDIFFSTORE diff third set",
		"ZADD zset 1 a 2 b 3 c 4 d",
		"ZINCRBY zset 10 a",
		"ZADD zset GT CH 99 b",
		"ZPOPMIN zset",
		"ZPOPMAX zset 1",
		"ZREMRANGEBYSCORE zset 100 200",
		"SET copysrc v EX 300",
		"COPY copysrc copydst",
		"SET renamed v",
		"RENAMENX renamed renamed2",
		"MSET u1 a u2 b",
		"UNLINK u1",
	}
	lc := &directClient{t: t, s: live}
	for _, cmd := range writes {
		if reply := lc.cmd(cmd); len(reply) > 0 && reply[0] == '-' {
			t.Fatalf("%q on the live server: %s", cmd, reply)
		}
	}
	// Reads whose result is random must have contributed nothing to the log.
	for _, cmd := range []string{"RANDOMKEY", "SRANDMEMBER set 3", "HRANDFIELD hash 2", "ZRANDMEMBER zset 2"} {
		lc.cmd(cmd)
	}
	if err := logf.Close(); err != nil {
		t.Fatal(err)
	}

	cmds, err := aof.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed := New(store.New(8))
	replayed.ReplayCommands(cmds)

	rc := &directClient{t: t, s: replayed}
	checks := []string{
		"DBSIZE",
		"GET plain", "GET nx", "GET keepttl", "GET counter", "GET range", "GET getex",
		"TTL withttl", "TTL keepttl", "TTL setex", "TTL psetex", "TTL getex",
		"TTL persisted", "TTL expnx", "TTL expgt", "TTL copysrc", "TTL copydst",
		"LRANGE list 0 -1", "LRANGE moved 0 -1",
		"HGET hash f1", "HGET hash f2", "HGET hash n", "HGET hash fl", "HLEN hash",
		"SCARD set", "SCARD otherset", "SCARD inter", "SCARD union", "SCARD diff",
		"ZRANGE zset 0 -1 WITHSCORES", "ZCARD zset",
		"GET copydst", "GET renamed2", "EXISTS renamed", "EXISTS u1 u2",
	}
	for _, cmd := range checks {
		if want, got := lc.cmd(cmd), rc.cmd(cmd); want != got {
			t.Errorf("%q: live %q != replayed %q", cmd, want, got)
		}
	}
	for _, key := range []string{"set", "otherset", "inter", "union", "diff"} {
		want := arrayFields(lc.cmd("SMEMBERS " + key))
		if got := arrayFields(rc.cmd("SMEMBERS " + key)); got != want {
			t.Errorf("set %q: live %q != replayed %q", key, want, got)
		}
	}
	// The hash comes out of a map, so compare it as a set of field/value tokens.
	if want, got := arrayFields(lc.cmd("HGETALL hash")), arrayFields(rc.cmd("HGETALL hash")); want != got {
		t.Errorf("HGETALL: live %q != replayed %q", want, got)
	}
	// The volatile keys must still be volatile, not silently made permanent.
	for _, key := range []string{"withttl", "keepttl", "setex", "psetex", "getex", "expnx", "expgt", "copydst"} {
		if got := rc.cmd("TTL " + key); got == ":-1" || got == ":-2" {
			t.Errorf("replayed key %q has TTL %q; want a live expiry", key, got)
		}
	}
	// And the persisted one must still be permanent.
	if got := rc.cmd("TTL persisted"); got != ":-1" {
		t.Errorf("replayed TTL for a persisted key = %q; want :-1", got)
	}
	// A rewritten INCRBYFLOAT keeps both the value and the volatility of its key.
	if got := rc.cmd("GET counter"); got != "10.75" {
		t.Errorf("replayed counter = %q; want 10.75", got)
	}
}

// TestAOFRewriteRoundTripsWidenedDataset checks that a compacting rewrite of the
// dataset the new commands build reconstructs it exactly: Dump has to cover every
// piece of state a command can create.
func TestAOFRewriteRoundTripsWidenedDataset(t *testing.T) {
	live := New(store.New(8))
	c := &directClient{t: t, s: live}
	for _, cmd := range []string{
		"SET str hello",
		"SETEX vol 300 v",
		"SETRANGE padded 3 x",
		"INCRBYFLOAT float 1.25",
		"RPUSH list a b c",
		"HSET hash f1 v1 f2 v2",
		"SADD set a b c",
		"ZADD zset 1 a 2.5 b",
		"ZADD infzset inf big -inf small",
	} {
		if reply := c.cmd(cmd); len(reply) > 0 && reply[0] == '-' {
			t.Fatalf("%q: %s", cmd, reply)
		}
	}

	replayed := New(store.New(8))
	replayed.ReplayCommands(live.store.Dump())
	rc := &directClient{t: t, s: replayed}

	for _, cmd := range []string{
		"DBSIZE", "GET str", "GET padded", "GET float",
		"LRANGE list 0 -1", "HLEN hash", "SCARD set",
		"ZRANGE zset 0 -1 WITHSCORES", "ZRANGE infzset 0 -1 WITHSCORES",
	} {
		if want, got := c.cmd(cmd), rc.cmd(cmd); want != got {
			t.Errorf("%q: live %q != replayed %q", cmd, want, got)
		}
	}
	if got := rc.cmd("TTL vol"); got == ":-1" || got == ":-2" {
		t.Errorf("rewritten TTL = %q; want a live expiry", got)
	}
	// The infinite scores have to survive the text form they are dumped in.
	if got := rc.cmd("ZSCORE infzset big"); got != "inf" {
		t.Errorf("replayed +inf score = %q; want inf", got)
	}
	if got := rc.cmd("ZSCORE infzset small"); got != "-inf" {
		t.Errorf("replayed -inf score = %q; want -inf", got)
	}
}
