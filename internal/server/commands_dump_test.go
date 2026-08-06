package server

import (
	"bufio"
	"bytes"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// fmtCmd renders a propagated command for a failure message.
func fmtCmd(args [][]byte) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = string(a)
	}
	return strings.Join(parts, " ")
}

// binClient is directClient for commands whose arguments or replies are not text: a
// DUMP payload contains NUL bytes and CRLFs, so it can neither be split out of a
// command string with strings.Fields nor compared as one.
type binClient struct {
	t *testing.T
	s *Server
}

// do runs a command given as pre-split arguments and returns the reply in readReply's
// flattened form. A bulk reply comes back as a Go string holding the exact bytes, so a
// payload can be round-tripped straight back in as an argument.
func (c *binClient) do(args ...string) string {
	c.t.Helper()
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	c.s.dispatch(w, cmdArgs(args...))
	if err := w.Flush(); err != nil {
		c.t.Fatalf("flush: %v", err)
	}
	return readReply(c.t, bufio.NewReader(&buf))
}

// sortedTokens normalizes a reply whose element order is not part of its meaning (a
// set's members, a hash's fields), so a comparison between the original key and the
// restored one is not at the mercy of two independent map iterations.
func sortedTokens(reply string) string {
	toks := strings.Fields(strings.Trim(reply, "[]{}~"))
	sort.Strings(toks)
	return strings.Join(toks, " ")
}

func newDumpServer(t *testing.T) *binClient {
	t.Helper()
	s := New(store.New(8))
	if err := s.SetDatabases(defaultDatabases); err != nil {
		t.Fatalf("SetDatabases: %v", err)
	}
	return &binClient{t: t, s: s}
}

// TestDumpRestoreEveryType is the round-trip that DUMP/RESTORE exists to make safe:
// every one of the ten Redis data types is built, serialized, restored under a second
// name, and compared against the original with reads that describe the whole of its
// state.
//
// "Ten types" is six stored kinds, because three of them share a representation -- a
// bitmap and a HyperLogLog are strings, a geo set is a sorted set -- and that sharing
// is exactly why each still needs its own case here: what must survive a bitmap is its
// bit pattern, what must survive a sketch is its register body byte for byte (so the
// other server's PFCOUNT agrees), and what must survive a geo set is a 52-bit score
// whose low bits are the position. A round-trip that merely produced "a string" or "a
// sorted set" would pass a weaker test and lose all three.
func TestDumpRestoreEveryType(t *testing.T) {
	c := newDumpServer(t)

	// Each case builds src, then names the reads that must agree between src and the
	// restored copy. unordered marks the reads whose element order is not meaning.
	cases := []struct {
		name      string
		src       string
		build     [][]string
		checks    [][]string // {command, key-position-placeholder...}; %s is the key
		unordered bool
	}{
		{
			name:  "string",
			src:   "str",
			build: [][]string{{"SET", "str", "hello world"}},
			checks: [][]string{
				{"GET", "%s"}, {"STRLEN", "%s"}, {"TYPE", "%s"},
			},
		},
		{
			name: "list",
			src:  "list",
			build: [][]string{
				{"RPUSH", "list", "a", "b", "c", "d"},
				{"LPUSH", "list", "z"},
			},
			checks: [][]string{
				{"LRANGE", "%s", "0", "-1"}, {"LLEN", "%s"}, {"TYPE", "%s"},
			},
		},
		{
			name:      "hash",
			src:       "hash",
			build:     [][]string{{"HSET", "hash", "f1", "v1", "f2", "v2", "f3", "v3"}},
			checks:    [][]string{{"HGETALL", "%s"}},
			unordered: true,
		},
		{
			name:      "set",
			src:       "set",
			build:     [][]string{{"SADD", "set", "alpha", "beta", "gamma"}},
			checks:    [][]string{{"SMEMBERS", "%s"}},
			unordered: true,
		},
		{
			name:  "zset",
			src:   "zset",
			build: [][]string{{"ZADD", "zset", "1.5", "a", "-2", "b", "inf", "c"}},
			checks: [][]string{
				{"ZRANGE", "%s", "0", "-1", "WITHSCORES"}, {"ZCARD", "%s"}, {"TYPE", "%s"},
			},
		},
		{
			name: "bitmap",
			src:  "bits",
			build: [][]string{
				{"SETBIT", "bits", "7", "1"}, {"SETBIT", "bits", "100", "1"},
				{"SETBIT", "bits", "4095", "1"},
			},
			checks: [][]string{
				{"BITCOUNT", "%s"}, {"GETBIT", "%s", "100"}, {"GETBIT", "%s", "4095"},
				{"BITPOS", "%s", "1"}, {"STRLEN", "%s"},
			},
		},
		{
			name: "hyperloglog-sparse",
			src:  "hllsparse",
			build: [][]string{
				{"PFADD", "hllsparse", "a", "b", "c", "d", "e"},
			},
			checks: [][]string{
				// The register body byte for byte, not merely the estimate: a sketch that
				// arrived with a different body would still count to five and would then
				// disagree with every other node the moment anything was merged into it.
				{"GET", "%s"}, {"PFCOUNT", "%s"}, {"PFDEBUG", "ENCODING", "%s"},
			},
		},
		{
			name: "hyperloglog-dense",
			src:  "hlldense",
			build: [][]string{
				{"PFADD", "hlldense", "a", "b", "c"},
				{"PFDEBUG", "TODENSE", "hlldense"},
			},
			checks: [][]string{
				{"GET", "%s"}, {"PFCOUNT", "%s"}, {"PFDEBUG", "ENCODING", "%s"},
			},
		},
		{
			name: "geo",
			src:  "geo",
			build: [][]string{
				{"GEOADD", "geo", "13.361389", "38.115556", "Palermo"},
				{"GEOADD", "geo", "15.087269", "37.502669", "Catania"},
			},
			checks: [][]string{
				// The score is the geohash, so an inexact round-trip moves the point.
				{"ZSCORE", "%s", "Palermo"}, {"ZSCORE", "%s", "Catania"},
				{"GEOPOS", "%s", "Palermo"}, {"GEOHASH", "%s", "Catania"},
				{"GEODIST", "%s", "Palermo", "Catania"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, b := range tc.build {
				if got := c.do(b...); strings.HasPrefix(got, "-") {
					t.Fatalf("building %s: %v -> %s", tc.name, b, got)
				}
			}
			payload := c.do("DUMP", tc.src)
			if strings.HasPrefix(payload, "-") || payload == "(nil)" {
				t.Fatalf("DUMP %s = %q", tc.src, payload)
			}
			dst := tc.src + ":copy"
			if got := c.do("RESTORE", dst, "0", payload); got != "+OK" {
				t.Fatalf("RESTORE %s = %q", dst, got)
			}
			for _, check := range tc.checks {
				a := make([]string, len(check))
				b := make([]string, len(check))
				for i, part := range check {
					a[i] = strings.ReplaceAll(part, "%s", tc.src)
					b[i] = strings.ReplaceAll(part, "%s", dst)
				}
				want, got := c.do(a...), c.do(b...)
				if tc.unordered {
					want, got = sortedTokens(want), sortedTokens(got)
				}
				if want != got {
					t.Errorf("%v: original %q, restored %q", check, want, got)
				}
			}
		})
	}
}

// TestDumpRestoreStreamWithGroups is the type whose value is not the whole of its
// state. A stream carries id counters no entry records and, in each consumer group, a
// last-delivered id, an entries-read counter, a consumer list and a pending-entries
// list -- the record of work handed out and not yet acknowledged.
//
// A round-trip that dropped any of it would look successful: the entries would all be
// there. What would break is invisible until later -- acknowledged work redelivered,
// outstanding work forgotten, a consumer that vanished, or the next XADD * minting an
// id a consumer has already seen. That is invariant 5's stream clause, and a migrated
// key has to honour it exactly as a replica seed does.
func TestDumpRestoreStreamWithGroups(t *testing.T) {
	c := newDumpServer(t)

	for _, cmd := range [][]string{
		{"XADD", "s", "1-1", "f", "v1"},
		{"XADD", "s", "2-1", "f", "v2"},
		{"XADD", "s", "3-1", "f", "v3"},
		{"XADD", "s", "4-1", "f", "v4"},
		{"XDEL", "s", "4-1"}, // so maxDeletedId and entriesAdded diverge from the entries
		{"XGROUP", "CREATE", "s", "g1", "0"},
		{"XGROUP", "CREATE", "s", "g2", "0"},
		// alice takes two entries and acknowledges one, leaving exactly one pending.
		{"XREADGROUP", "GROUP", "g1", "alice", "COUNT", "2", "STREAMS", "s", ">"},
		{"XACK", "s", "g1", "1-1"},
		// bob is created with nothing outstanding, which is the consumer a snapshot that
		// only walked the pending list would silently lose.
		{"XGROUP", "CREATECONSUMER", "s", "g1", "bob"},
	} {
		if got := c.do(cmd...); strings.HasPrefix(got, "-") {
			t.Fatalf("%v -> %s", cmd, got)
		}
	}

	payload := c.do("DUMP", "s")
	if payload == "(nil)" || strings.HasPrefix(payload, "-") {
		t.Fatalf("DUMP s = %q", payload)
	}
	if got := c.do("RESTORE", "s2", "0", payload); got != "+OK" {
		t.Fatalf("RESTORE s2 = %q", got)
	}

	// Everything a stream is, compared between the two keys.
	//
	// XINFO CONSUMERS is deliberately not in this list: it reports each consumer's idle
	// and inactive times in milliseconds, measured at the moment of the call, so two
	// calls disagree by however long the first took. Its stable half -- which consumers
	// exist and how much each holds -- is asserted separately below.
	for _, check := range []string{
		"XRANGE %s - +",
		"XLEN %s",
		"XINFO STREAM %s",
		"XINFO GROUPS %s",
		"XPENDING %s g1",
		"XPENDING %s g1 - + 10",
		"XPENDING %s g2",
	} {
		want := c.do(strings.Fields(strings.ReplaceAll(check, "%s", "s"))...)
		got := c.do(strings.Fields(strings.ReplaceAll(check, "%s", "s2"))...)
		if want != got {
			t.Errorf("%s:\n original  %s\n restored  %s", check, want, got)
		}
	}
	// The consumers themselves: same names, same amount of work outstanding.
	for _, field := range []string{"name alice", "pending :1", "name bob", "pending :0"} {
		if got := c.do("XINFO", "CONSUMERS", "s2", "g1"); !strings.Contains(got, field) {
			t.Errorf("restored XINFO CONSUMERS = %q; want %q in it", got, field)
		}
	}

	// The specifics the comparison above would also pass if both sides were empty.
	if got := c.do("XPENDING", "s2", "g1"); !strings.Contains(got, "2-1") ||
		!strings.Contains(got, "alice") {
		t.Errorf("restored PEL = %q; want alice still holding 2-1", got)
	}
	if got := c.do("XINFO", "CONSUMERS", "s2", "g1"); !strings.Contains(got, "bob") {
		t.Errorf("restored consumers = %q; want bob to have survived with nothing pending", got)
	}
	// The id counters no entry records: 4-1 was added and then deleted, so a stream that
	// carried only its entries would come back claiming three were ever added and that
	// nothing had been deleted -- and would then re-mint an id a consumer has seen.
	info := c.do("XINFO", "STREAM", "s2")
	if !strings.Contains(info, "max-deleted-entry-id 4-1") {
		t.Errorf("restored XINFO STREAM = %q; want max-deleted-entry-id 4-1", info)
	}
	if !strings.Contains(info, "entries-added :4") {
		t.Errorf("restored XINFO STREAM = %q; want entries-added 4", info)
	}
}

// TestDumpRestoreTTL covers the deadline, which DUMP deliberately does not carry: it
// is RESTORE's own operand, so a key can be restored under a different expiry from the
// one it had. ABSTTL says the operand is already an instant rather than a duration.
func TestDumpRestoreTTL(t *testing.T) {
	c := newDumpServer(t)
	c.do("SET", "k", "v")
	c.do("EXPIRE", "k", "1000")
	payload := c.do("DUMP", "k")

	// A payload restored with ttl 0 has no deadline at all, whatever the source had.
	if got := c.do("RESTORE", "noexp", "0", payload); got != "+OK" {
		t.Fatalf("RESTORE = %q", got)
	}
	if got := c.do("TTL", "noexp"); got != ":-1" {
		t.Errorf("TTL of a key restored with ttl 0 = %q; want -1", got)
	}

	// A relative ttl, in milliseconds.
	if got := c.do("RESTORE", "rel", "50000", payload); got != "+OK" {
		t.Fatalf("RESTORE = %q", got)
	}
	if got := c.do("TTL", "rel"); got != ":50" {
		t.Errorf("TTL after RESTORE ... 50000 = %q; want 50", got)
	}

	// An absolute deadline.
	at := time.Now().Add(80 * time.Second).UnixMilli()
	if got := c.do("RESTORE", "abs", strconv.FormatInt(at, 10), payload, "ABSTTL"); got != "+OK" {
		t.Fatalf("RESTORE ABSTTL = %q", got)
	}
	if got := c.do("TTL", "abs"); got != ":80" && got != ":79" {
		t.Errorf("TTL after RESTORE ... ABSTTL = %q; want ~80", got)
	}
	// A deadline already in the past leaves no key behind.
	if got := c.do("RESTORE", "gone", "1", payload, "ABSTTL"); got != "+OK" {
		t.Fatalf("RESTORE past ABSTTL = %q", got)
	}
	if got := c.do("EXISTS", "gone"); got != ":0" {
		t.Errorf("a key restored with a past deadline still exists: %q", got)
	}
	if got := c.do("RESTORE", "neg", "-1", payload); got != "-ERR Invalid TTL value, must be >= 0" {
		t.Errorf("RESTORE with a negative ttl = %q", got)
	}
}

// TestRestoreOptions covers REPLACE, BUSYKEY, IDLETIME and FREQ.
func TestRestoreOptions(t *testing.T) {
	c := newDumpServer(t)
	c.do("SET", "src", "value")
	payload := c.do("DUMP", "src")

	// Without REPLACE an existing key is refused, and refused without being touched.
	c.do("SET", "taken", "original")
	if got := c.do("RESTORE", "taken", "0", payload); got != "-BUSYKEY Target key name already exists." {
		t.Errorf("RESTORE onto an existing key = %q", got)
	}
	if got := c.do("GET", "taken"); got != "original" {
		t.Errorf("a refused RESTORE changed the key: %q", got)
	}
	if got := c.do("RESTORE", "taken", "0", payload, "REPLACE"); got != "+OK" {
		t.Errorf("RESTORE ... REPLACE = %q", got)
	}
	if got := c.do("GET", "taken"); got != "value" {
		t.Errorf("after REPLACE, GET = %q", got)
	}

	// IDLETIME is applied, so the key arrives with the age it had rather than looking
	// freshly written; FREQ is validated and has no effect (this server's sampler is
	// LRU, not LFU).
	if got := c.do("RESTORE", "idle", "0", payload, "IDLETIME", "120"); got != "+OK" {
		t.Errorf("RESTORE ... IDLETIME = %q", got)
	}
	if got := c.do("OBJECT", "IDLETIME", "idle"); got != ":120" {
		t.Errorf("OBJECT IDLETIME after RESTORE ... IDLETIME 120 = %q", got)
	}
	if got := c.do("RESTORE", "freq", "0", payload, "FREQ", "10"); got != "+OK" {
		t.Errorf("RESTORE ... FREQ = %q", got)
	}

	for _, tc := range []struct{ args, want string }{
		{"RESTORE bad 0 %p IDLETIME -1", "-ERR Invalid IDLETIME value, must be >= 0"},
		{"RESTORE bad 0 %p FREQ 300", "-ERR Invalid FREQ value, must be >= 0 and <= 255"},
		{"RESTORE bad 0 %p NOPE", "-ERR syntax error"},
		{"RESTORE bad 0 %p IDLETIME", "-ERR syntax error"},
		{"RESTORE bad notanumber %p", "-ERR value is not an integer or out of range"},
	} {
		args := strings.Fields(tc.args)
		for i := range args {
			if args[i] == "%p" {
				args[i] = payload
			}
		}
		if got := c.do(args...); got != tc.want {
			t.Errorf("%s -> %q; want %q", tc.args, got, tc.want)
		}
	}
}

// TestRestoreRejectsForeignPayload is the reason the payload is framed the way it is.
//
// This server does not implement Redis's RDB object encoding, so a payload from a real
// Redis must be *rejected*, not guessed at: a serialization that half-understood a
// foreign payload would produce a key with plausible contents and no error, which is
// the failure mode a checksum exists to prevent. Each of the three gates -- magic,
// version, checksum -- is defeated in turn here, and each must answer with Redis's own
// message so a client library that matches on it keeps working.
func TestRestoreRejectsForeignPayload(t *testing.T) {
	c := newDumpServer(t)
	c.do("SET", "src", "hello")
	good := c.do("DUMP", "src")
	const want = "-ERR DUMP payload version or checksum are wrong"

	// flip inverts one byte, so each corruption is a real change wherever the payload's
	// own bytes happen to fall.
	flip := func(s string, i int) string {
		b := []byte(s)
		b[i] ^= 0xff
		return string(b)
	}
	corrupt := map[string]string{
		"empty":     "",
		"too short": "SHARDKV1",
		// What a real redis:7-alpine answers DUMP with for SET k hello: a one-byte type,
		// a length-prefixed string, then its own two-byte RDB version and CRC-64/Jones.
		// It has to be refused rather than guessed at -- see the test's doc comment.
		"redis rdb string": "\x00\x05hello\x0b\x00\x9d\x8f\x1c\x1f\x92\x08\x1e\xb3",
		"foreign magic":    flip(good, 0),
		"truncated body":   good[:len(good)-12] + good[len(good)-10:],
		"flipped body byte": flip(good, len(dumpMagic)+
			(len(good)-len(dumpMagic)-dumpFooterLen)/2),
		"bad checksum":   flip(good, len(good)-1),
		"future version": good[:len(good)-10] + "\xff\xff" + good[len(good)-8:],
	}
	for name, payload := range corrupt {
		if got := c.do("RESTORE", "dst", "0", payload); got != want {
			t.Errorf("RESTORE with a %s payload = %q; want %q", name, got, want)
		}
		if got := c.do("EXISTS", "dst"); got != ":0" {
			t.Errorf("a rejected %s payload left a key behind", name)
		}
	}
	// And the good payload still works, so the gates are not simply refusing everything.
	if got := c.do("RESTORE", "dst", "0", good); got != "+OK" {
		t.Errorf("RESTORE with an intact payload = %q", got)
	}
}

// TestRestoreRejectsUnlistedCommands pins the whitelist. A payload is input a client
// controls, and its body is a command sequence: without the whitelist, a payload
// carrying FLUSHALL would be a remote wipe with a valid checksum. The checksum proves
// only that the bytes are intact, never that they are benign.
func TestRestoreRejectsUnlistedCommands(t *testing.T) {
	c := newDumpServer(t)
	c.do("SET", "witness", "still here")

	forged := string(encodeDumpPayload([][][]byte{
		cmdArgs("SET", "k", "v"),
		cmdArgs("FLUSHALL"),
	}))
	if got := c.do("RESTORE", "k", "0", forged); got != "-ERR Bad data format" {
		t.Errorf("RESTORE of a payload carrying FLUSHALL = %q; want a data-format error", got)
	}
	if got := c.do("GET", "witness"); got != "still here" {
		t.Errorf("the forged payload ran FLUSHALL: witness = %q", got)
	}
	// And it left no partial key: the SET that preceded the FLUSHALL was applied to the
	// scratch store, which is discarded.
	if got := c.do("EXISTS", "k"); got != ":0" {
		t.Errorf("a rejected payload left its first command's key behind: %q", got)
	}
}

// TestDumpMissingKey pins DUMP's null reply, which is what tells MIGRATE that a key it
// was asked to move is not there.
func TestDumpMissingKey(t *testing.T) {
	c := newDumpServer(t)
	if got := c.do("DUMP", "nosuch"); got != "(nil)" {
		t.Errorf("DUMP of a missing key = %q; want a null reply", got)
	}
	c.do("SET", "expiring", "v")
	c.do("PEXPIRE", "expiring", "1")
	time.Sleep(10 * time.Millisecond)
	if got := c.do("DUMP", "expiring"); got != "(nil)" {
		t.Errorf("DUMP of an expired key = %q; want a null reply", got)
	}
}

// TestRestorePropagatesAbsoluteDeadline is invariant 3 for RESTORE.
//
// RESTORE's ttl counts from now, so a replica applying it later -- or an AOF replaying
// it an hour later -- would give the key more life than it had. The propagated form
// therefore carries the deadline as an instant, with ABSTTL to say so. A ttl of 0 has
// nothing relative in it and must not grow an ABSTTL, which would turn "never expires"
// into "expired at the epoch".
func TestRestorePropagatesAbsoluteDeadline(t *testing.T) {
	s := New(store.New(8))
	c := &binClient{t: t, s: s}
	next := tapReplica(t, s)

	c.do("SET", "src", "v")
	payload := c.do("DUMP", "src")
	next() // the SET; DUMP is a read and propagates nothing

	c.do("RESTORE", "k", "60000", payload)
	got := next()
	if len(got) < 5 || strings.ToUpper(string(got[0])) != "RESTORE" {
		t.Fatalf("propagated %q", fmtCmd(got))
	}
	if strings.ToUpper(string(got[len(got)-1])) != "ABSTTL" {
		t.Errorf("RESTORE propagated as %q; want a trailing ABSTTL", fmtCmd(got))
	}
	atMs, ok := parseInt64(got[2])
	if !ok {
		t.Fatalf("propagated ttl %q is not a number", got[2])
	}
	if delta := atMs - time.Now().UnixMilli(); delta < 55_000 || delta > 60_000 {
		t.Errorf("propagated deadline is %d ms away; want ~60000", delta)
	}

	// A permanent key propagates verbatim.
	c.do("RESTORE", "k2", "0", payload)
	got = next()
	for _, a := range got {
		if strings.EqualFold(string(a), "ABSTTL") {
			t.Errorf("RESTORE with ttl 0 propagated as %q; want no ABSTTL", fmtCmd(got))
		}
	}
}

// TestRestoreReplayable closes the loop the propagated form opens: the command a
// replica or an AOF replay receives has to rebuild the same key. A payload is binary,
// so this is also the check that it survives the RESP encoding both hops use.
func TestRestoreReplayable(t *testing.T) {
	c := newDumpServer(t)
	for _, cmd := range [][]string{
		{"HSET", "h", "f", "v", "g", "w"},
		{"XADD", "st", "1-1", "a", "b"},
		{"XGROUP", "CREATE", "st", "grp", "0"},
		{"PFADD", "hll", "x", "y", "z"},
	} {
		c.do(cmd...)
	}

	replica := New(store.New(8))
	rc := &binClient{t: t, s: replica}
	for _, key := range []string{"h", "st", "hll"} {
		payload := c.do("DUMP", key)
		// Exactly what the master would ship: a ttl of 0 names no deadline, so there is
		// nothing to rewrite and cmdRestore propagates the command verbatim. (The rewritten
		// form is covered by TestRestorePropagatesAbsoluteDeadline above.)
		wire := cmdArgs("RESTORE", key, "0", payload)
		replica.applyCommand(resp.NewWriter(io.Discard), wire)
	}
	for _, check := range []string{"HGETALL h", "XRANGE st - +", "XINFO GROUPS st", "GET hll"} {
		want := c.do(strings.Fields(check)...)
		got := rc.do(strings.Fields(check)...)
		if sortedTokens(want) != sortedTokens(got) {
			t.Errorf("%s: master %q, replica %q", check, want, got)
		}
	}
}
