package server

import (
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/store"
)

// migratePair stands up two real listening nodes and returns clients for both, with the
// slots of `tag` owned by the source and the migration already opened in both
// directions. It is the state a resharding script leaves a pair of nodes in between
// SETSLOT MIGRATING/IMPORTING and SETSLOT NODE.
type migratePair struct {
	src, dst   *Server
	csrc, cdst *directClient
	srcAddr    string
	dstHost    string
	dstPort    string
	slot       int
}

func newMigratePair(t *testing.T, tag string) *migratePair {
	t.Helper()
	src, srcAddr := startClusterNode(t, "")
	dst, dstAddr := startClusterNode(t, "")
	csrc := &directClient{t: t, s: src}
	cdst := &directClient{t: t, s: dst}

	// The source owns everything; the destination is told about the source and vice
	// versa, since a MOVED or ASK has to name a real address.
	csrc.cmd("CLUSTER ADDSLOTSRANGE 0 16383")
	srcHost, srcPort := splitHostPortTest(t, srcAddr)
	dstHost, dstPort := splitHostPortTest(t, dstAddr)
	if got := cdst.cmd("CLUSTER MEET " + srcHost + " " + srcPort); got != "+OK" {
		t.Fatalf("destination MEET source = %q", got)
	}
	if got := csrc.cmd("CLUSTER MEET " + dstHost + " " + dstPort); got != "+OK" {
		t.Fatalf("source MEET destination = %q", got)
	}

	slot := KeySlot(tag)
	srcID, dstID := src.cluster.myself().id, dst.cluster.myself().id
	if got := cdst.cmd("CLUSTER SETSLOT " + itoa(slot) + " IMPORTING " + srcID); got != "+OK" {
		t.Fatalf("SETSLOT IMPORTING = %q", got)
	}
	if got := csrc.cmd("CLUSTER SETSLOT " + itoa(slot) + " MIGRATING " + dstID); got != "+OK" {
		t.Fatalf("SETSLOT MIGRATING = %q", got)
	}
	return &migratePair{
		src: src, dst: dst, csrc: csrc, cdst: cdst,
		srcAddr: srcAddr, dstHost: dstHost, dstPort: dstPort, slot: slot,
	}
}

// TestMigrateMovesEveryType is the end-to-end move: real keys of every stored kind
// leave one running node and arrive at another over a socket, and the source no longer
// has them.
func TestMigrateMovesEveryType(t *testing.T) {
	p := newMigratePair(t, "{m}")

	build := [][]string{
		{"SET", "{m}:str", "hello"},
		{"RPUSH", "{m}:list", "a", "b", "c"},
		{"HSET", "{m}:hash", "f", "v", "g", "w"},
		{"SADD", "{m}:set", "x", "y"},
		{"ZADD", "{m}:zset", "1.5", "a", "2.5", "b"},
		{"SETBIT", "{m}:bits", "100", "1"},
		{"PFADD", "{m}:hll", "a", "b", "c"},
		{"GEOADD", "{m}:geo", "13.361389", "38.115556", "Palermo"},
		{"XADD", "{m}:stream", "1-1", "f", "v"},
		{"XGROUP", "CREATE", "{m}:stream", "grp", "0"},
		{"XREADGROUP", "GROUP", "grp", "alice", "COUNT", "1", "STREAMS", "{m}:stream", ">"},
	}
	bc := &binClient{t: t, s: p.src}
	for _, cmd := range build {
		if got := bc.do(cmd...); strings.HasPrefix(got, "-") {
			t.Fatalf("%v -> %s", cmd, got)
		}
	}
	keys := []string{
		"{m}:str", "{m}:list", "{m}:hash", "{m}:set", "{m}:zset",
		"{m}:bits", "{m}:hll", "{m}:geo", "{m}:stream",
	}

	// What each key looks like before it leaves.
	checks := []string{
		"GET {m}:str", "LRANGE {m}:list 0 -1", "HLEN {m}:hash", "SCARD {m}:set",
		"ZRANGE {m}:zset 0 -1 WITHSCORES", "BITCOUNT {m}:bits", "GET {m}:hll",
		"ZSCORE {m}:geo Palermo", "XRANGE {m}:stream - +", "XPENDING {m}:stream grp",
	}
	before := make([]string, len(checks))
	for i, check := range checks {
		before[i] = bc.do(strings.Fields(check)...)
	}

	// One MIGRATE for the whole batch, which is what a resharding loop issues.
	args := append([]string{"MIGRATE", p.dstHost, p.dstPort, "", "0", "5000", "KEYS"}, keys...)
	if got := bc.do(args...); got != "+OK" {
		t.Fatalf("MIGRATE = %q", got)
	}

	// The destination has every one of them, byte for byte.
	dbc := &binClient{t: t, s: p.dst}
	for i, check := range checks {
		if got := dbc.do(strings.Fields(check)...); got != before[i] {
			t.Errorf("%s after MIGRATE:\n source before  %s\n destination    %s",
				check, before[i], got)
		}
	}
	// A group's pending-entries list is work in flight, and it made the trip.
	if got := dbc.do("XPENDING", "{m}:stream", "grp"); !strings.Contains(got, "alice") {
		t.Errorf("the migrated stream's PEL = %q; want alice still holding an entry", got)
	}
	// And the source no longer has any of them: MIGRATE moves, it does not copy.
	for _, k := range keys {
		if got := bc.do("EXISTS", k); got != ":0" {
			t.Errorf("%s is still on the source after MIGRATE", k)
		}
	}
}

// TestMigrateOptions covers COPY, REPLACE and the NOKEY reply.
func TestMigrateOptions(t *testing.T) {
	p := newMigratePair(t, "{o}")
	bc, dbc := &binClient{t: t, s: p.src}, &binClient{t: t, s: p.dst}

	// A key that is not here is not an error: a resharding loop asks for a batch and
	// takes whatever is in it.
	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "{o}:nosuch", "0", "1000"); got != "+NOKEY" {
		t.Errorf("MIGRATE of a missing key = %q; want NOKEY", got)
	}

	// COPY leaves the key here as well.
	bc.do("SET", "{o}:copied", "v")
	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "{o}:copied", "0", "1000", "COPY"); got != "+OK" {
		t.Fatalf("MIGRATE ... COPY = %q", got)
	}
	if got := bc.do("GET", "{o}:copied"); got != "v" {
		t.Errorf("MIGRATE ... COPY removed the source key: %q", got)
	}
	if got := dbc.do("GET", "{o}:copied"); got != "v" {
		t.Errorf("MIGRATE ... COPY did not deliver the key: %q", got)
	}

	// Without REPLACE, a key the destination already holds is refused -- and refused
	// without taking the source's copy away, which is the property that matters.
	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "{o}:copied", "0", "1000"); !strings.Contains(got, "BUSYKEY") {
		t.Errorf("MIGRATE onto an existing key = %q; want the target's BUSYKEY quoted back", got)
	}
	if got := bc.do("GET", "{o}:copied"); got != "v" {
		t.Errorf("a failed MIGRATE removed the source key: %q", got)
	}
	// With REPLACE it goes through.
	bc.do("SET", "{o}:copied", "newer")
	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "{o}:copied", "0", "1000", "REPLACE"); got != "+OK" {
		t.Errorf("MIGRATE ... REPLACE = %q", got)
	}
	if got := dbc.do("GET", "{o}:copied"); got != "newer" {
		t.Errorf("after REPLACE the destination has %q", got)
	}
	if got := bc.do("EXISTS", "{o}:copied"); got != ":0" {
		t.Errorf("MIGRATE without COPY left the key on the source")
	}
}

// TestMigratePreservesTTL is the deadline crossing a node boundary. The destination is
// given the key's *remaining* life rather than its deadline, because RESTORE's operand
// counts from the destination's own clock and two nodes do not share one.
func TestMigratePreservesTTL(t *testing.T) {
	p := newMigratePair(t, "{t}")
	bc, dbc := &binClient{t: t, s: p.src}, &binClient{t: t, s: p.dst}

	bc.do("SET", "{t}:volatile", "v")
	bc.do("EXPIRE", "{t}:volatile", "600")
	bc.do("SET", "{t}:permanent", "v")

	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "", "0", "1000",
		"KEYS", "{t}:volatile", "{t}:permanent"); got != "+OK" {
		t.Fatalf("MIGRATE = %q", got)
	}
	if got := dbc.do("TTL", "{t}:volatile"); got != ":600" && got != ":599" {
		t.Errorf("the migrated key's TTL = %q; want ~600", got)
	}
	// A key with no deadline must not acquire one, which is why a passed deadline is
	// shipped as 1ms rather than as 0 -- 0 means "no expiry" to RESTORE.
	if got := dbc.do("TTL", "{t}:permanent"); got != ":-1" {
		t.Errorf("a permanent key arrived with TTL %q; want -1", got)
	}
}

// TestMigrateErrors covers the failures an operator has to be able to tell apart: a
// destination that is not there, and a syntactically wrong command.
func TestMigrateErrors(t *testing.T) {
	p := newMigratePair(t, "{e}")
	bc := &binClient{t: t, s: p.src}
	bc.do("SET", "{e}:k", "v")

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"MIGRATE", "127.0.0.1", "1", "{e}:k", "0", "200"},
			"-IOERR error or timeout connecting to the client"},
		{[]string{"MIGRATE", "127.0.0.1", "notaport", "{e}:k", "0", "200"},
			"-ERR Invalid target port"},
		{[]string{"MIGRATE", p.dstHost, p.dstPort, "{e}:k", "0", "notatimeout"},
			"-ERR timeout is not an integer or out of range"},
		{[]string{"MIGRATE", p.dstHost, p.dstPort, "{e}:k", "0", "1000", "NOPE"},
			"-ERR syntax error"},
		{[]string{"MIGRATE", p.dstHost, p.dstPort, "", "0", "1000"},
			"-ERR When using MIGRATE KEYS option, the key argument must be set to the empty string"},
		{[]string{"MIGRATE", p.dstHost, p.dstPort, "{e}:k", "0", "1000", "KEYS", "a"},
			"-ERR When using MIGRATE KEYS option, the key argument must be set to the empty string"},
	} {
		if got := bc.do(tc.args...); got != tc.want {
			t.Errorf("%v -> %q; want %q", tc.args, got, tc.want)
		}
	}
	// A failure leaves the key exactly where it was.
	if got := bc.do("GET", "{e}:k"); got != "v" {
		t.Errorf("a failed MIGRATE disturbed the source key: %q", got)
	}
}

// TestMigrateRedirectsDuringMigration is the whole point of the IMPORTING/MIGRATING
// states, driven through the client path: while a slot is open, a key that has moved
// draws an ASK to the destination, a key that has not is still served here, and the
// destination serves it only to a client that says ASKING.
func TestMigrateRedirectsDuringMigration(t *testing.T) {
	p := newMigratePair(t, "{r}")
	bc := &binClient{t: t, s: p.src}
	bc.do("SET", "{r}:stays", "here")
	bc.do("SET", "{r}:goes", "there")

	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "{r}:goes", "0", "1000"); got != "+OK" {
		t.Fatalf("MIGRATE = %q", got)
	}

	// The source: one key still here, one gone.
	sc := newSessionClient(t, p.src)
	if got := sc.cmd("GET {r}:stays"); got != "here" {
		t.Errorf("a key still on the source = %q; want it served", got)
	}
	askTo := p.dst.cluster.myself().addr()
	if got := sc.cmd("GET {r}:goes"); got != "-ASK "+itoa(p.slot)+" "+askTo {
		t.Errorf("a key that has already moved = %q; want an ASK to %s", got, askTo)
	}

	// The destination: MOVED for an ordinary client, served after ASKING.
	dc := newSessionClient(t, p.dst)
	movedTo := p.src.cluster.myself().addr()
	if got := dc.cmd("GET {r}:goes"); got != "-MOVED "+itoa(p.slot)+" "+movedTo {
		t.Errorf("an ordinary client on the importing node = %q", got)
	}
	dc.cmd("ASKING")
	if got := dc.cmd("GET {r}:goes"); got != "there" {
		t.Errorf("after ASKING the importing node answered %q; want the migrated value", got)
	}

	// Closing the migration on both sides settles the slot: the destination now owns it
	// outright and the source redirects with MOVED rather than ASK.
	dstID := p.dst.cluster.myself().id
	if got := p.csrc.cmd("CLUSTER SETSLOT " + itoa(p.slot) + " NODE " + dstID); !strings.HasPrefix(got, "-ERR Can't assign") {
		t.Errorf("SETSLOT NODE while the source still holds a key = %q; want a refusal", got)
	}
	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "{r}:stays", "0", "1000"); got != "+OK" {
		t.Fatalf("migrating the last key = %q", got)
	}
	if got := p.csrc.cmd("CLUSTER SETSLOT " + itoa(p.slot) + " NODE " + dstID); got != "+OK" {
		t.Fatalf("SETSLOT NODE on the source = %q", got)
	}
	if got := p.cdst.cmd("CLUSTER SETSLOT " + itoa(p.slot) + " NODE " + dstID); got != "+OK" {
		t.Fatalf("SETSLOT NODE on the destination = %q", got)
	}
	if got := sc.cmd("GET {r}:stays"); got != "-MOVED "+itoa(p.slot)+" "+askTo {
		t.Errorf("after the slot settled the source answered %q; want a MOVED", got)
	}
	if got := dc.cmd("GET {r}:stays"); got != "here" {
		t.Errorf("the new owner answered %q without ASKING; want it served outright", got)
	}
}

// TestMigratePropagatesDeletion is invariant 2 and 4 for MIGRATE: what reaches the AOF
// and the replicas is the effect on *this* node's dataset, which is the DEL of the keys
// that left -- never the MIGRATE itself.
//
// Shipping the command verbatim would have every replica open its own connection to the
// destination and send the same keys again, and an AOF replay would do it once more on
// every restart, to a node that may no longer exist.
func TestMigratePropagatesDeletion(t *testing.T) {
	p := newMigratePair(t, "{p}")
	bc := &binClient{t: t, s: p.src}
	bc.do("SET", "{p}:a", "1")
	bc.do("SET", "{p}:b", "2")

	next := tapReplica(t, p.src)
	bc.do("SET", "{p}:c", "3")
	next() // the SET

	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "", "0", "1000",
		"KEYS", "{p}:a", "{p}:b"); got != "+OK" {
		t.Fatalf("MIGRATE = %q", got)
	}
	shipped := next()
	if strings.ToUpper(string(shipped[0])) != "DEL" {
		t.Fatalf("MIGRATE propagated %q; want a DEL", fmtCmd(shipped))
	}
	if got := fmtCmd(shipped); got != "DEL {p}:a {p}:b" {
		t.Errorf("MIGRATE propagated %q; want the two keys that left", got)
	}

	// MIGRATE ... COPY changes nothing here, so it propagates nothing. The next command
	// on the wire is the one after it.
	if got := bc.do("MIGRATE", p.dstHost, p.dstPort, "{p}:c", "0", "1000", "COPY", "REPLACE"); got != "+OK" {
		t.Fatalf("MIGRATE ... COPY = %q", got)
	}
	bc.do("SET", "marker", "1")
	if got := fmtCmd(next()); got != "SET marker 1" {
		t.Errorf("MIGRATE ... COPY propagated %q; want nothing at all", got)
	}
}

// TestMigrateKeysAreTheOneExtraction is invariant 7 for MIGRATE, whose keys are neither
// at argument 1 nor at a fixed position: the address occupies the first three
// arguments, and the keys are either one operand or a variadic KEYS clause. WATCH,
// COMMAND GETKEYS and the cluster redirect all read the same extraction.
func TestMigrateKeysAreTheOneExtraction(t *testing.T) {
	s := New(store.New(8))
	c := &directClient{t: t, s: s}

	if got := c.cmd("COMMAND GETKEYS MIGRATE host 6379 mykey 0 1000"); got != "[mykey]" {
		t.Errorf("COMMAND GETKEYS for the single-key form = %q", got)
	}
	if got := c.cmd("COMMAND GETKEYS MIGRATE host 6379 EMPTY 0 1000 KEYS k1 k2 k3"); got != "-ERR The command has no key arguments" {
		// The literal "EMPTY" is not the empty string, so the KEYS form is rejected -- which
		// is exactly the mistake the error exists to catch.
		t.Errorf("COMMAND GETKEYS with a non-empty key and KEYS = %q", got)
	}
	keys := commandKeys("MIGRATE", cmdArgs("MIGRATE", "host", "6379", "", "0", "1000", "KEYS", "k1", "k2"))
	if strings.Join(keys, ",") != "k1,k2" {
		t.Errorf("the KEYS form extracts %v; want [k1 k2]", keys)
	}
	// And WATCH sees them, so a transaction guarding a key that MIGRATE moves away
	// aborts rather than committing over its disappearance.
	if got := strings.Join(affectedKeys("MIGRATE",
		cmdArgs("MIGRATE", "h", "1", "", "0", "1", "KEYS", "w1", "w2")), ","); got != "w1,w2" {
		t.Errorf("affectedKeys for MIGRATE = %q; want w1,w2", got)
	}
}
