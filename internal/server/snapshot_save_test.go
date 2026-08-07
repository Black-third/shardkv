package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/store"
)

// snapServer is a server started with a snapshot path, the wire connection to drive it, and
// the shutdown. Snapshot tests always need all three together, and always need a *second*
// one pointed at the same file.
type snapServer struct {
	t    *testing.T
	srv  *Server
	conn net.Conn
	br   *bufio.Reader
	stop func()
}

// snapClock is the instant every snapshot test runs at. A frozen clock is what makes the
// round trip an *exact* comparison instead of an approximate one: a pending entry's delivery
// time, a consumer's idle time and a key's remaining TTL are all "now minus something
// recorded", so on a live clock they move between the two measurements and the only thing
// that can be compared is that they are both plausible. Frozen, they are equalities -- which
// is the difference between checking that the PEL survived and checking that it survived
// with the delivery times it had.
var snapClock = time.Unix(1_700_000_000, 0)

// startSnapServer starts a server whose snapshots go to path. It is a local copy of
// startTestServer rather than a parameter added to it, so the snapshot tests cannot change
// what every other test in the package starts.
func startSnapServer(t *testing.T, path string) *snapServer {
	t.Helper()
	st := store.New(16)
	st.SetClock(func() time.Time { return snapClock })
	s := New(st)
	if err := s.SetDatabases(defaultDatabases); err != nil {
		t.Fatalf("SetDatabases: %v", err)
	}
	s.SetEnableDebugCommand("yes")
	s.SetSnapshotPath(path)
	// No schedule: every save in these tests is asked for explicitly, so a background one
	// firing halfway through would make the assertions race.
	if !s.SetSaveSchedule("") {
		t.Fatal("SetSaveSchedule(\"\") was refused")
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	ss := &snapServer{t: t, srv: s, conn: conn, br: bufio.NewReader(conn)}
	ss.stop = func() {
		conn.Close()
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("server did not shut down in time")
		}
	}
	return ss
}

func (s *snapServer) cmd(format string, a ...any) string {
	s.t.Helper()
	line := fmt.Sprintf(format, a...)
	if _, err := s.conn.Write([]byte(line + "\r\n")); err != nil {
		s.t.Fatalf("write %q: %v", line, err)
	}
	return readReply(s.t, s.br)
}

func (s *snapServer) mustOK(format string, a ...any) {
	s.t.Helper()
	line := fmt.Sprintf(format, a...)
	if got := s.cmd("%s", line); strings.HasPrefix(got, "-") {
		s.t.Fatalf("%q -> %s", line, got)
	}
}

// populateEveryType writes one value of every stored kind, in two databases, with an
// absolute expiry on one of them. It returns the read commands whose replies define the
// state, which is what a round trip has to preserve -- not the key count.
//
// "Every type" means all ten Redis types: the six stored kinds plus the three that are a
// string or a sorted set underneath (bitmap, HyperLogLog, geo set), and the stream with
// groups, consumers and a non-empty pending-entries list, which is the one whose value is
// not the whole of its state (invariant 5).
func populateEveryType(s *snapServer) []string {
	s.mustOK("SELECT 0")
	s.mustOK("SET plain hello")
	s.mustOK("SET withttl volatile")
	// An absolute deadline far in the future, so the round trip can be compared exactly
	// rather than approximately: PEXPIRETIME reports the instant, and the instant is what
	// must survive (invariant 3).
	s.mustOK("PEXPIREAT withttl 4000000000000")
	s.mustOK("APPEND appended abc")
	s.mustOK("SETRANGE offsetstr 5 tail")
	s.mustOK("SET number 12345")
	s.mustOK("RPUSH mylist a b c d e")
	s.mustOK("HSET myhash f1 v1 f2 v2 f3 v3")
	s.mustOK("SADD myset alpha beta gamma")
	s.mustOK("SADD intset 1 2 3")
	s.mustOK("ZADD myzset 1 one 2.5 two 3 three")
	s.mustOK("ZADD infzset -inf low +inf high")
	// bitmap: a string addressed by bit
	s.mustOK("SETBIT bits 100 1")
	s.mustOK("SETBIT bits 7 1")
	// HyperLogLog: a string in Redis's HYLL format
	s.mustOK("PFADD hll a b c d e f g h i j")
	// geo: a sorted set of 52-bit geohashes
	s.mustOK("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania")
	// stream with two groups, consumers, and outstanding work in one of them
	s.mustOK("XADD orders 1000-1 item widget qty 3")
	s.mustOK("XADD orders 1000-2 item gizmo qty 1")
	s.mustOK("XADD orders 2000-1 item sprocket qty 7")
	s.mustOK("XADD orders 3000-1 item doomed qty 0")
	s.mustOK("XDEL orders 3000-1") // so maxDeletedId and entriesAdded diverge from the entries
	s.mustOK("XGROUP CREATE orders fulfil 0")
	s.mustOK("XGROUP CREATE orders audit $")
	s.mustOK("XGROUP CREATECONSUMER orders audit idle-consumer")
	s.mustOK("XREADGROUP GROUP fulfil alice COUNT 2 STREAMS orders >")
	s.mustOK("XREADGROUP GROUP fulfil bob COUNT 1 STREAMS orders >")
	s.mustOK("XACK orders fulfil 1000-1") // one acknowledged, the rest still in flight
	s.mustOK("XADD empty 5000-5 f v")
	s.mustOK("XDEL empty 5000-5") // a stream with history and no entries left

	// A second database, so the SELECT framing is exercised (invariant 11).
	s.mustOK("SELECT 7")
	s.mustOK("SET other-db-key value7")
	s.mustOK("RPUSH other-db-list x y")
	s.mustOK("PEXPIREAT other-db-key 4100000000000")
	s.mustOK("SELECT 0")

	return []string{
		"SELECT 0",
		"DBSIZE",
		"GET plain", "STRLEN plain", "OBJECT ENCODING plain",
		"GET withttl", "PEXPIRETIME withttl", "TTL withttl",
		"GET appended", "GETRANGE offsetstr 0 -1", "STRLEN offsetstr",
		"GET number", "OBJECT ENCODING number",
		"LRANGE mylist 0 -1", "LLEN mylist", "OBJECT ENCODING mylist",
		"HLEN myhash", "HGET myhash f1", "HGET myhash f2", "HGET myhash f3",
		"SORT_RO myhash BY nosort", // refuses on a hash; the *error* must be the same too
		"SCARD myset", "SISMEMBER myset alpha", "SISMEMBER myset beta", "SISMEMBER myset gamma",
		"SCARD intset", "OBJECT ENCODING intset",
		"ZRANGE myzset 0 -1 WITHSCORES", "ZCARD myzset", "OBJECT ENCODING myzset",
		"ZRANGE infzset 0 -1 WITHSCORES",
		"BITCOUNT bits", "GETBIT bits 100", "GETBIT bits 7", "STRLEN bits",
		"PFCOUNT hll", "GET hll",
		"ZSCORE Sicily Palermo", "ZSCORE Sicily Catania",
		"GEOPOS Sicily Palermo", "GEOHASH Sicily Palermo Catania",
		"GEODIST Sicily Palermo Catania",
		"XLEN orders", "XRANGE orders - +",
		"XINFO STREAM orders",
		"XPENDING orders fulfil",
		"XPENDING orders fulfil - + 10",
		"XPENDING orders audit",
		"XLEN empty", "XINFO STREAM empty",
		"TYPE orders", "TYPE empty", "TYPE Sicily", "TYPE hll", "TYPE bits",
		"SELECT 7",
		"DBSIZE",
		"GET other-db-key", "PEXPIRETIME other-db-key", "LRANGE other-db-list 0 -1",
		"SELECT 0",
	}
}

// stateOf runs the read battery and returns the replies, with the two order-dependent ones
// normalized: a set's members and a stream's consumer groups come out of a map, so their
// order is not part of the state and must not be compared as if it were.
func stateOf(s *snapServer, probes []string) []string {
	s.t.Helper()
	out := make([]string, 0, len(probes)+3)
	for _, p := range probes {
		out = append(out, p+" => "+s.cmd("%s", p))
	}
	// Sets and consumer-group listings, sorted here rather than left to map order.
	for _, p := range []string{"SMEMBERS myset", "SMEMBERS intset", "KEYS *"} {
		out = append(out, p+" => "+sortedArrayReply(s.cmd("%s", p)))
	}
	// XINFO GROUPS/CONSUMERS: sorted the same way, because the groups live in a map. The
	// pending counts and last-delivered ids inside them are the point -- that is the record
	// of work in flight (invariant 5).
	out = append(out, "XINFO GROUPS orders => "+sortedArrayReply(s.cmd("XINFO GROUPS orders")))
	for _, g := range []string{"fulfil", "audit"} {
		out = append(out, "XINFO CONSUMERS orders "+g+" => "+
			sortedArrayReply(s.cmd("XINFO CONSUMERS orders %s", g)))
	}
	return out
}

// sortedArrayReply sorts the elements of a flat "[a b c]" reply, and sorts the top level of a
// nested one. Anything else is returned unchanged.
func sortedArrayReply(reply string) string {
	if !strings.HasPrefix(reply, "[") || !strings.HasSuffix(reply, "]") {
		return reply
	}
	body := reply[1 : len(reply)-1]
	if body == "" {
		return reply
	}
	// Split at top level only, so a nested "[...]" stays one element.
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '[', '{':
			depth++
		case ']', '}':
			depth--
		case ' ':
			if depth == 0 {
				parts = append(parts, body[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, body[start:])
	sort.Strings(parts)
	return "[" + strings.Join(parts, " ") + "]"
}

// TestSnapshotRoundTripsEveryType is the whole point of the feature: dump, restart, compare
// the *state*. A key count would pass while every value was wrong, so what is compared is the
// reply to every read that can distinguish one state from another -- including a stream's id
// counters, its groups, its consumers and its pending-entries list, the absolute instant of
// every expiry, and the second database.
func TestSnapshotRoundTripsEveryType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")

	first := startSnapServer(t, path)
	probes := populateEveryType(first)
	before := stateOf(first, probes)
	if got := first.cmd("SAVE"); got != "+OK" {
		t.Fatalf("SAVE -> %s", got)
	}
	// The file exists and is not empty, which is the minimum an operator's backup script
	// checks before believing the +OK.
	fi, err := os.Stat(path)
	if err != nil || fi.Size() == 0 {
		t.Fatalf("SAVE reported OK but the file is %v (size %v)", err, fi)
	}
	first.stop()

	// A different process would be a different server; this is the same thing without the
	// fork: a fresh, empty Server pointed at the file that was just written.
	second := startSnapServer(t, path)
	defer second.stop()
	keys, savedAt, err := second.srv.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if keys == 0 {
		t.Fatal("LoadSnapshot restored no keys")
	}
	if savedAt.IsZero() {
		t.Error("LoadSnapshot reported no save time")
	}
	after := stateOf(second, probes)

	if len(before) != len(after) {
		t.Fatalf("probe counts differ: %d vs %d", len(before), len(after))
	}
	diffs := 0
	for i := range before {
		if before[i] != after[i] {
			diffs++
			t.Errorf("state differs after the round trip:\n  before: %s\n  after : %s",
				before[i], after[i])
		}
	}
	if diffs == 0 {
		t.Logf("%d state probes identical across the round trip", len(before))
	}
}

// TestSnapshotSurvivesAReloadInPlace is the same property through DEBUG RELOAD, which is what
// Redis's own suite leans on: it exercises save-then-load without a second server, so a
// regression in either half shows up in every test file that calls it.
func TestSnapshotSurvivesAReloadInPlace(t *testing.T) {
	for _, withPath := range []bool{true, false} {
		name := "with a snapshot file"
		path := ""
		if withPath {
			path = filepath.Join(t.TempDir(), "dump.skv")
		} else {
			name = "with snapshots disabled (in-memory round trip)"
		}
		t.Run(name, func(t *testing.T) {
			s := startSnapServer(t, path)
			defer s.stop()
			probes := populateEveryType(s)
			before := stateOf(s, probes)
			if got := s.cmd("DEBUG RELOAD"); got != "+OK" {
				t.Fatalf("DEBUG RELOAD -> %s", got)
			}
			after := stateOf(s, probes)
			for i := range before {
				if before[i] != after[i] {
					t.Errorf("state differs after DEBUG RELOAD:\n  before: %s\n  after : %s",
						before[i], after[i])
				}
			}
		})
	}
}

// TestSnapshotChunksLargeCollections is the round trip at a size where invariant 5's chunking
// is what makes it work: each collection spans several emitted commands, so a boundary that
// split an HSET field/value pair or a ZADD score/member pair would corrupt the reload -- and
// a command carrying the whole collection would exceed resp.MaxMultiBulk and be rejected by
// the reader, taking the entire snapshot with it.
func TestSnapshotChunksLargeCollections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	const n = 1500 // several chunks per collection at 256 elements each

	first := startSnapServer(t, path)
	for i := 0; i < n; i++ {
		first.mustOK("RPUSH biglist e%d", i)
		first.mustOK("HSET bighash f%d v%d", i, i)
		first.mustOK("SADD bigset m%d", i)
		first.mustOK("ZADD bigzset %d z%d", i, i)
		first.mustOK("XADD bigstream %d-1 seq %d", i+1, i)
	}
	first.mustOK("PEXPIREAT bighash 4200000000000")
	probes := []string{
		"LLEN biglist", "LRANGE biglist 0 4", "LRANGE biglist -5 -1",
		"HLEN bighash", "HGET bighash f0", "HGET bighash f1499", "PEXPIRETIME bighash",
		"SCARD bigset", "SISMEMBER bigset m0", "SISMEMBER bigset m1499",
		"ZCARD bigzset", "ZRANGE bigzset 0 3 WITHSCORES", "ZSCORE bigzset z1499",
		"XLEN bigstream", "XRANGE bigstream - + COUNT 2", "XINFO STREAM bigstream",
	}
	before := make([]string, 0, len(probes))
	for _, p := range probes {
		before = append(before, p+" => "+first.cmd("%s", p))
	}
	if got := first.cmd("SAVE"); got != "+OK" {
		t.Fatalf("SAVE -> %s", got)
	}
	first.stop()

	second := startSnapServer(t, path)
	defer second.stop()
	if _, _, err := second.srv.LoadSnapshot(); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	for i, p := range probes {
		if got := p + " => " + second.cmd("%s", p); got != before[i] {
			t.Errorf("differs:\n  before: %s\n  after : %s", before[i], got)
		}
	}
}

// TestSaveIsConsistentUnderConcurrentWrites is the honesty check on the guarantee the file
// comment states, and it is deliberately built on the hazard the sharded keyspace creates: a
// write that touches two keys in *different shards* must never be caught half-done.
//
// The invariant is conservation. Each writer shuttles one element back and forth between two
// lists with LMOVE, which takes both shards' locks in index order (invariant 8), so at every
// instant in memory `LLEN left + LLEN right` is exactly the number of elements seeded. A
// snapshot that walked shards one at a time while writes landed would read the element's
// destination shard after the move and its source shard before it -- counting the element
// twice -- or the reverse, and count it not at all. Either way the file would hold a state
// that never existed in memory, and nothing anywhere would report it. Holding every shard's
// read lock across the whole walk is what makes that impossible, and this test is what says
// so.
//
// It runs in both propagation modes on purpose. With an AOF attached, writes are already
// serialized by propMu and the shard locks are belt and braces; with neither an AOF nor a
// replica, writes are sharded-concurrent (invariant 1) and the shard locks are the *only*
// thing standing between the cut and a torn file. The second is the case that matters, and it
// is the one the claim in snapshot.go is about.
//
// LMOVE rather than MSET, and the difference is not incidental: MSET here is a loop of
// independent single-key Sets with no cross-shard lock at all, so a cut *can* land between
// its keys -- exactly as a concurrent MGET can. That is a property of MSET, not of the
// snapshot, and the guarantee stated in snapshot.go is worded to say so rather than to claim
// the atomicity MSET does not have.
func TestSaveIsConsistentUnderConcurrentWrites(t *testing.T) {
	for _, withAOF := range []bool{false, true} {
		name := "sharded-concurrent writes (no AOF, no replica)"
		if withAOF {
			name = "serialized writes (AOF attached)"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "dump.skv")
			s := startSnapServer(t, path)
			defer s.stop()
			if withAOF {
				logf, err := aof.Open(filepath.Join(dir, "dump.aof"), aof.SyncNo)
				if err != nil {
					t.Fatal(err)
				}
				defer logf.Close()
				s.srv.AttachAOF(logf)
			}

			const writers, perWriter = 4, 6
			for w := 0; w < writers; w++ {
				for e := 0; e < perWriter; e++ {
					s.mustOK("RPUSH left:%d e%d", w, e)
				}
			}
			stopWrites := make(chan struct{})
			done := make(chan struct{}, writers)
			for w := 0; w < writers; w++ {
				go func(w int) {
					defer func() { done <- struct{}{} }()
					c, err := net.Dial("tcp", s.srv.Addr().String())
					if err != nil {
						return
					}
					defer c.Close()
					br := bufio.NewReader(c)
					for n := 0; ; n++ {
						select {
						case <-stopWrites:
							return
						default:
						}
						// One command, two keys, two shards, one element in flight. The total
						// across the pair is invariant in memory, so it must be invariant in
						// every snapshot.
						if n%2 == 0 {
							fmt.Fprintf(c, "LMOVE left:%d right:%d LEFT RIGHT\r\n", w, w)
						} else {
							fmt.Fprintf(c, "LMOVE right:%d left:%d LEFT RIGHT\r\n", w, w)
						}
						if _, err := parseReply(br); err != nil {
							return
						}
					}
				}(w)
			}

			checked := 0
			for i := 0; i < 8; i++ {
				time.Sleep(10 * time.Millisecond)
				if err := s.srv.SaveSnapshot(); err != nil {
					t.Fatalf("SaveSnapshot: %v", err)
				}
				checked += requireUntornSnapshot(t, path, writers, perWriter)
			}
			close(stopWrites)
			for i := 0; i < writers; i++ {
				<-done
			}
			if checked != writers*8 {
				t.Fatalf("checked %d pairs, expected %d: a snapshot was missing a pair entirely",
					checked, writers*8)
			}
			t.Logf("checked %d cross-shard pairs across 8 snapshots taken under concurrent writes",
				checked)
		})
	}
}

// TestSaveDoesNotSplitATransactionWhenPropagationIsActive is the same check one level up, and
// it is scoped exactly as narrowly as the implementation earns.
//
// With an AOF or a replica attached, EXEC holds propMu across the whole batch (invariant 1)
// and the cut takes propMu too, so a transaction is either wholly in the snapshot or wholly
// out of it. Without either, EXEC deliberately does *not* take propMu -- a pure cache keeps
// sharded-concurrent writes -- so its commands are applied one at a time and a cut can land
// between them. That is a property of the transaction implementation and not of the snapshot:
// a concurrent reader on such a server can already observe a half-applied batch. The test
// therefore asserts it only in the configuration where it is true, and the file comment says
// so rather than claiming the stronger guarantee everywhere.
func TestSaveDoesNotSplitATransactionWhenPropagationIsActive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.skv")
	s := startSnapServer(t, path)
	defer s.stop()
	logf, err := aof.Open(filepath.Join(dir, "dump.aof"), aof.SyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer logf.Close()
	s.srv.AttachAOF(logf)

	const writers, perWriter = 4, 6
	for w := 0; w < writers; w++ {
		for e := 0; e < perWriter; e++ {
			s.mustOK("RPUSH left:%d e%d", w, e)
		}
	}
	stopWrites := make(chan struct{})
	done := make(chan struct{}, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			c, err := net.Dial("tcp", s.srv.Addr().String())
			if err != nil {
				return
			}
			defer c.Close()
			br := bufio.NewReader(c)
			for n := 0; ; n++ {
				select {
				case <-stopWrites:
					return
				default:
				}
				// Two separate single-key writes inside one transaction: the pair is conserved
				// only because EXEC applies them together, which is what is being checked.
				if n%2 == 0 {
					fmt.Fprintf(c, "MULTI\r\nLPOP left:%d\r\nRPUSH right:%d e\r\nEXEC\r\n", w, w)
				} else {
					fmt.Fprintf(c, "MULTI\r\nLPOP right:%d\r\nRPUSH left:%d e\r\nEXEC\r\n", w, w)
				}
				for i := 0; i < 4; i++ {
					if _, err := parseReply(br); err != nil {
						return
					}
				}
			}
		}(w)
	}
	checked := 0
	for i := 0; i < 8; i++ {
		time.Sleep(10 * time.Millisecond)
		if err := s.srv.SaveSnapshot(); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		checked += requireUntornSnapshot(t, path, writers, perWriter)
	}
	close(stopWrites)
	for i := 0; i < writers; i++ {
		<-done
	}
	if checked != writers*8 {
		t.Fatalf("checked %d pairs, expected %d", checked, writers*8)
	}
	t.Logf("checked %d transactionally-written pairs across 8 snapshots", checked)
}

// requireUntornSnapshot replays a snapshot into a private server and checks that every
// left/right pair still holds exactly the number of elements that was seeded into it. It
// returns how many pairs it checked, so a caller can refuse to call an empty check a pass.
func requireUntornSnapshot(t *testing.T, path string, writers, perWriter int) int {
	t.Helper()
	side := New(store.New(16))
	if err := side.SetDatabases(defaultDatabases); err != nil {
		t.Fatal(err)
	}
	side.SetSnapshotPath(path)
	if _, _, err := side.LoadSnapshot(); err != nil {
		t.Fatalf("loading the snapshot just written: %v", err)
	}
	db := side.DB(0)
	pairs := 0
	for w := 0; w < writers; w++ {
		left, lerr := db.LLen(fmt.Sprintf("left:%d", w))
		right, rerr := db.LLen(fmt.Sprintf("right:%d", w))
		if lerr != nil || rerr != nil {
			t.Fatalf("LLen: %v / %v", lerr, rerr)
		}
		if left+right != perWriter {
			t.Errorf("snapshot is torn: left:%d has %d elements and right:%d has %d, total %d "+
				"but %d were seeded -- the cut caught two shards at different instants",
				w, left, w, right, left+right, perWriter)
		}
		pairs++
	}
	return pairs
}

// TestSaveRefusedWhenDisabled: a save that cannot happen must say so rather than answer OK.
// A backup script that reads +OK and finds no file has no way to tell what went wrong.
func TestSaveRefusedWhenDisabled(t *testing.T) {
	s := startSnapServer(t, "")
	defer s.stop()
	for _, c := range []string{"SAVE", "BGSAVE", "BGSAVE SCHEDULE"} {
		got := s.cmd("%s", c)
		if !strings.HasPrefix(got, "-ERR") || !strings.Contains(got, "snapshots are disabled") {
			t.Errorf("%s with no snapshot path -> %q; want an error naming the reason", c, got)
		}
	}
	if got := s.cmd("BGSAVE NOSAVE"); got != "-ERR syntax error" {
		t.Errorf("BGSAVE NOSAVE -> %q, want -ERR syntax error", got)
	}
	if got := s.cmd("BGSAVE SCHEDULE EXTRA"); got != "-ERR syntax error" {
		t.Errorf("BGSAVE SCHEDULE EXTRA -> %q, want -ERR syntax error", got)
	}
}

// TestBGSaveWritesAndReportsProgress covers the asynchronous form end to end: it returns
// Redis's status immediately, the file appears, and the counters INFO reports move.
func TestBGSaveWritesAndReportsProgress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	s := startSnapServer(t, path)
	defer s.stop()
	s.mustOK("SET k v")

	if got := s.cmd("BGSAVE"); got != "+Background saving started" {
		t.Fatalf("BGSAVE -> %q", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.srv.SnapshotSaves() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s.srv.SnapshotSaves() != 1 {
		t.Fatalf("rdb_saves = %d after a BGSAVE, want 1", s.srv.SnapshotSaves())
	}
	if s.srv.SnapshotInProgress() {
		t.Error("rdb_bgsave_in_progress is still set after the save finished")
	}
	if s.srv.SnapshotStatus() != "ok" {
		t.Errorf("rdb_last_bgsave_status = %q", s.srv.SnapshotStatus())
	}
	if s.srv.SnapshotCurrentDurationSec() != -1 {
		t.Errorf("rdb_current_bgsave_time_sec = %d with no save running, want -1",
			s.srv.SnapshotCurrentDurationSec())
	}
	if s.srv.SnapshotLastDurationSec() < 0 {
		t.Errorf("rdb_last_bgsave_time_sec = %d after a save, want >= 0",
			s.srv.SnapshotLastDurationSec())
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("BGSAVE said it started but no file appeared: %v", err)
	}
	// LASTSAVE now reports the save, not the server's start time, and the change counter is
	// back to zero because everything the counter was counting is on disk.
	if got := s.cmd("LASTSAVE"); got == ":0" {
		t.Errorf("LASTSAVE -> %q", got)
	}
	if got := s.srv.DirtyChanges(); got != 0 {
		t.Errorf("rdb_changes_since_last_save = %d right after a save, want 0", got)
	}
	// And the field INFO already reports from real state carries it.
	info := s.cmd("INFO persistence")
	if !strings.Contains(info, fmt.Sprintf("rdb_last_save_time:%d", s.srv.LastSave())) {
		t.Errorf("INFO persistence does not carry the new rdb_last_save_time:\n%s", info)
	}
	if !strings.Contains(info, "rdb_changes_since_last_save:0") {
		t.Errorf("INFO persistence does not report the cleared change counter:\n%s", info)
	}
}

// TestSaveWhileSavingIsRefusedWithRedisMessage: the two commands share one flag, so either
// arriving during a save gets Redis's message, and BGSAVE SCHEDULE gets Redis's other one.
func TestSaveWhileSavingIsRefusedWithRedisMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	s := startSnapServer(t, path)
	defer s.stop()
	s.mustOK("SET k v")

	// Hold the flag by hand: a real save is far too quick to collide with reliably, and a
	// test that waits for a race is a test that passes by luck.
	if !s.srv.snap().saving.CompareAndSwap(false, true) {
		t.Fatal("the saving flag was already set")
	}
	if got := s.cmd("SAVE"); got != "-ERR Background save already in progress" {
		t.Errorf("SAVE during a save -> %q", got)
	}
	if got := s.cmd("BGSAVE"); got != "-ERR Background save already in progress" {
		t.Errorf("BGSAVE during a save -> %q", got)
	}
	if got := s.cmd("BGSAVE SCHEDULE"); got != "+Background saving scheduled" {
		t.Errorf("BGSAVE SCHEDULE during a save -> %q", got)
	}
	if !s.srv.SnapshotInProgress() {
		t.Error("rdb_bgsave_in_progress is not set while a save is held")
	}
	s.srv.snap().saving.Store(false)
}

// TestSaveScheduleFires checks the `save <seconds> <changes>` schedule: a rule whose change
// threshold is met and whose interval has passed starts a save, and one whose threshold is
// not met does not.
func TestSaveScheduleFires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	s := startSnapServer(t, path)
	defer s.stop()

	if !s.srv.SetSaveSchedule("0 3") { // three changes, no waiting
		t.Fatal("SetSaveSchedule was refused")
	}
	if got := s.srv.SaveSchedule(); got != "0 3" {
		t.Errorf("SaveSchedule() = %q", got)
	}
	s.mustOK("SET a 1")
	s.mustOK("SET b 2")
	s.srv.maybeScheduledSave()
	if s.srv.SnapshotSaves() != 0 {
		t.Fatal("the schedule fired at two changes with a threshold of three")
	}
	s.mustOK("SET c 3")
	s.srv.maybeScheduledSave()
	deadline := time.Now().Add(5 * time.Second)
	for s.srv.SnapshotSaves() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s.srv.SnapshotSaves() != 1 {
		t.Fatalf("the schedule did not fire at three changes (rdb_saves=%d)", s.srv.SnapshotSaves())
	}
}

// TestSaveScheduleParsing pins what the spec accepts, because a schedule that was quietly
// accepted and does something else is a durability setting that lies.
func TestSaveScheduleParsing(t *testing.T) {
	s := startSnapServer(t, "")
	defer s.stop()
	good := map[string]string{
		"3600 1 300 100 60 10000": "3600 1 300 100 60 10000",
		"":                        "",
		"  900   1  ":             "900 1",
		"0 0":                     "0 0",
	}
	for spec, want := range good {
		if !s.srv.SetSaveSchedule(spec) {
			t.Errorf("SetSaveSchedule(%q) was refused", spec)
			continue
		}
		if got := s.srv.SaveSchedule(); got != want {
			t.Errorf("SetSaveSchedule(%q) then SaveSchedule() = %q, want %q", spec, got, want)
		}
	}
	for _, spec := range []string{"900", "abc 1", "900 -1", "-1 900", "900 1 300"} {
		if s.srv.SetSaveSchedule(spec) {
			t.Errorf("SetSaveSchedule(%q) was accepted", spec)
		}
	}
	// A refused spec must leave the previous one in place rather than half of the new one.
	s.srv.SetSaveSchedule("60 10")
	s.srv.SetSaveSchedule("60 10 bogus 5")
	if got := s.srv.SaveSchedule(); got != "60 10" {
		t.Errorf("a refused spec changed the schedule to %q", got)
	}
}

// TestLoadRefusesACorruptSnapshot: the load must fail loudly and leave the dataset alone. A
// partial load would present a subset of the data as the whole of it, and the next save would
// then write that subset over the good copy.
func TestLoadRefusesACorruptSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	first := startSnapServer(t, path)
	first.mustOK("SET a 1")
	first.mustOK("SET b 2")
	first.mustOK("SET c 3")
	if got := first.cmd("SAVE"); got != "+OK" {
		t.Fatalf("SAVE -> %s", got)
	}
	first.stop()

	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, blob[:len(blob)-12], 0o644); err != nil {
		t.Fatal(err)
	}

	second := startSnapServer(t, path)
	defer second.stop()
	keys, _, err := second.srv.LoadSnapshot()
	if err == nil {
		t.Fatalf("a truncated snapshot loaded %d keys; it must be refused", keys)
	}
	if got := second.cmd("DBSIZE"); got != ":0" {
		t.Errorf("a refused load left %s keys behind", got)
	}
}

// TestLoadedSnapshotReportsItsOwnSaveTime: LASTSAVE after a restart is about the file, not
// about this process. Redis reports its own start time here; a backup taken three days ago is
// not a save that happened at boot, and the whole use of the field is to answer "how stale is
// the copy on disk".
func TestLoadedSnapshotReportsItsOwnSaveTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	first := startSnapServer(t, path)
	first.mustOK("SET a 1")
	if got := first.cmd("SAVE"); got != "+OK" {
		t.Fatalf("SAVE -> %s", got)
	}
	saved := first.srv.LastSave()
	first.stop()

	second := startSnapServer(t, path)
	defer second.stop()
	if _, _, err := second.srv.LoadSnapshot(); err != nil {
		t.Fatal(err)
	}
	if got := second.srv.LastSave(); got != saved {
		t.Errorf("LASTSAVE after loading = %d, want the file's %d", got, saved)
	}
	if got := second.srv.SnapshotLoadedKeys(); got != 1 {
		t.Errorf("rdb_last_load_keys_loaded = %d, want 1", got)
	}
	if got := second.srv.DirtyChanges(); got != 0 {
		t.Errorf("rdb_changes_since_last_save = %d right after a load, want 0", got)
	}
}

// TestSnapshotIsNotPropagated: a save changes nothing, so it must not appear on the AOF or
// the replica stream, and DEBUG RELOAD must not either -- a replica replaying a FLUSHALL and
// a reload would be reconstructing a dataset it already has, and any failure in the middle
// would leave it short.
func TestSnapshotIsNotPropagated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	s := startSnapServer(t, path)
	defer s.stop()
	s.mustOK("SET k v")

	for _, name := range []string{"SAVE", "BGSAVE", "LASTSAVE", "DEBUG"} {
		c, ok := commandTable[name]
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if c.write {
			t.Errorf("%s is registered as a write; a save changes nothing and must not be "+
				"persisted or replicated", name)
		}
	}
}
