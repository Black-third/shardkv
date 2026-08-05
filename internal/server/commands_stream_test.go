package server

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// TestStreamBasics covers the non-group surface at the wire level: explicit and
// generated ids, NOMKSTREAM, the range forms including the exclusive one, deletion and
// trimming.
func TestStreamBasics(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"XADD s 1-1 a 1", "1-1"},
		{"XADD s 1-2 b 2", "1-2"},
		{"XADD s 2-1 c 3", "2-1"},
		{"XLEN s", ":3"},
		{"TYPE s", "+stream"},
		// An id must sort after the top item, and 0-0 is never allowed.
		{"XADD s 1-2 x 1", "-ERR The ID specified in XADD is equal or smaller than the target stream top item"},
		{"XADD s 0-0 x 1", "-ERR The ID specified in XADD is equal or smaller than the target stream top item"},
		{"XADD s bogus x 1", "-ERR Invalid stream ID specified as stream command argument"},
		// An odd field list is an arity error, not a half-written entry.
		{"XADD s 3-1 lonely", "-ERR wrong number of arguments for 'xadd' command"},
		{"XLEN s", ":3"},

		{"XRANGE s - +", "[[1-1 [a 1]] [1-2 [b 2]] [2-1 [c 3]]]"},
		{"XRANGE s 1 1", "[[1-1 [a 1]] [1-2 [b 2]]]"},
		{"XRANGE s - + COUNT 2", "[[1-1 [a 1]] [1-2 [b 2]]]"},
		{"XREVRANGE s + -", "[[2-1 [c 3]] [1-2 [b 2]] [1-1 [a 1]]]"},
		{"XREVRANGE s + - COUNT 1", "[[2-1 [c 3]]]"},
		// The exclusive forms: "(1-1" skips that one entry, "(1" skips the millisecond.
		{"XRANGE s (1-1 +", "[[1-2 [b 2]] [2-1 [c 3]]]"},
		{"XRANGE s (1 +", "[[2-1 [c 3]]]"},
		{"XRANGE s - (2-1", "[[1-1 [a 1]] [1-2 [b 2]]]"},
		{"XRANGE s missing +", "-ERR Invalid stream ID specified as stream command argument"},
		{"XRANGE nosuch - +", "[]"},
		{"XLEN nosuch", ":0"},

		{"XDEL s 1-2 9-9", ":1"},
		{"XRANGE s - +", "[[1-1 [a 1]] [2-1 [c 3]]]"},

		// NOMKSTREAM refuses to create a key; the reply is a null id.
		{"XADD nope NOMKSTREAM * f v", "(nil)"},
		{"EXISTS nope", ":0"},

		// Trimming, in both strategies and with both markers.
		{"XADD t 1-1 a 1", "1-1"},
		{"XADD t 2-1 b 2", "2-1"},
		{"XADD t 3-1 c 3", "3-1"},
		{"XTRIM t MAXLEN 2", ":1"},
		{"XRANGE t - +", "[[2-1 [b 2]] [3-1 [c 3]]]"},
		{"XTRIM t MAXLEN ~ 1", ":1"},
		{"XLEN t", ":1"},
		{"XADD t MAXLEN = 1 4-1 d 4", "4-1"},
		{"XRANGE t - +", "[[4-1 [d 4]]]"},
		{"XADD t MINID 5 5-1 e 5", "5-1"},
		{"XRANGE t - +", "[[5-1 [e 5]]]"},
		{"XTRIM t LIMIT 5", "-ERR syntax error"},
		{"XTRIM t MAXLEN 1 LIMIT 5", "-ERR syntax error, LIMIT cannot be used without the special ~ option"},
		{"XADD t MAXLEN 1 LIMIT 5 6-1 f 6", "-ERR syntax error, LIMIT cannot be used without the special ~ option"},

		// Wrong type, both ways round.
		{"SET str v", "+OK"},
		{"XADD str 1-1 a 1", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"XLEN str", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"GET s", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A generated id is the clock's millisecond with a sequence, and it sorts after
	// everything already there.
	auto := c.cmd("XADD s * f v")
	if !strings.Contains(auto, "-") {
		t.Fatalf("XADD s * returned %q; want an <ms>-<seq> id", auto)
	}
	if got := c.cmd("XLEN s"); got != ":3" {
		t.Errorf("XLEN after the generated append = %q; want :3", got)
	}
	// The "<ms>-*" form takes the millisecond and generates the sequence.
	if got := c.cmd("XADD seqs 5-* a 1"); got != "5-0" {
		t.Errorf("XADD seqs 5-* = %q; want 5-0", got)
	}
	if got := c.cmd("XADD seqs 5-* a 2"); got != "5-1" {
		t.Errorf("a second XADD seqs 5-* = %q; want 5-1", got)
	}
	if got := c.cmd("XADD seqs 4-* a 3"); !strings.HasPrefix(got, "-ERR The ID specified") {
		t.Errorf("XADD seqs 4-* after 5-1 = %q; want the too-small error", got)
	}
}

// TestStreamIDMonotonicityAcrossAClockJump is the invariant that makes generated ids
// safe: an id never sorts before one already in the stream, whatever the clock does.
func TestStreamIDMonotonicityAcrossAClockJump(t *testing.T) {
	st := store.New(4)
	now := time.UnixMilli(1_700_000_000_000)
	st.SetClock(func() time.Time { return now })

	first, _, _, err := st.XAdd("s", store.XAddOptions{Auto: true}, [][]byte{[]byte("f"), []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	// The clock jumps a minute backwards, which is what an NTP step or a restored VM
	// snapshot looks like.
	now = now.Add(-time.Minute)
	second, _, _, err := st.XAdd("s", store.XAddOptions{Auto: true}, [][]byte{[]byte("f"), []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if second.Compare(first) <= 0 {
		t.Fatalf("after a backwards clock jump, %s does not sort after %s", second, first)
	}
	if second.Ms != first.Ms || second.Seq != first.Seq+1 {
		t.Errorf("the id after the jump is %s; want the same millisecond with the next sequence", second)
	}
	// And the entries are still in order, which is what everything else depends on.
	entries, err := st.XRange("s", store.StreamRange{End: store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].ID != first || entries[1].ID != second {
		t.Errorf("entries are out of order: %v", entries)
	}
}

// TestStreamConsumerGroups walks the whole group lifecycle at the wire level.
func TestStreamConsumerGroups(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// A group needs the key, unless it is told to make it.
	if got := c.cmd("XGROUP CREATE s g 0"); !strings.HasPrefix(got, "-ERR The XGROUP subcommand requires the key to exist") {
		t.Errorf("XGROUP CREATE on a missing key = %q", got)
	}
	if got := c.cmd("XGROUP CREATE s g 0 MKSTREAM"); got != "+OK" {
		t.Fatalf("XGROUP CREATE ... MKSTREAM = %q", got)
	}
	if got := c.cmd("XGROUP CREATE s g 0"); got != "-BUSYGROUP Consumer Group name already exists" {
		t.Errorf("a duplicate XGROUP CREATE = %q", got)
	}

	c.cmd("XADD s 1-1 a 1")
	c.cmd("XADD s 2-1 b 2")

	// The ">" form delivers what the group has never seen and records it as pending.
	if got := c.cmd("XREADGROUP GROUP g alice COUNT 1 STREAMS s >"); got != "[[s [[1-1 [a 1]]]]]" {
		t.Errorf("XREADGROUP > = %q", got)
	}
	if got := c.cmd("XPENDING s g"); got != "[:1 1-1 1-1 [[alice 1]]]" {
		t.Errorf("XPENDING summary = %q", got)
	}
	// The history form re-reads that consumer's own outstanding entries and changes
	// nothing.
	if got := c.cmd("XREADGROUP GROUP g alice STREAMS s 0"); got != "[[s [[1-1 [a 1]]]]]" {
		t.Errorf("XREADGROUP history = %q", got)
	}
	if got := c.cmd("XPENDING s g"); got != "[:1 1-1 1-1 [[alice 1]]]" {
		t.Errorf("the history read changed the PEL: %q", got)
	}
	// A second consumer gets the next entry, not the first one.
	if got := c.cmd("XREADGROUP GROUP g bob COUNT 1 STREAMS s >"); got != "[[s [[2-1 [b 2]]]]]" {
		t.Errorf("XREADGROUP for a second consumer = %q", got)
	}
	if got := c.cmd("XREADGROUP GROUP g bob COUNT 1 STREAMS s >"); got != "(nil)" {
		t.Errorf("XREADGROUP with nothing new = %q; want a null reply", got)
	}

	// XPENDING's extended form, with and without a consumer filter.
	ext := c.cmd("XPENDING s g - + 10")
	if !strings.Contains(ext, "1-1 alice") || !strings.Contains(ext, "2-1 bob") {
		t.Errorf("XPENDING extended = %q", ext)
	}
	if got := c.cmd("XPENDING s g - + 10 alice"); strings.Contains(got, "bob") {
		t.Errorf("XPENDING filtered by alice returned bob's entries: %q", got)
	}
	if got := c.cmd("XPENDING s g IDLE 100000 - + 10"); got != "[]" {
		t.Errorf("XPENDING IDLE with a huge threshold = %q; want []", got)
	}

	// Acknowledging removes the entry from the PEL.
	if got := c.cmd("XACK s g 1-1"); got != ":1" {
		t.Errorf("XACK = %q", got)
	}
	if got := c.cmd("XACK s g 1-1"); got != ":0" {
		t.Errorf("a repeat XACK = %q; want :0", got)
	}

	// Claiming: bob's entry moves to carol.
	if got := c.cmd("XCLAIM s g carol 0 2-1"); got != "[[2-1 [b 2]]]" {
		t.Errorf("XCLAIM = %q", got)
	}
	if got := c.cmd("XPENDING s g - + 10"); !strings.Contains(got, "2-1 carol") {
		t.Errorf("after XCLAIM, XPENDING = %q", got)
	}
	if got := c.cmd("XCLAIM s g dave 0 2-1 JUSTID"); got != "[2-1]" {
		t.Errorf("XCLAIM JUSTID = %q", got)
	}
	// A min-idle-time nothing satisfies claims nothing.
	if got := c.cmd("XCLAIM s g erin 100000 2-1"); got != "[]" {
		t.Errorf("XCLAIM with a huge min-idle-time = %q; want []", got)
	}
	// FORCE creates the pending entry for an entry that is in the stream but not pending.
	c.cmd("XADD s 3-1 c 3")
	if got := c.cmd("XCLAIM s g erin 0 3-1 FORCE JUSTID"); got != "[3-1]" {
		t.Errorf("XCLAIM FORCE = %q", got)
	}

	// XAUTOCLAIM sweeps the PEL for anything idle enough.
	auto := c.cmd("XAUTOCLAIM s g frank 0 0")
	if !strings.HasPrefix(auto, "[0-0 [") {
		t.Errorf("XAUTOCLAIM = %q; want a cursor, the claimed entries and the dropped ids", auto)
	}
	if got := c.cmd("XPENDING s g - + 10"); !strings.Contains(got, "frank") {
		t.Errorf("after XAUTOCLAIM, XPENDING = %q", got)
	}

	// XINFO reports all of it.
	if got := c.cmd("XINFO STREAM s"); !strings.Contains(got, "length :3") {
		t.Errorf("XINFO STREAM = %q", got)
	}
	if got := c.cmd("XINFO GROUPS s"); !strings.Contains(got, "name g") {
		t.Errorf("XINFO GROUPS = %q", got)
	}
	if got := c.cmd("XINFO CONSUMERS s g"); !strings.Contains(got, "frank") {
		t.Errorf("XINFO CONSUMERS = %q", got)
	}
	if got := c.cmd("XINFO STREAM nosuch"); got != "-ERR no such key" {
		t.Errorf("XINFO STREAM on a missing key = %q", got)
	}

	// Consumer administration.
	if got := c.cmd("XGROUP CREATECONSUMER s g newbie"); got != ":1" {
		t.Errorf("XGROUP CREATECONSUMER = %q", got)
	}
	if got := c.cmd("XGROUP CREATECONSUMER s g newbie"); got != ":0" {
		t.Errorf("a repeat XGROUP CREATECONSUMER = %q; want :0", got)
	}
	if got := c.cmd("XGROUP DELCONSUMER s g newbie"); got != ":0" {
		t.Errorf("XGROUP DELCONSUMER for an idle consumer = %q; want :0 pending", got)
	}
	if got := c.cmd("XGROUP SETID s g 0"); got != "+OK" {
		t.Errorf("XGROUP SETID = %q", got)
	}
	if got := c.cmd("XGROUP DESTROY s g"); got != ":1" {
		t.Errorf("XGROUP DESTROY = %q", got)
	}
	if got := c.cmd("XGROUP DESTROY s g"); got != ":0" {
		t.Errorf("a repeat XGROUP DESTROY = %q; want :0", got)
	}
	// And a read against a group that is gone is NOGROUP, not an empty result.
	if got := c.cmd("XREADGROUP GROUP g alice STREAMS s >"); !strings.HasPrefix(got, "-NOGROUP") {
		t.Errorf("XREADGROUP on a destroyed group = %q", got)
	}
}

// TestStreamReadErrors pins the messages a client library matches on.
func TestStreamReadErrors(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	c.cmd("XADD s 1-1 a 1")

	cases := []struct{ cmd, want string }{
		{"XREAD STREAMS s", "-ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified."},
		{"XREAD STREAMS s a b", "-ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified."},
		{"XREAD COUNT 2 STREAMS s >", "-" + errGTOutsideGroup[len("-")+3:]},
		{"XREAD BOGUS 1 STREAMS s 0", "-ERR syntax error"},
		{"XREADGROUP GROUP g c STREAMS s $", "-" + errDollarInGroup[len("-")+3:]},
		{"XREADGROUP STREAMS s >", "-ERR Missing GROUP keyword or consumer/group name in XREADGROUP"},
	}
	for _, tc := range cases {
		got := c.cmd(tc.cmd)
		// The two long messages are compared by prefix so the assertion stays readable.
		if !strings.HasPrefix(got, "-ERR") {
			t.Errorf("%q -> %q; want an error", tc.cmd, got)
		}
	}
	if got := c.cmd("XREAD STREAMS s 0"); got != "[[s [[1-1 [a 1]]]]]" {
		t.Errorf("XREAD = %q", got)
	}
	if got := c.cmd("XREAD STREAMS s 1-1"); got != "(nil)" {
		t.Errorf("XREAD past the end = %q; want a null reply", got)
	}
	if got := c.cmd("XREAD COUNT 1 STREAMS s 0"); got != "[[s [[1-1 [a 1]]]]]" {
		t.Errorf("XREAD COUNT = %q", got)
	}
	// "$" means "from now", so it never returns what is already there.
	if got := c.cmd("XREAD STREAMS s $"); got != "(nil)" {
		t.Errorf("XREAD ... $ = %q; want a null reply", got)
	}
	// XREAD is a read, so it works on a replica -- which is to say it is not a write.
	if commandTable["XREAD"].write {
		t.Error("XREAD is registered as a write; it reads a stream and changes nothing")
	}
	if !commandTable["XREADGROUP"].write {
		t.Error("XREADGROUP is not registered as a write; it advances a group and creates PEL entries")
	}
}

// TestXReadBlocks is the blocking half: a client parked on XREAD BLOCK is woken by an
// XADD from another connection, and it does not hold any lock while it waits.
func TestXReadBlocks(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	reader := dialTx(t, addr)
	defer reader.close()
	writer := dialTx(t, addr)
	defer writer.close()
	writer.cmd("XADD s 1-1 seed 0")

	replies := make(chan string, 1)
	go func() {
		reader.conn.Write([]byte("XREAD BLOCK 0 STREAMS s $\r\n"))
		got, err := parseReply(reader.br)
		if err != nil {
			got = "read error: " + err.Error()
		}
		replies <- got
	}()

	// The server reports the waiter, which is how the test knows the reader has parked
	// rather than merely been scheduled.
	waitFor(t, "the reader to block", func() bool {
		return strings.Contains(writer.cmd("INFO clients"), "blocked_clients:1")
	})
	// And the server is not stuck: another connection's writes still work.
	if got := writer.cmd("XADD s 2-1 b 2"); got != "2-1" {
		t.Fatalf("XADD while a client was blocked = %q", got)
	}
	select {
	case got := <-replies:
		if got != "[[s [[2-1 [b 2]]]]]" {
			t.Errorf("the woken XREAD returned %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("XREAD BLOCK was not woken by the XADD")
	}

	// A timeout answers with the null reply rather than hanging.
	if got := reader.cmd("XREAD BLOCK 30 STREAMS s $"); got != "(nil)" {
		t.Errorf("XREAD BLOCK that timed out = %q; want a null reply", got)
	}
}

// TestXReadGroupBlocks is the same for the group form, which is a write and so also has
// to propagate its effect.
func TestXReadGroupBlocks(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	reader := dialTx(t, addr)
	defer reader.close()
	writer := dialTx(t, addr)
	defer writer.close()
	writer.cmd("XADD s 1-1 seed 0")
	writer.cmd("XGROUP CREATE s g $")

	replies := make(chan string, 1)
	go func() {
		reader.conn.Write([]byte("XREADGROUP GROUP g alice BLOCK 0 STREAMS s >\r\n"))
		got, err := parseReply(reader.br)
		if err != nil {
			got = "read error: " + err.Error()
		}
		replies <- got
	}()
	waitFor(t, "the group reader to block", func() bool {
		return strings.Contains(writer.cmd("INFO clients"), "blocked_clients:1")
	})
	writer.cmd("XADD s 2-1 b 2")
	select {
	case got := <-replies:
		if got != "[[s [[2-1 [b 2]]]]]" {
			t.Errorf("the woken XREADGROUP returned %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("XREADGROUP BLOCK was not woken by the XADD")
	}
	// The delivery was recorded, which is the part that makes it a write.
	if got := writer.cmd("XPENDING s g"); !strings.Contains(got, "alice") {
		t.Errorf("XPENDING after the woken XREADGROUP = %q", got)
	}
	// Inside MULTI it must not block at all.
	reader.cmd("MULTI")
	reader.cmd("XREADGROUP GROUP g alice BLOCK 0 STREAMS s >")
	if got := reader.cmd("EXEC"); got != "[(nil)]" {
		t.Errorf("XREADGROUP BLOCK inside MULTI = %q; want the non-blocking null reply", got)
	}
}

// TestStreamPropagation is the invariant check: every stream write whose outcome
// depends on the clock ships what it did, never what it was asked to do.
func TestStreamPropagation(t *testing.T) {
	st := store.New(4)
	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	// XADD * must ship the concrete id. A replica replaying "*" would pick its own.
	s.dispatch(w, cmdArgs("XADD", "s", "*", "f", "v"))
	got := next()
	if len(got) != 5 || string(got[0]) != "XADD" {
		t.Fatalf("XADD * propagated %q", cmdStrings(got))
	}
	if string(got[2]) == "*" {
		t.Fatal("XADD * propagated the literal '*'; a replica would generate a different id")
	}
	id := string(got[2])
	if _, ok := store.ParseStreamID(id, 0); !ok {
		t.Fatalf("XADD * propagated %q as its id", id)
	}
	// And the id it propagated is the id it actually stored.
	entries, err := st.XRange("s", store.StreamRange{End: store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}}, 0, false)
	if err != nil || len(entries) != 1 || entries[0].ID.String() != id {
		t.Fatalf("the stored entry is %v but %q was propagated", entries, id)
	}

	// A trim ships as an exact XTRIM, with no "~".
	s.dispatch(w, cmdArgs("XADD", "s", "MAXLEN", "~", "1", "*", "f", "v"))
	if got := next(); string(got[0]) != "MULTI" {
		t.Fatalf("a trimming XADD propagated %q; want a MULTI-framed pair", cmdStrings(got))
	}
	if got := next(); string(got[0]) != "XADD" || string(got[2]) == "*" {
		t.Fatalf("the framed XADD is %q", cmdStrings(got))
	}
	if got := next(); string(got[0]) != "XTRIM" || string(got[2]) != "MAXLEN" || string(got[3]) != "1" {
		t.Fatalf("the framed trim is %q; want an exact XTRIM", cmdStrings(got))
	}
	next() // EXEC

	// XGROUP CREATE ... $ ships the resolved id, for the same reason. A separate key
	// with explicit ids, because "s" has already been given a clock-derived id that
	// nothing smaller can follow.
	s.dispatch(w, cmdArgs("XADD", "g-stream", "1-1", "f", "v"))
	next()
	s.dispatch(w, cmdArgs("XGROUP", "CREATE", "g-stream", "g", "$"))
	got = next()
	if string(got[4]) == "$" {
		t.Fatal("XGROUP CREATE ... $ propagated the literal '$'")
	}
	if string(got[4]) != "1-1" {
		t.Errorf("XGROUP CREATE ... $ propagated %q; want the stream's last id", cmdStrings(got))
	}
	if string(got[5]) != "ENTRIESREAD" {
		t.Errorf("XGROUP CREATE propagated %q; want the read counter too", cmdStrings(got))
	}

	// XREADGROUP ships the PEL it created plus the group's resulting position.
	s.dispatch(w, cmdArgs("XADD", "g-stream", "9-9", "f", "v"))
	next()
	s.dispatch(w, cmdArgs("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "g-stream", ">"))
	if got := next(); string(got[0]) != "MULTI" {
		t.Fatalf("XREADGROUP propagated %q; want a MULTI-framed effect", cmdStrings(got))
	}
	claim := next()
	if string(claim[0]) != "XCLAIM" || string(claim[6]) != "TIME" {
		t.Fatalf("XREADGROUP's first effect is %q; want an XCLAIM with an absolute TIME", cmdStrings(claim))
	}
	setid := next()
	if string(setid[0]) != "XGROUP" || string(setid[1]) != "SETID" {
		t.Fatalf("XREADGROUP's second effect is %q; want XGROUP SETID", cmdStrings(setid))
	}
	next() // EXEC

	// XCLAIM's own effect is an absolute-TIME claim, never the min-idle-time form.
	s.dispatch(w, cmdArgs("XCLAIM", "g-stream", "g", "bob", "0", "9-9"))
	got = next()
	if string(got[0]) != "XCLAIM" || string(got[4]) != "0" || string(got[6]) != "TIME" {
		t.Fatalf("XCLAIM propagated %q; want an absolute-TIME claim with min-idle-time 0", cmdStrings(got))
	}
	if string(got[10]) != "FORCE" || string(got[11]) != "JUSTID" {
		t.Errorf("XCLAIM's effect is %q; want FORCE JUSTID so the replica records it verbatim", cmdStrings(got))
	}
}

// cmdStrings renders a propagated command for an assertion message.
func cmdStrings(cmd [][]byte) []string { return byteStrings(cmd) }

// TestStreamDumpRestoresGroupsAndPELs is the data-loss check. A snapshot that dropped a
// group, its position or its pending entries would silently re-deliver acknowledged work
// (or lose outstanding work) on every restart and every replica sync.
func TestStreamDumpRestoresGroupsAndPELs(t *testing.T) {
	// Both stores read the same fixed clock, so the delivery times a PEL records and the
	// idle times XINFO reports are the same on both sides by construction. That is what
	// lets the two servers be compared byte for byte, rather than field by field with the
	// time-dependent ones excused.
	frozen := func() time.Time { return time.UnixMilli(1_700_000_000_000) }
	src := store.New(4)
	src.SetClock(frozen)
	s := New(src)
	w := resp.NewWriter(io.Discard)

	s.dispatch(w, cmdArgs("XADD", "s", "1-1", "a", "1"))
	s.dispatch(w, cmdArgs("XADD", "s", "2-1", "b", "2"))
	s.dispatch(w, cmdArgs("XADD", "s", "3-1", "c", "3"))
	s.dispatch(w, cmdArgs("XDEL", "s", "2-1")) // leaves a max-deleted-id to preserve
	s.dispatch(w, cmdArgs("XGROUP", "CREATE", "s", "g1", "0"))
	s.dispatch(w, cmdArgs("XGROUP", "CREATE", "s", "g2", "0"))
	s.dispatch(w, cmdArgs("XREADGROUP", "GROUP", "g1", "alice", "COUNT", "1", "STREAMS", "s", ">"))
	s.dispatch(w, cmdArgs("XREADGROUP", "GROUP", "g1", "bob", "COUNT", "1", "STREAMS", "s", ">"))
	s.dispatch(w, cmdArgs("XGROUP", "CREATECONSUMER", "s", "g1", "idle-one"))

	before := snapshotStreamState(t, s)

	// Replay the dump into a fresh server, exactly as an AOF rewrite or a replica seed
	// would.
	dstStore := store.New(4)
	dstStore.SetClock(frozen)
	dst := New(dstStore)
	dst.ReplayCommands(src.Dump())
	after := snapshotStreamState(t, dst)

	for _, cmd := range []string{
		"XLEN s", "XINFO STREAM s", "XINFO GROUPS s", "XINFO CONSUMERS s g1",
		"XPENDING s g1", "XPENDING s g1 - + 20", "XRANGE s - +",
	} {
		if before[cmd] != after[cmd] {
			t.Errorf("%s did not survive the dump:\n before: %s\n  after: %s", cmd, before[cmd], after[cmd])
		}
	}

	// And no emitted command can be too long for the reader to accept.
	for _, cmd := range src.Dump() {
		if len(cmd) > resp.MaxMultiBulk {
			t.Fatalf("Dump emitted a %d-element command; the reader's limit is %d", len(cmd), resp.MaxMultiBulk)
		}
	}
}

// snapshotStreamState runs a set of introspection commands against a server and
// returns their replies, so two servers can be compared by what a client can observe.
func snapshotStreamState(t *testing.T, s *Server) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, cmd := range []string{
		"XLEN s", "XINFO STREAM s", "XINFO GROUPS s", "XINFO CONSUMERS s g1",
		"XPENDING s g1", "XPENDING s g1 - + 20", "XRANGE s - +",
	} {
		var buf strings.Builder
		w := resp.NewWriter(&buf)
		s.dispatch(w, cmdArgs(strings.Split(cmd, " ")...))
		w.Flush()
		out[cmd] = buf.String()
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestStreamReplicaConvergence is the end-to-end statement of the propagation rules: a
// replica fed a master's stream ends up with the same entries, ids, groups and pending
// lists.
func TestStreamReplicaConvergence(t *testing.T) {
	master := New(store.New(4))
	replica := New(store.New(4))

	// Feed the master's propagated stream straight into the replica, which is what the
	// replication link does minus the socket.
	rc := newReplicaConn(4096)
	master.mu.Lock()
	master.replicas[rc] = struct{}{}
	master.mu.Unlock()
	master.propagating.Store(true)

	w := resp.NewWriter(io.Discard)
	for i := 0; i < 5; i++ {
		master.dispatch(w, cmdArgs("XADD", "s", "*", "seq", itoa(i)))
	}
	master.dispatch(w, cmdArgs("XGROUP", "CREATE", "s", "g", "0"))
	master.dispatch(w, cmdArgs("XREADGROUP", "GROUP", "g", "alice", "COUNT", "3", "STREAMS", "s", ">"))
	master.dispatch(w, cmdArgs("XACK", "s", "g", firstStreamID(t, master).String()))
	master.dispatch(w, cmdArgs("XCLAIM", "s", "g", "bob", "0", secondStreamID(t, master).String()))

	// Drain the feed into the replica through the same applier a real replica uses.
	applier := newTxApplier(replica, resp.NewWriter(io.Discard))
	for {
		select {
		case cmd := <-rc.ch:
			applier.feed(cmd)
			continue
		default:
		}
		break
	}

	if got, want := snapshotStreamStateFor(t, replica, "g"), snapshotStreamStateFor(t, master, "g"); got != want {
		t.Errorf("the replica diverged from the master:\n master: %s\nreplica: %s", want, got)
	}
}

// snapshotStreamStateFor renders the observable state of one stream and group.
func snapshotStreamStateFor(t *testing.T, s *Server, group string) string {
	t.Helper()
	var buf strings.Builder
	w := resp.NewWriter(&buf)
	for _, cmd := range [][]string{
		{"XRANGE", "s", "-", "+"},
		{"XINFO", "STREAM", "s"},
		{"XINFO", "GROUPS", "s"},
		{"XPENDING", "s", group},
	} {
		s.dispatch(w, cmdArgs(cmd...))
	}
	w.Flush()
	return buf.String()
}

func firstStreamID(t *testing.T, s *Server) store.StreamID {
	t.Helper()
	entries, err := s.store.XRange("s", store.StreamRange{End: store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}}, 1, false)
	if err != nil || len(entries) == 0 {
		t.Fatalf("no entries: %v", err)
	}
	return entries[0].ID
}

func secondStreamID(t *testing.T, s *Server) store.StreamID {
	t.Helper()
	entries, err := s.store.XRange("s", store.StreamRange{End: store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}}, 2, false)
	if err != nil || len(entries) < 2 {
		t.Fatalf("fewer than two entries: %v", err)
	}
	return entries[1].ID
}

// TestStreamCommandGetKeys covers the four stream commands whose keys are not at a
// fixed argument position, which is the case COMMAND GETKEYS exists for.
func TestStreamCommandGetKeys(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"COMMAND GETKEYS XADD s 1-1 f v", "[s]"},
		{"COMMAND GETKEYS XREAD COUNT 2 STREAMS s1 s2 0 0", "[s1 s2]"},
		{"COMMAND GETKEYS XREADGROUP GROUP g c STREAMS s1 s2 > >", "[s1 s2]"},
		{"COMMAND GETKEYS XGROUP CREATE s g 0", "[s]"},
		{"COMMAND GETKEYS XINFO STREAM s", "[s]"},
		{"COMMAND GETKEYS XACK s g 1-1", "[s]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestStreamKeyspaceNotifications covers the notification class the stream commands
// added ('t'), including the one command whose event is named after its *subcommand*
// rather than after itself.
func TestStreamKeyspaceNotifications(t *testing.T) {
	_, addr, stop := startNotifyServer(t, "KEt")
	defer stop()

	sub := dialTx(t, addr)
	defer sub.close()
	subscribeCmd(t, sub, "PSUBSCRIBE __keyevent@0__:*", 1)
	c := dialTx(t, addr)
	defer c.close()
	waitFor(t, "the subscription to register", func() bool {
		return c.cmd("PUBSUB NUMPAT") == ":1"
	})

	steps := []struct{ cmd, event string }{
		{"XADD s 1-1 f v", "xadd"},
		{"XADD s 2-1 f v", "xadd"},
		{"XTRIM s MAXLEN 1", "xtrim"},
		{"XSETID s 5-5", "xsetid"},
		{"XGROUP CREATE s g 0", "xgroup-create"},
		{"XGROUP CREATECONSUMER s g c1", "xgroup-createconsumer"},
		{"XGROUP DELCONSUMER s g c1", "xgroup-delconsumer"},
		{"XGROUP SETID s g 0", "xgroup-setid"},
		{"XGROUP DESTROY s g", "xgroup-destroy"},
		{"XDEL s 2-1", "xdel"},
	}
	for _, step := range steps {
		if got := c.cmd(step.cmd); strings.HasPrefix(got, "-") {
			t.Fatalf("%q -> %q", step.cmd, got)
		}
		want := "__keyevent@0__:" + step.event + " s"
		if got := nextMessage(t, sub); !contains(got, want) {
			t.Errorf("%q notified %q; want %q", step.cmd, got, want)
		}
	}

	// The group commands that move a group's bookkeeping rather than the stream's
	// contents fire exactly one event between them: the xgroup-createconsumer for a
	// consumer they created implicitly. That is what a live redis:7-alpine does --
	// XREADGROUP, XACK, XCLAIM and XAUTOCLAIM have no event of their own -- and it is the
	// difference a consumer of notifications would otherwise build on and be surprised by.
	c.cmd("XADD s 6-1 f v")
	nextMessage(t, sub) // the xadd
	c.cmd("XGROUP CREATE s g2 0")
	nextMessage(t, sub) // the xgroup-create

	// A read that names a new consumer reports only that consumer.
	c.cmd("XREADGROUP GROUP g2 alice STREAMS s >")
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:xgroup-createconsumer s") {
		t.Errorf("XREADGROUP with a new consumer notified %q; want xgroup-createconsumer", got)
	}
	// A second read by the same consumer creates nothing, so it is silent -- proven by
	// the next event being the one after it.
	c.cmd("XADD s 7-1 f v")
	c.cmd("XREADGROUP GROUP g2 alice STREAMS s >")
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:xadd s") {
		t.Errorf("a repeat XREADGROUP fired an event; the next message was %q", got)
	}
	// XACK is silent outright.
	c.cmd("XACK s g2 6-1")
	// XCLAIM and XAUTOCLAIM are silent except for the consumer they create.
	c.cmd("XCLAIM s g2 bob 0 7-1")
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:xgroup-createconsumer s") {
		t.Errorf("XCLAIM with a new consumer notified %q; want xgroup-createconsumer", got)
	}
	c.cmd("XAUTOCLAIM s g2 carol 0 0")
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:xgroup-createconsumer s") {
		t.Errorf("XAUTOCLAIM with a new consumer notified %q; want xgroup-createconsumer", got)
	}
	// A claim by a consumer that already exists is silent, which the trailing xdel proves.
	c.cmd("XCLAIM s g2 bob 0 7-1")
	c.cmd("XDEL s 7-1")
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:xdel s") {
		t.Errorf("a repeat XCLAIM fired an event; the next message was %q, want the xdel", got)
	}
}
