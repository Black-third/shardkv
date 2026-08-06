package server

import (
	"strconv"
	"strings"
	"testing"
)

// TestHyperLogLogCommands covers the command surface at the wire level. The statistical
// behaviour is tested in the store (see hyperloglog_test.go); what matters here is the
// replies, the errors, and the interaction with the string type the sketch is stored in.
func TestHyperLogLogCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// PFADD reports whether the sketch changed.
		{"PFADD hll a b c", ":1"},
		{"PFADD hll a b c", ":0"},
		{"PFADD hll d", ":1"},
		{"PFCOUNT hll", ":4"},
		// PFADD with no elements creates an empty sketch, and only the creation counts.
		{"PFADD empty", ":1"},
		{"PFADD empty", ":0"},
		{"PFCOUNT empty", ":0"},
		{"PFCOUNT missing", ":0"},

		// A sketch is a string, which is exactly what makes it portable.
		{"TYPE hll", "+string"},
		{"PFDEBUG ENCODING hll", "+sparse"},

		// The union, both ways.
		{"PFADD other d e f", ":1"},
		{"PFCOUNT hll other", ":6"},
		{"PFMERGE dest hll other", "+OK"},
		{"PFCOUNT dest", ":6"},
		// Merging again changes nothing, so it propagates nothing.
		{"PFMERGE dest hll other", "+OK"},
		{"PFCOUNT dest", ":6"},
		// The destination joins the union rather than being replaced by it.
		{"PFADD acc x y z", ":1"},
		{"PFMERGE acc hll", "+OK"},
		{"PFCOUNT acc", ":7"},

		// A plain string is not a sketch, and says so distinguishably from a wrong type.
		{"SET plain hello", "+OK"},
		{"PFADD plain x", "-WRONGTYPE Key is not a valid HyperLogLog string value."},
		{"PFCOUNT plain", "-WRONGTYPE Key is not a valid HyperLogLog string value."},
		{"PFMERGE plain hll", "-WRONGTYPE Key is not a valid HyperLogLog string value."},
		{"RPUSH list x", ":1"},
		{"PFADD list x", "-WRONGTYPE Operation against a key holding the wrong kind of value"},

		// PFDEBUG and PFSELFTEST.
		{"PFDEBUG TODENSE hll", ":1"},
		{"PFDEBUG ENCODING hll", "+dense"},
		{"PFDEBUG TODENSE hll", ":0"},
		{"PFCOUNT hll", ":4"}, // the count survives the promotion
		{"PFDEBUG ENCODING nosuch", "-ERR The specified key does not exist"},
		{"PFDEBUG BOGUS hll", "-ERR Unknown PFDEBUG subcommand 'BOGUS'"},
		{"PFSELFTEST", "+OK"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// PFDEBUG GETREG returns all 16384 registers, which is what makes a wrong one
	// findable.
	regs := c.cmd("PFDEBUG GETREG hll")
	if n := strings.Count(regs, " ") + 1; n != 16384 {
		t.Errorf("PFDEBUG GETREG returned %d registers; want 16384", n)
	}
}

// TestHyperLogLogSurvivesADumpAndAnExpiry covers the two things that follow from a
// sketch being a string: a snapshot carries it as a SET, and it can have a TTL.
func TestHyperLogLogSurvivesADumpAndAnExpiry(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	for i := 0; i < 2000; i++ {
		c.cmd("PFADD big v" + strconv.Itoa(i))
	}
	before := c.cmd("PFCOUNT big")

	// A sketch that has been through GET/SET -- which is what Dump emits -- counts the
	// same, because the value is the whole of its state.
	raw := c.cmd("GET big")
	c.cmdRaw("SET", "copy", raw)
	if after := c.cmd("PFCOUNT copy"); after != before {
		t.Errorf("a sketch copied with GET/SET counts %s; the original counts %s", after, before)
	}

	// And a TTL on a sketch behaves like a TTL on any string, including surviving a
	// PFADD.
	c.cmd("EXPIRE big 100")
	c.cmd("PFADD big one-more")
	if ttl := c.cmd("TTL big"); ttl == ":-1" || ttl == ":-2" {
		t.Errorf("PFADD cleared the sketch's TTL (TTL = %s)", ttl)
	}
}

// TestHyperLogLogPropagatesVerbatim is the propagation statement: PFADD and PFMERGE are
// deterministic, so shipping the command is both correct and far cheaper than shipping
// the 12KB value it produced.
func TestHyperLogLogPropagatesVerbatim(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	master := dialTx(t, addr)
	defer master.close()

	// The same additions on two keys give byte-identical values, which is the property a
	// replica relies on when it replays the command instead of the value.
	for i := 0; i < 300; i++ {
		master.cmd("PFADD one e" + strconv.Itoa(i))
		master.cmd("PFADD two e" + strconv.Itoa(i))
	}
	a, b := master.cmd("GET one"), master.cmd("GET two")
	if a != b {
		t.Error("the same PFADD sequence produced two different sketches; verbatim propagation would diverge")
	}
	// And in the opposite order, because the encoding is canonical rather than a history
	// of in-place edits.
	for i := 299; i >= 0; i-- {
		master.cmd("PFADD three e" + strconv.Itoa(i))
	}
	if master.cmd("GET three") != a {
		t.Error("the same additions in a different order produced a different sketch")
	}

	// PFCOUNT must not be a write: it is registered read-only, so a replica serves it.
	if commandTable["PFCOUNT"].write {
		t.Error("PFCOUNT is registered as a write; it must be servable by a replica")
	}
	if !commandTable["PFADD"].write || !commandTable["PFMERGE"].write {
		t.Error("PFADD/PFMERGE must be registered as writes")
	}
}
