package server

import (
	"sort"
	"strings"
	"testing"
)

// replyMembers parses an array reply into its elements, sorted, so a reply whose order
// comes from map iteration can be compared for contents *and* counted. arrayFields joins
// with commas, which makes it unusable for counting -- splitting its result on spaces
// yields one field for any non-empty reply.
func replyMembers(reply string) []string {
	inner := reply
	if len(inner) >= 2 && inner[0] == '[' {
		inner = inner[1 : len(inner)-1]
	}
	f := splitSpace(inner)
	sort.Strings(f)
	return f
}

// TestSetRandomCommands covers SPOP and SRANDMEMBER: the same random draw, one
// destructive and one not, with reply shapes that differ between the bare and the
// counted form.
func TestSetRandomCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// A bare SPOP replies with the member; a missing key is a null.
		{"SADD one only", ":1"},
		{"SPOP one", "only"},
		{"EXISTS one", ":0"},
		{"SPOP missing", "(nil)"},
		// With a count the reply is an array -- empty, not null, for a missing key.
		{"SPOP missing 3", "[]"},
		{"SADD s a b c", ":3"},
		{"SPOP s 0", "[]"},
		{"SCARD s", ":3"},
		{"SPOP s -1", "-ERR value is out of range, must be positive"},
		{"SPOP s abc", "-ERR value is not an integer or out of range"},
		{"SPOP s 1 2", "-ERR wrong number of arguments for 'spop' command"},
		// SRANDMEMBER never removes anything.
		{"SADD r only", ":1"},
		{"SRANDMEMBER r", "only"},
		{"SRANDMEMBER r 3", "[only]"},
		{"SRANDMEMBER r -3", "[only only only]"},
		{"SRANDMEMBER r 0", "[]"},
		{"SCARD r", ":1"},
		{"SRANDMEMBER missing", "(nil)"},
		{"SRANDMEMBER missing 3", "[]"},
		{"SRANDMEMBER r abc", "-ERR value is not an integer or out of range"},
		{"SRANDMEMBER r 1 2", "-ERR wrong number of arguments for 'srandmember' command"},
		// SMISMEMBER answers one 0/1 per member.
		{"SMISMEMBER s a zzz b", "[:1 :0 :1]"},
		{"SMISMEMBER missing a", "[:0]"},
		{"SMISMEMBER s", "-ERR wrong number of arguments for 'smismember' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// SPOP with a count removes exactly that many, and takes the key with the last.
	//
	// The members come back in map order, so the reply is compared as a set -- but it is
	// compared for its *contents*, which is what the two assertions here used to miss.
	// The first counted `splitSpace` over an arrayFields result, and arrayFields joins with
	// commas: the count was 1 for every non-empty reply, so "returned nothing" was the only
	// reachable failure. The second asked for len(reply) >= 3, which "-WRONGTYPE ..." also
	// satisfies. Between them the two SPOPs must yield exactly the three members added.
	first := replyMembers(c.cmd("SPOP s 2"))
	if len(first) != 2 {
		t.Errorf("SPOP s 2 returned %d members (%v); want 2", len(first), first)
	}
	if got := c.cmd("SCARD s"); got != ":1" {
		t.Errorf("SCARD after SPOP 2 = %q; want :1", got)
	}
	rest := replyMembers(c.cmd("SPOP s 10"))
	if len(rest) != 1 {
		t.Errorf("SPOP s 10 over a one-member set returned %d members (%v); want 1", len(rest), rest)
	}
	drawn := append(append([]string{}, first...), rest...)
	sort.Strings(drawn)
	if got := strings.Join(drawn, ","); got != "a,b,c" {
		t.Errorf("the two SPOPs drew %q; want exactly a, b and c once each", got)
	}
	if got := c.cmd("EXISTS s"); got != ":0" {
		t.Errorf("emptied set still exists: %q", got)
	}

	c.cmd("SET str v")
	for _, cmd := range []string{"SPOP str", "SRANDMEMBER str", "SMISMEMBER str a"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestSMove covers the cross-key member move, including the source it deletes and
// the destination it creates.
func TestSMove(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SADD src a b", ":2"},
		{"SADD dst c", ":1"},
		{"SMOVE src dst a", ":1"},
		{"SMOVE src dst zzz", ":0"},
		{"SMOVE missing dst a", ":0"},
		// Moving a member onto its own set is a no-op that still succeeds.
		{"SMOVE src src b", ":1"},
		{"SCARD src", ":1"},
		{"SMOVE src", "-ERR wrong number of arguments for 'smove' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
	if got := arrayFields(c.cmd("SMEMBERS dst")); got != "a,c" {
		t.Errorf("destination after SMOVE = %q; want a,c", got)
	}
	if got := arrayFields(c.cmd("SMEMBERS src")); got != "b" {
		t.Errorf("source after SMOVE = %q; want b", got)
	}

	// The move creates a missing destination and deletes a drained source.
	c.cmd("SADD lonely x")
	if got := c.cmd("SMOVE lonely fresh x"); got != ":1" {
		t.Fatalf("SMOVE to a new set = %q; want :1", got)
	}
	if got := c.cmd("EXISTS lonely"); got != ":0" {
		t.Errorf("drained source still exists: %q", got)
	}
	if got := c.cmd("SMEMBERS fresh"); got != "[x]" {
		t.Errorf("created destination = %q; want [x]", got)
	}

	// A wrong-typed source or destination is an error and moves nothing.
	c.cmd("SET str v")
	for _, cmd := range []string{"SMOVE str dst a", "SMOVE src str b"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q = %q; want %q", cmd, got, wrongType)
		}
	}
	if got := arrayFields(c.cmd("SMEMBERS src")); got != "b" {
		t.Errorf("source after a rejected SMOVE = %q; want b", got)
	}
}

// TestSetAlgebra covers SINTER/SUNION/SDIFF, their *STORE variants, and SINTERCARD.
// A missing key behaves as the empty set in each of them.
func TestSetAlgebra(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SADD k1 a b c d")
	c.cmd("SADD k2 b c")
	c.cmd("SADD k3 c d e")

	reads := []struct{ cmd, want string }{
		{"SINTER k1 k2", "b,c"},
		{"SINTER k1 k2 k3", "c"},
		{"SINTER k1", "a,b,c,d"},
		{"SINTER k1 missing", ""},
		{"SINTER missing k1", ""},
		{"SUNION k1 k3", "a,b,c,d,e"},
		{"SUNION k2 missing", "b,c"},
		{"SUNION missing other", ""},
		{"SDIFF k1 k2", "a,d"},
		{"SDIFF k1 k2 k3", "a"},
		{"SDIFF missing k1", ""},
		{"SDIFF k2 missing", "b,c"},
	}
	for _, tc := range reads {
		if got := arrayFields(c.cmd(tc.cmd)); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	cases := []struct{ cmd, want string }{
		{"SINTERSTORE d1 k1 k2", ":2"},
		{"SUNIONSTORE d2 k1 k3", ":5"},
		{"SDIFFSTORE d3 k1 k2", ":2"},
		// An empty result deletes the destination rather than storing an empty set.
		{"SINTERSTORE d1 k1 missing", ":0"},
		{"EXISTS d1", ":0"},
		// The destination may be one of the sources.
		{"SINTERSTORE k3 k1 k3", ":2"},
		{"SCARD k3", ":2"},
		{"SINTERSTORE d1", "-ERR wrong number of arguments for 'sinterstore' command"},
		// SINTERCARD counts without materialising, and LIMIT stops it early.
		{"SINTERCARD 2 k1 k2", ":2"},
		{"SINTERCARD 2 k1 k2 LIMIT 1", ":1"},
		{"SINTERCARD 2 k1 k2 LIMIT 0", ":2"}, // 0 means unlimited
		{"SINTERCARD 1 k1", ":4"},
		{"SINTERCARD 0 k1", "-ERR numkeys should be greater than 0"},
		{"SINTERCARD abc k1", "-ERR numkeys should be greater than 0"},
		{"SINTERCARD 3 k1 k2", "-ERR Number of keys can't be greater than number of args"},
		{"SINTERCARD 2 k1 k2 LIMIT -1", "-ERR LIMIT can't be negative"},
		{"SINTERCARD 2 k1 k2 BOGUS 1", "-ERR syntax error"},
		{"SINTERCARD 2 k1 k2 LIMIT", "-ERR syntax error"},
		{"SINTERCARD 1", "-ERR wrong number of arguments for 'sintercard' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
	if got := arrayFields(c.cmd("SMEMBERS d2")); got != "a,b,c,d,e" {
		t.Errorf("SUNIONSTORE destination = %q; want a,b,c,d,e", got)
	}
	if got := arrayFields(c.cmd("SMEMBERS d3")); got != "a,d" {
		t.Errorf("SDIFFSTORE destination = %q; want a,d", got)
	}

	c.cmd("SET str v")
	for _, cmd := range []string{
		"SINTER k1 str", "SUNION k1 str", "SDIFF k1 str",
		"SINTERSTORE d k1 str", "SINTERCARD 2 k1 str",
	} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q with a string operand = %q; want %q", cmd, got, wrongType)
		}
	}
}
