package server

import "testing"

// TestListIndexCommands covers the indexed list operations, whose edge cases are
// the negative and out-of-range indexes and the two distinct errors LSET reports.
func TestListIndexCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"RPUSH l a b c d", ":4"},
		{"LINDEX l 0", "a"},
		{"LINDEX l 3", "d"},
		{"LINDEX l -1", "d"},
		{"LINDEX l -4", "a"},
		{"LINDEX l 10", "(nil)"},
		{"LINDEX l -10", "(nil)"},
		{"LINDEX missing 0", "(nil)"},
		{"LINDEX l abc", "-ERR value is not an integer or out of range"},
		{"LINDEX l", "-ERR wrong number of arguments for 'lindex' command"},
		// LSET overwrites in place.
		{"LSET l 1 B", "+OK"},
		{"LSET l -1 D", "+OK"},
		{"LRANGE l 0 -1", "[a B c D]"},
		{"LSET l 10 x", "-ERR index out of range"},
		{"LSET l -10 x", "-ERR index out of range"},
		{"LSET missing 0 x", "-ERR no such key"},
		// LINSERT distinguishes "no list" (0) from "no such pivot" (-1).
		{"LINSERT l BEFORE c X", ":5"},
		{"LRANGE l 0 -1", "[a B X c D]"},
		{"LINSERT l AFTER c Y", ":6"},
		{"LRANGE l 0 -1", "[a B X c Y D]"},
		{"LINSERT l BEFORE nope Z", ":-1"},
		{"LINSERT missing BEFORE a b", ":0"},
		{"LINSERT l SIDEWAYS a b", "-ERR syntax error"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("SET str v")
	for _, cmd := range []string{"LINDEX str 0", "LSET str 0 x", "LINSERT str BEFORE a b"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestListRemoveCommands covers LREM and LTRIM, including the shared rule that a
// list emptied by either one takes its key with it.
func TestListRemoveCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"RPUSH r a b a c a", ":5"},
		{"LREM r 2 a", ":2"},
		{"LRANGE r 0 -1", "[b c a]"},
		{"LREM r -1 a", ":1"},
		{"LRANGE r 0 -1", "[b c]"},
		{"LREM r 0 zzz", ":0"},
		{"LREM missing 0 a", ":0"},
		{"LREM r abc a", "-ERR value is not an integer or out of range"},
		// A count of zero removes every match, which here empties the list.
		{"RPUSH gone a a a", ":3"},
		{"LREM gone 0 a", ":3"},
		{"EXISTS gone", ":0"},
		// LTRIM keeps a range and always replies OK.
		{"RPUSH t a b c d e", ":5"},
		{"LTRIM t 1 3", "+OK"},
		{"LRANGE t 0 -1", "[b c d]"},
		{"LTRIM t -2 -1", "+OK"},
		{"LRANGE t 0 -1", "[c d]"},
		{"LTRIM missing 0 -1", "+OK"},
		{"LTRIM t abc 1", "-ERR value is not an integer or out of range"},
		// A range that selects nothing empties the list, deleting the key.
		{"LTRIM t 5 10", "+OK"},
		{"EXISTS t", ":0"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("SET str v")
	for _, cmd := range []string{"LREM str 0 a", "LTRIM str 0 -1"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestLPos covers LPOS's RANK/COUNT/MAXLEN options and the reply shape that
// changes with COUNT: a bare index (or null) without it, an array with it.
func TestLPos(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	const rankZeroErr = "-ERR RANK can't be zero: use 1 to start from the first match, " +
		"2 from the second ... or use negative to start from the end of the list"

	cases := []struct{ cmd, want string }{
		{"RPUSH p a b c 1 2 3 c c", ":8"},
		{"LPOS p a", ":0"},
		{"LPOS p c", ":2"},
		{"LPOS p c RANK 2", ":6"},
		{"LPOS p c RANK -1", ":7"},
		{"LPOS p c RANK -2", ":6"},
		{"LPOS p c COUNT 2", "[:2 :6]"},
		{"LPOS p c COUNT 0", "[:2 :6 :7]"},
		{"LPOS p c RANK -1 COUNT 0", "[:7 :6 :2]"},
		{"LPOS p nope", "(nil)"},
		{"LPOS p nope COUNT 0", "[]"},
		{"LPOS missing a", "(nil)"},
		{"LPOS missing a COUNT 0", "[]"},
		// MAXLEN bounds the comparisons, not the results.
		{"LPOS p c MAXLEN 3", ":2"},
		{"LPOS p c MAXLEN 2 COUNT 0", "[]"},
		{"LPOS p c RANK 0", rankZeroErr},
		{"LPOS p c COUNT -1", "-ERR COUNT can't be negative"},
		{"LPOS p c MAXLEN -1", "-ERR MAXLEN can't be negative"},
		{"LPOS p c BOGUS 1", "-ERR syntax error"},
		{"LPOS p c COUNT", "-ERR syntax error"},
		{"LPOS p c COUNT abc", "-ERR value is not an integer or out of range"},
		{"LPOS p", "-ERR wrong number of arguments for 'lpos' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("SET str v")
	if got := c.cmd("LPOS str a"); got != wrongType {
		t.Errorf("LPOS on a string = %q; want %q", got, wrongType)
	}
}

// TestListPopCountAndMove covers the count form of LPOP/RPOP -- which replies with a
// null *array* for a missing key, not a null bulk string -- and the two move
// commands.
func TestListPopCountAndMove(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"RPUSH q a b c d e", ":5"},
		{"LPOP q 2", "[a b]"},
		{"RPOP q 2", "[e d]"},
		{"LPOP q 0", "[]"},
		{"LPOP q 10", "[c]"},
		{"EXISTS q", ":0"},
		{"LPOP missing 2", "(nil)"},
		{"LPOP missing", "(nil)"},
		{"RPUSH q2 a", ":1"},
		{"LPOP q2 -1", "-ERR value is out of range, must be positive"},
		{"LPOP q2 abc", "-ERR value is not an integer or out of range"},
		{"LPOP q2 1 2", "-ERR wrong number of arguments for 'lpop' command"},
		{"RPOP q2 -1", "-ERR value is out of range, must be positive"},
		// RPOPLPUSH and LMOVE.
		{"RPUSH s1 a b c", ":3"},
		{"RPOPLPUSH s1 d1", "c"},
		{"LRANGE s1 0 -1", "[a b]"},
		{"LRANGE d1 0 -1", "[c]"},
		{"LMOVE s1 d1 LEFT RIGHT", "a"},
		{"LRANGE d1 0 -1", "[c a]"},
		{"LMOVE s1 d1 RIGHT LEFT", "b"},
		{"LRANGE d1 0 -1", "[b c a]"},
		{"EXISTS s1", ":0"}, // drained source is deleted
		{"RPOPLPUSH missing d1", "(nil)"},
		{"LMOVE missing d1 LEFT LEFT", "(nil)"},
		{"LMOVE d1 d1 UP DOWN", "-ERR syntax error"},
		// A move onto itself rotates the list.
		{"RPUSH rot a b c", ":3"},
		{"LMOVE rot rot LEFT RIGHT", "a"},
		{"LRANGE rot 0 -1", "[b c a]"},
		{"LMOVE rot rot RIGHT LEFT", "a"},
		{"LRANGE rot 0 -1", "[a b c]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A wrong-typed source or destination is an error, and moves nothing.
	c.cmd("SET str v")
	c.cmd("RPUSH ok a")
	for _, cmd := range []string{"RPOPLPUSH str ok", "LMOVE ok str LEFT LEFT"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q = %q; want %q", cmd, got, wrongType)
		}
	}
	if got := c.cmd("LRANGE ok 0 -1"); got != "[a]" {
		t.Errorf("list after a rejected move = %q; want [a]", got)
	}
}
