package server

import "testing"

// TestZAddFlags covers ZADD's NX/XX/GT/LT/CH/INCR flags. The reply is the subtle
// part: by default it counts members *added*, so an update that changed a score
// still answers 0 unless CH asks for changes instead.
func TestZAddFlags(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"ZADD z 1 a 2 b", ":2"},
		// NX never updates; XX never inserts.
		{"ZADD z NX 5 a", ":0"},
		{"ZSCORE z a", "1"},
		{"ZADD z NX 3 c", ":1"},
		{"ZADD z XX 5 a", ":0"},
		{"ZSCORE z a", "5"},
		{"ZADD z XX 1 absent", ":0"},
		{"ZSCORE z absent", "(nil)"},
		{"EXISTS fresh", ":0"},
		{"ZADD fresh XX 1 a", ":0"},
		{"EXISTS fresh", ":0"}, // XX must not leave an empty set behind
		// CH counts changes rather than insertions.
		{"ZADD z CH 10 a", ":1"},
		{"ZADD z CH 10 a", ":0"},
		// GT only raises a score, LT only lowers one; both still insert new members.
		{"ZADD z GT 5 a", ":0"},
		{"ZSCORE z a", "10"},
		{"ZADD z GT CH 30 a", ":1"},
		{"ZSCORE z a", "30"},
		{"ZADD z LT CH 40 a", ":0"},
		{"ZSCORE z a", "30"},
		{"ZADD z LT CH 20 a", ":1"},
		{"ZSCORE z a", "20"},
		{"ZADD z GT 1 brandnew", ":1"},
		// INCR replies with the new score, or a null when the flags refused.
		{"ZADD z INCR 5 a", "25"},
		{"ZADD z NX INCR 5 a", "(nil)"},
		{"ZADD z XX INCR 5 absent", "(nil)"},
		{"ZADD z GT INCR -5 a", "(nil)"},
		{"ZSCORE z a", "25"},
		{"ZADD z LT INCR -5 a", "20"},
		// Infinite scores are legal and read back the way Redis spells them.
		{"ZADD z inf big", ":1"},
		{"ZSCORE z big", "inf"},
		{"ZADD z -inf small", ":1"},
		{"ZSCORE z small", "-inf"},
		// Incompatible flags and malformed pairs.
		{"ZADD z NX XX 1 a", "-ERR XX and NX options at the same time are not compatible"},
		{"ZADD z NX GT 1 a", "-ERR GT, LT, and/or NX options at the same time are not compatible"},
		{"ZADD z GT LT 1 a", "-ERR GT, LT, and/or NX options at the same time are not compatible"},
		{"ZADD z INCR 1 a 2 b", "-ERR INCR option supports a single increment-element pair"},
		{"ZADD z 1 a 2", "-ERR wrong number of arguments for 'zadd' command"},
		{"ZADD z NX", "-ERR wrong number of arguments for 'zadd' command"},
		{"ZADD z nan a", "-ERR value is not a valid float"},
		{"ZADD z abc a", "-ERR value is not a valid float"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A rejected pair must not apply any of the command's pairs.
	c.cmd("ZADD atomic 1 keep")
	if got := c.cmd("ZADD atomic 2 ok abc bad"); got != "-ERR value is not a valid float" {
		t.Fatalf("ZADD with a bad score = %q", got)
	}
	if got := c.cmd("ZRANGE atomic 0 -1"); got != "[keep]" {
		t.Errorf("set after a rejected ZADD = %q; want [keep]", got)
	}

	c.cmd("SET str v")
	for _, cmd := range []string{"ZADD str 1 a", "ZADD str INCR 1 a", "ZINCRBY str 1 a"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestZIncrByAndZRem covers the increment and the now-variadic removal.
func TestZIncrByAndZRem(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"ZINCRBY zi 5 m", "5"}, // creates the set and the member
		{"ZINCRBY zi 2.5 m", "7.5"},
		{"ZINCRBY zi -7.5 m", "0"},
		{"ZINCRBY zi abc m", "-ERR value is not a valid float"},
		{"ZINCRBY zi nan m", "-ERR value is not a valid float"},
		{"ZINCRBY zi 1", "-ERR wrong number of arguments for 'zincrby' command"},
		// Opposite infinities are the only way to reach a NaN score.
		{"ZADD inf inf m", ":1"},
		{"ZINCRBY inf -inf m", "-ERR resulting score is not a number (NaN)"},
		{"ZSCORE inf m", "inf"},
		// ZREM takes any number of members.
		{"ZADD zm 1 a 2 b 3 c", ":3"},
		{"ZREM zm a b nope", ":2"},
		{"ZRANGE zm 0 -1", "[c]"},
		{"ZREM zm c", ":1"},
		{"EXISTS zm", ":0"},
		{"ZREM missing a b", ":0"},
		{"ZREM zm", "-ERR wrong number of arguments for 'zrem' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("SET str v")
	if got := c.cmd("ZREM str a"); got != wrongType {
		t.Errorf("ZREM on a string = %q; want %q", got, wrongType)
	}
}

// TestZRangeQueries covers the rank- and score-ordered range queries, including the
// reversed argument order ZREVRANGEBYSCORE takes and the LIMIT clause.
func TestZRangeQueries(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("ZADD sc 1 a 2 b 3 c")

	cases := []struct{ cmd, want string }{
		// Reverse rank ranges.
		{"ZREVRANGE sc 0 -1", "[c b a]"},
		{"ZREVRANGE sc 0 0", "[c]"},
		{"ZREVRANGE sc -2 -1", "[b a]"},
		{"ZREVRANGE sc 0 -1 WITHSCORES", "[c 3 b 2 a 1]"},
		{"ZREVRANGE sc 5 10", "[]"},
		{"ZREVRANGE missing 0 -1", "[]"},
		{"ZREVRANGE sc 0 -1 BOGUS", "-ERR syntax error"},
		{"ZREVRANGE sc abc 1", "-ERR value is not an integer or out of range"},
		// Score ranges, with exclusive bounds and infinities.
		{"ZRANGEBYSCORE sc 1 2", "[a b]"},
		{"ZRANGEBYSCORE sc (1 3", "[b c]"},
		{"ZRANGEBYSCORE sc (1 (3", "[b]"},
		{"ZRANGEBYSCORE sc -inf +inf", "[a b c]"},
		{"ZRANGEBYSCORE sc -inf +inf WITHSCORES", "[a 1 b 2 c 3]"},
		{"ZRANGEBYSCORE sc 5 10", "[]"},
		{"ZRANGEBYSCORE sc 3 1", "[]"},
		{"ZRANGEBYSCORE missing 1 2", "[]"},
		{"ZRANGEBYSCORE sc 1 3 LIMIT 1 1", "[b]"},
		{"ZRANGEBYSCORE sc 1 3 LIMIT 1 -1", "[b c]"},
		{"ZRANGEBYSCORE sc 1 3 LIMIT 5 2", "[]"},
		{"ZRANGEBYSCORE sc 1 3 LIMIT -1 2", "[]"},
		{"ZRANGEBYSCORE sc 1 3 WITHSCORES LIMIT 1 1", "[b 2]"},
		{"ZRANGEBYSCORE sc abc 2", "-ERR min or max is not a float"},
		{"ZRANGEBYSCORE sc 1 nan", "-ERR min or max is not a float"},
		{"ZRANGEBYSCORE sc 1 2 BOGUS", "-ERR syntax error"},
		{"ZRANGEBYSCORE sc 1 2 LIMIT 1", "-ERR syntax error"},
		{"ZRANGEBYSCORE sc 1 2 LIMIT a b", "-ERR value is not an integer or out of range"},
		// The reverse form takes max first.
		{"ZREVRANGEBYSCORE sc 3 1", "[c b a]"},
		{"ZREVRANGEBYSCORE sc +inf -inf", "[c b a]"},
		{"ZREVRANGEBYSCORE sc 3 (1", "[c b]"},
		{"ZREVRANGEBYSCORE sc 3 1 LIMIT 1 1", "[b]"},
		{"ZREVRANGEBYSCORE sc 1 3", "[]"},
		{"ZREVRANGEBYSCORE missing 3 1", "[]"},
		// Ranks and counts.
		{"ZREVRANK sc c", ":0"},
		{"ZREVRANK sc a", ":2"},
		{"ZREVRANK sc nope", "(nil)"},
		{"ZREVRANK missing a", "(nil)"},
		{"ZCOUNT sc 1 3", ":3"},
		{"ZCOUNT sc (1 3", ":2"},
		{"ZCOUNT sc -inf +inf", ":3"},
		{"ZCOUNT sc 5 10", ":0"},
		{"ZCOUNT missing 1 2", ":0"},
		{"ZCOUNT sc abc 1", "-ERR min or max is not a float"},
		{"ZMSCORE sc a nope c", "[1 (nil) 3]"},
		{"ZMSCORE missing a b", "[(nil) (nil)]"},
		{"ZMSCORE sc", "-ERR wrong number of arguments for 'zmscore' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// ZLEXCOUNT is defined over members sharing one score.
	c.cmd("ZADD lex 0 a 0 b 0 c")
	lexCases := []struct{ cmd, want string }{
		{"ZLEXCOUNT lex - +", ":3"},
		{"ZLEXCOUNT lex [a [b", ":2"},
		{"ZLEXCOUNT lex (a [c", ":2"},
		{"ZLEXCOUNT lex - [b", ":2"},
		{"ZLEXCOUNT lex [c +", ":1"},
		{"ZLEXCOUNT lex [z +", ":0"},
		{"ZLEXCOUNT missing - +", ":0"},
		{"ZLEXCOUNT lex a b", "-ERR min or max not valid string range item"},
		{"ZLEXCOUNT lex - b", "-ERR min or max not valid string range item"},
	}
	for _, tc := range lexCases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("SET str v")
	for _, cmd := range []string{
		"ZREVRANGE str 0 -1", "ZRANGEBYSCORE str 1 2", "ZREVRANGEBYSCORE str 2 1",
		"ZREVRANK str a", "ZCOUNT str 1 2", "ZMSCORE str a", "ZLEXCOUNT str - +",
	} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestZRemRangeAndPop covers the range removals and the two pops, all of which
// delete the key once the set is empty.
func TestZRemRangeAndPop(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"ZADD rr 1 a 2 b 3 c 4 d", ":4"},
		{"ZREMRANGEBYRANK rr 0 1", ":2"},
		{"ZRANGE rr 0 -1", "[c d]"},
		{"ZREMRANGEBYRANK rr -1 -1", ":1"},
		{"ZRANGE rr 0 -1", "[c]"},
		{"ZREMRANGEBYRANK rr 5 10", ":0"},
		{"ZREMRANGEBYRANK rr 0 -1", ":1"},
		{"EXISTS rr", ":0"},
		{"ZREMRANGEBYRANK missing 0 -1", ":0"},
		{"ZREMRANGEBYRANK rr abc 1", "-ERR value is not an integer or out of range"},
		{"ZADD rs 1 a 2 b 3 c", ":3"},
		{"ZREMRANGEBYSCORE rs 1 2", ":2"},
		{"ZRANGE rs 0 -1", "[c]"},
		{"ZREMRANGEBYSCORE rs (3 +inf", ":0"},
		{"ZREMRANGEBYSCORE rs -inf +inf", ":1"},
		{"EXISTS rs", ":0"},
		{"ZREMRANGEBYSCORE missing 1 2", ":0"},
		{"ZREMRANGEBYSCORE rs abc 1", "-ERR min or max is not a float"},
		// The pops reply with flattened member/score pairs.
		{"ZADD pop 1 a 2 b 3 c", ":3"},
		{"ZPOPMIN pop", "[a 1]"},
		{"ZPOPMAX pop", "[c 3]"},
		{"ZPOPMIN pop 5", "[b 2]"},
		{"EXISTS pop", ":0"},
		{"ZPOPMIN missing", "[]"},
		{"ZPOPMAX missing 3", "[]"},
		{"ZADD pop2 1 a 2 b 3 c", ":3"},
		{"ZPOPMIN pop2 2", "[a 1 b 2]"},
		{"ZPOPMAX pop2 0", "[]"},
		{"ZPOPMIN pop2 -1", "-ERR value is out of range, must be positive"},
		{"ZPOPMIN pop2 abc", "-ERR value is not an integer or out of range"},
		{"ZPOPMIN pop2 1 2", "-ERR wrong number of arguments for 'zpopmin' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("SET str v")
	for _, cmd := range []string{
		"ZREMRANGEBYRANK str 0 -1", "ZREMRANGEBYSCORE str 1 2", "ZPOPMIN str", "ZPOPMAX str 2",
	} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestZRandMember covers ZRANDMEMBER's reply shapes and the sign of count.
func TestZRandMember(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("ZADD one 1 only")

	cases := []struct{ cmd, want string }{
		{"ZRANDMEMBER one", "only"},
		{"ZRANDMEMBER one 3", "[only]"},
		{"ZRANDMEMBER one -3", "[only only only]"},
		{"ZRANDMEMBER one 1 WITHSCORES", "[only 1]"},
		{"ZRANDMEMBER one 0", "[]"},
		{"ZRANDMEMBER missing", "(nil)"},
		{"ZRANDMEMBER missing 3", "[]"},
		{"ZRANDMEMBER one abc", "-ERR value is not an integer or out of range"},
		{"ZRANDMEMBER one 1 BOGUS", "-ERR syntax error"},
		{"ZRANDMEMBER one 1 WITHSCORES EXTRA", "-ERR syntax error"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("ZADD many 1 a 2 b 3 c")
	if got := arrayFields(c.cmd("ZRANDMEMBER many 10")); got != "a,b,c" {
		t.Errorf("ZRANDMEMBER many 10 = %q; want the whole set", got)
	}
	if got := c.cmd("ZCARD many"); got != ":3" {
		t.Errorf("ZRANDMEMBER removed members: ZCARD = %q", got)
	}

	c.cmd("SET str v")
	if got := c.cmd("ZRANDMEMBER str"); got != wrongType {
		t.Errorf("ZRANDMEMBER on a string = %q; want %q", got, wrongType)
	}
}
