package server

// Wire-level tests for the commands added alongside the encoding-threshold configuration:
// the sorted-set algebra and its lexicographic ranges, SORT, LCS, the radius searches,
// EXPIRETIME/PEXPIRETIME, LPUSHX/RPUSHX, ROLE, CLIENT REPLY and the connection bounds.
//
// Every expected reply here was captured from a real redis:7.2-alpine driven with the same
// commands, which is the only way the error strings and the reply *shapes* can be trusted
// -- reasoning about them is exactly how a server ends up almost-compatible.

import (
	"net"
	"strings"
	"testing"
	"time"
)

// TestZSetAlgebra covers ZUNION/ZINTER/ZDIFF and their storing forms, with the WEIGHTS and
// AGGREGATE options, the numkeys refusals, and the fact that a plain set is a legal input
// whose members each weigh 1.
func TestZSetAlgebra(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"ZADD z1 1 a 2 b 3 c", ":3"},
		{"ZADD z2 1 a", ":1"},
		{"SADD plain a d", ":2"},

		{"ZUNION 2 z1 z2", "[a b c]"},
		{"ZUNION 2 z1 z2 WITHSCORES", "[a 2 b 2 c 3]"},
		{"ZINTER 2 z1 z2 WITHSCORES", "[a 2]"},
		{"ZDIFF 2 z1 z2", "[b c]"},
		{"ZDIFF 2 z1 z2 WITHSCORES", "[b 2 c 3]"},
		{"ZINTERCARD 2 z1 z2", ":1"},
		{"ZINTERCARD 2 z1 z2 LIMIT 0", ":1"},
		{"ZINTERCARD 2 z1 z1 LIMIT 1", ":1"},

		// A set contributes a score of 1 per member, which is what makes the algebra work
		// across the two types.
		{"ZUNION 2 z1 plain WITHSCORES", "[d 1 a 2 b 2 c 3]"},
		{"ZINTER 2 z1 plain WITHSCORES", "[a 2]"},

		// WEIGHTS multiply, AGGREGATE folds.
		{"ZUNIONSTORE dst 2 z1 z2 WEIGHTS 2 3", ":3"},
		{"ZRANGE dst 0 -1 WITHSCORES", "[b 4 a 5 c 6]"},
		{"ZUNIONSTORE dst 2 z1 z2 AGGREGATE MIN", ":3"},
		{"ZRANGE dst 0 -1 WITHSCORES", "[a 1 b 2 c 3]"},
		{"ZUNIONSTORE dst 2 z1 z2 AGGREGATE MAX", ":3"},
		{"ZRANGE dst 0 -1 WITHSCORES", "[a 1 b 2 c 3]"},
		{"ZINTERSTORE dst 2 z1 z2", ":1"},
		{"ZDIFFSTORE dst 2 z1 z2", ":2"},
		{"ZRANGE dst 0 -1", "[b c]"},

		// An empty result deletes the destination, which is what makes the 0 reply mean
		// "there is nothing there now" rather than "nothing happened".
		{"ZINTERSTORE dst 2 z1 nosuch", ":0"},
		{"EXISTS dst", ":0"},

		// The refusals, with Redis's exact wording.
		{"ZUNIONSTORE dst 0 k", "-ERR at least 1 input key is needed for 'zunionstore' command"},
		{"ZUNIONSTORE dst notanum z1", "-ERR value is not an integer or out of range"},
		{"ZUNIONSTORE dst 3 z1", "-ERR syntax error"},
		{"ZUNIONSTORE dst 2 z1 z2 WEIGHTS 1", "-ERR syntax error"},
		{"ZUNIONSTORE dst 2 z1 z2 WEIGHTS 1 bad", "-ERR weight value is not a float"},
		{"ZUNIONSTORE dst 2 z1 z2 AGGREGATE bogus", "-ERR syntax error"},
		{"ZUNIONSTORE dst 2 z1 z2 WITHSCORES", "-ERR syntax error"},
		// The difference has no weights and no aggregation to apply.
		{"ZDIFFSTORE dst 2 z1 z2 WEIGHTS 1 1", "-ERR syntax error"},
		{"ZDIFF 2 z1 z2 AGGREGATE MIN", "-ERR syntax error"},
		{"ZINTERCARD 0", "-ERR wrong number of arguments for 'zintercard' command"},
		{"ZINTERCARD 1 z1 LIMIT -1", "-ERR LIMIT can't be negative"},
		{"ZINTERCARD 1 z1 LIMIT a", "-ERR LIMIT can't be negative"},
		{"ZINTERCARD 1 z1 z1", "-ERR syntax error"},

		// A wrong-typed input is a WRONGTYPE wherever it appears in the list.
		{"SET str v", "+OK"},
		{"ZUNIONSTORE dst 2 z1 str", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"ZINTER 2 str z1", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestZSetLexRanges covers the lexicographic range family and the general ZRANGE's
// BYSCORE/BYLEX/REV/LIMIT options, including the two error messages that distinguish a
// malformed bound from a malformed option combination.
func TestZSetLexRanges(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"ZADD z 0 a 0 b 0 c 0 d", ":4"},
		{"ZRANGEBYLEX z - +", "[a b c d]"},
		{"ZRANGEBYLEX z [b [c", "[b c]"},
		{"ZRANGEBYLEX z (b +", "[c d]"},
		{"ZRANGEBYLEX z - + LIMIT 1 2", "[b c]"},
		{"ZREVRANGEBYLEX z + -", "[d c b a]"},
		{"ZREVRANGEBYLEX z [c [b", "[c b]"},
		// Both infinities are legal at either end: "+ -" is a lower bound above every member
		// and an upper bound below every one, so it selects nothing rather than erroring.
		{"ZRANGEBYLEX z + -", "[]"},
		{"ZLEXCOUNT z + -", ":0"},
		{"ZLEXCOUNT z - +", ":4"},
		// A bare member is not a bound.
		{"ZRANGEBYLEX z a c", "-ERR min or max not valid string range item"},
		{"ZRANGEBYLEX z - + LIMIT 1", "-ERR syntax error"},

		{"ZREMRANGEBYLEX z [a [b", ":2"},
		{"ZRANGEBYLEX z - +", "[c d]"},

		// The general ZRANGE.
		{"ZADD s 1 a 2 b 3 c", ":3"},
		{"ZRANGE s 0 -1 REV", "[c b a]"},
		{"ZRANGE s 1 3 BYSCORE", "[a b c]"},
		{"ZRANGE s 3 1 BYSCORE REV", "[c b a]"},
		{"ZRANGE s (1 +inf BYSCORE", "[b c]"},
		{"ZRANGE s 1 3 BYSCORE LIMIT 1 1", "[b]"},
		{"ZRANGE s 0 -1 REV WITHSCORES", "[c 3 b 2 a 1]"},
		{"ZRANGE s 0 -1 LIMIT 1 2", "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"},
		{"ZRANGE s 0 -1 BYLEX WITHSCORES", "-ERR syntax error, WITHSCORES not supported in combination with BYLEX"},
		{"ZRANGE s 0 -1 BYSCORE BYLEX", "-ERR syntax error"},

		// ZRANGESTORE copies the same selection into a key.
		{"ZRANGESTORE d s 0 -1", ":3"},
		{"ZRANGE d 0 -1 WITHSCORES", "[a 1 b 2 c 3]"},
		{"ZRANGESTORE d s 2 3 BYSCORE", ":2"},
		{"ZRANGE d 0 -1", "[b c]"},
		{"ZRANGESTORE d s 5 10", ":0"},
		{"EXISTS d", ":0"},
		{"ZRANGESTORE d str 0 -1", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
	}
	// The wrong-typed source used by the last case.
	c.cmd("SET str v")
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestSort covers SORT's numeric and alphabetic orders, BY and GET patterns (including the
// hash-field form and "#"), LIMIT, STORE, and the two places where "do not sort" still
// honours DESC.
func TestSort(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	setup := []string{
		"RPUSH ml 3 1 2",
		"MSET weight_1 10 weight_2 5 weight_3 1 data_1 a data_2 b data_3 c",
		"HSET h_1 w 10 d aa", "HSET h_2 w 5 d bb", "HSET h_3 w 1 d cc",
		"RPUSH sl c a b",
		"ZADD zs 1 a 5 b 2 c",
	}
	for _, cmd := range setup {
		c.cmd(cmd)
	}

	cases := []struct{ cmd, want string }{
		{"SORT ml", "[1 2 3]"},
		{"SORT ml DESC", "[3 2 1]"},
		{"SORT ml LIMIT 0 2", "[1 2]"},
		{"SORT ml LIMIT 1 -1", "[2 3]"},
		{"SORT ml LIMIT 9 2", "[]"},
		{"SORT sl ALPHA", "[a b c]"},
		// Numeric sorting of non-numeric elements is refused rather than guessed at.
		{"SORT sl", "-ERR One or more scores can't be converted into double"},
		// BY a string pattern, and BY a hash field.
		{"SORT ml BY weight_*", "[3 2 1]"},
		{"SORT ml BY weight_* GET data_*", "[c b a]"},
		{"SORT ml BY h_*->w GET h_*->d", "[cc bb aa]"},
		{"SORT ml GET # GET data_*", "[1 a 2 b 3 c]"},
		// BY with ALPHA compares the weights as strings: "1" < "10" < "5".
		{"SORT ml BY weight_* ALPHA", "[3 1 2]"},
		// A pattern with no '*' is Redis's documented way to ask for no ordering at all --
		// and DESC still reverses the order the collection already has.
		{"SORT ml BY nosort", "[3 1 2]"},
		{"SORT ml BY nosort DESC", "[2 1 3]"},
		{"SORT zs BY nosort", "[a c b]"},
		{"SORT zs BY nosort DESC", "[b c a]"},
		// A missing GET key is a null in the reply and an empty string in a STORE.
		{"SORT ml GET nosuch_*", "[(nil) (nil) (nil)]"},

		{"SORT ml STORE d1", ":3"},
		{"LRANGE d1 0 -1", "[1 2 3]"},
		{"TYPE d1", "+list"},
		{"SORT nokey", "[]"},
		{"SORT nokey STORE d2", ":0"},
		{"EXISTS d2", ":0"},
		{"SORT ml bogus", "-ERR syntax error"},
		{"SORT ml LIMIT 1", "-ERR syntax error"},
		{"SORT ml BY", "-ERR syntax error"},
		// SORT_RO is the same command with STORE refused, so it can be sent to a replica.
		{"SORT_RO ml", "[1 2 3]"},
		{"SORT_RO ml STORE d3", "-ERR syntax error"},
		{"SET str v", "+OK"},
		{"SORT str", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// COMMAND GETKEYS reports the collection and the *last* STORE destination, because an
	// earlier one is parsed and then overwritten without ever being written to.
	if got := c.cmd("COMMAND GETKEYS SORT abc STORE invalid STORE def"); got != "[abc def]" {
		t.Errorf("COMMAND GETKEYS SORT with two STOREs -> %q; want [abc def]", got)
	}
	if got := c.cmd("COMMAND GETKEYS SORT abc BY w_* GET d_*"); got != "[abc]" {
		t.Errorf("COMMAND GETKEYS SORT with patterns -> %q; want [abc]", got)
	}
}

// TestLCS covers the three reply shapes and the refusals.
func TestLCS(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("MSET key1 ohmytext key2 mynewtext")
	cases := []struct{ cmd, want string }{
		{"LCS key1 key2", "mytext"},
		{"LCS key1 key2 LEN", ":6"},
		{"LCS key1 key2 IDX", "[matches [[[:4 :7] [:5 :8]] [[:2 :3] [:0 :1]]] len :6]"},
		{"LCS key1 key2 IDX MINMATCHLEN 4", "[matches [[[:4 :7] [:5 :8]]] len :6]"},
		{"LCS key1 key2 IDX WITHMATCHLEN", "[matches [[[:4 :7] [:5 :8] :4] [[:2 :3] [:0 :1] :2]] len :6]"},
		// WITHMATCHLEN without IDX has nothing to describe and is accepted, as in Redis.
		{"LCS key1 key2 WITHMATCHLEN", "mytext"},
		{"LCS key1 key2 LEN IDX", "-ERR If you want both the length and indexes, please just use IDX."},
		{"LCS key1 key2 bogus", "-ERR syntax error"},
		{"LCS key1 key2 MINMATCHLEN", "-ERR syntax error"},
		// A missing key is the empty string, so there is simply nothing in common.
		{"LCS nokey1 nokey2", ""},
		{"LCS nokey1 nokey2 LEN", ":0"},
		{"LCS nokey1 nokey2 IDX", "[matches [] len :0]"},
		// The refusal names the values rather than the operation, which is Redis's wording.
		{"RPUSH l a", ":1"},
		{"LCS key1 l", "-ERR The specified keys must contain string values"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestGeoRadius covers the deprecated radius searches, including the STORE/STOREDIST key
// operands (which GEOSEARCHSTORE spells as a flag) and the combination Redis refuses.
func TestGeoRadius(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania")
	cases := []struct{ cmd, want string }{
		{"GEORADIUS Sicily 15 37 200 km", "[Palermo Catania]"},
		{"GEORADIUS Sicily 15 37 200 km ASC", "[Catania Palermo]"},
		{"GEORADIUS Sicily 15 37 200 km DESC", "[Palermo Catania]"},
		{"GEORADIUS Sicily 15 37 200 km WITHDIST ASC", "[[Catania 56.4413] [Palermo 190.4424]]"},
		{"GEORADIUS Sicily 15 37 200 km COUNT 1", "[Catania]"},
		{"GEORADIUSBYMEMBER Sicily Palermo 200 km", "[Palermo Catania]"},
		{"GEORADIUSBYMEMBER Sicily Palermo 1 km", "[Palermo]"},
		{"GEORADIUS_RO Sicily 15 37 200 km", "[Palermo Catania]"},
		{"GEORADIUSBYMEMBER_RO Sicily Palermo 200 km", "[Palermo Catania]"},

		// STORE keeps the geohashes, so the destination is still a geo set; STOREDIST keeps
		// the distances, which makes it an ordinary sorted set ordered by proximity.
		{"GEORADIUS Sicily 15 37 200 km STORE d1", ":2"},
		{"ZSCORE d1 Palermo", "3479099956230698"},
		{"GEORADIUS Sicily 15 37 200 km STOREDIST d2", ":2"},
		{"ZRANGE d2 0 -1", "[Catania Palermo]"},
		// An empty result deletes the destination.
		{"GEORADIUS Sicily 15 37 1 m STORE d1", ":0"},
		{"EXISTS d1", ":0"},

		{"GEORADIUS Sicily 15 37 200 km WITHDIST STORE d1",
			"-ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"},
		{"GEORADIUS_RO Sicily 15 37 200 km STORE d1", "-ERR syntax error"},
		{"GEORADIUSBYMEMBER_RO Sicily Palermo 200 km STOREDIST d1", "-ERR syntax error"},
		{"GEORADIUS Sicily 15 37 200 bogus", "-ERR unsupported unit provided. please use M, KM, FT, MI"},
		{"GEORADIUS Sicily 15 37 200 km COUNT 0", "-ERR COUNT must be > 0"},
		{"GEORADIUS Sicily 15 37 200 km ANY", "-ERR the ANY argument requires COUNT argument"},
		{"GEORADIUS Sicily bad 37 200 km", "-ERR value is not a valid float"},
		{"GEORADIUSBYMEMBER Sicily nomember 200 km", "-ERR could not decode requested zset member"},
		// A missing key is an empty search, not a missing centre.
		{"GEORADIUSBYMEMBER nokey m 200 km", "[]"},
		{"GEORADIUS nokey 15 37 200 km", "[]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// The STORE destination is a key even though it is not at a fixed position: without it
	// in the list a WATCH on it would miss the overwrite (invariant 7).
	if got := c.cmd("COMMAND GETKEYS GEORADIUS Sicily 15 37 200 km STORE d9"); got != "[Sicily d9]" {
		t.Errorf("COMMAND GETKEYS GEORADIUS ... STORE -> %q; want [Sicily d9]", got)
	}
}

// TestAbsoluteExpireReaders covers EXPIRETIME and PEXPIRETIME, whose whole point is that
// the caller does not have to reconstruct a deadline from a remaining TTL and its own clock.
func TestAbsoluteExpireReaders(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET k v")
	c.cmd("PEXPIREAT k 33177117420000")
	cases := []struct{ cmd, want string }{
		{"EXPIRETIME k", ":33177117420"},
		{"PEXPIRETIME k", ":33177117420000"},
		{"SET persistent v", "+OK"},
		{"EXPIRETIME persistent", ":-1"},
		{"PEXPIRETIME persistent", ":-1"},
		{"EXPIRETIME ghost", ":-2"},
		{"PEXPIRETIME ghost", ":-2"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestPushXAndHMSet covers the conditional pushes and HMSET's older reply.
func TestPushXAndHMSet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"LPUSHX nolist a", ":0"},
		{"EXISTS nolist", ":0"}, // the conditional form must create nothing
		{"RPUSHX nolist a", ":0"},
		{"EXISTS nolist", ":0"},
		{"RPUSH l a", ":1"},
		{"LPUSHX l b c", ":3"},
		{"RPUSHX l d", ":4"},
		{"LRANGE l 0 -1", "[c b a d]"},
		// The wrong-type check still comes first: LPUSHX against a string is an error, not a
		// silent 0.
		{"SET str v", "+OK"},
		{"LPUSHX str x", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"LPUSHX l", "-ERR wrong number of arguments for 'lpushx' command"},

		{"HMSET h a 1 b 2", "+OK"},
		{"HGET h a", "1"},
		{"HMSET h a", "-ERR wrong number of arguments for 'hmset' command"},
		{"HMSET h", "-ERR wrong number of arguments for 'hmset' command"},

		// A negative RANK counts from the tail, so the search negates it -- and the smallest
		// int64 negates to itself. Refused by name, as Redis refuses it.
		{"LPOS l a RANK -9223372036854775808",
			"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"},
		{"LPOS l a RANK -1", ":2"},
		// The same hazard in the RANDMEMBER family, which also negates its count.
		{"SADD rs a b c", ":3"},
		{"SRANDMEMBER rs -9223372036854775808",
			"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"},
		{"HRANDFIELD h -9223372036854775808 WITHVALUES",
			"-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"},
		{"ZADD rz 1 a", ":1"},
		{"ZRANDMEMBER rz -9223372036854770000 WITHSCORES", "-ERR value is out of range"},
		// And the offset SETRANGE would have sliced at.
		{"SETRANGE sr 9223372036854775807 A",
			"-ERR string exceeds maximum allowed size (proto-max-bulk-len)"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestScanTypeAndNoValues covers SCAN's TYPE filter and HSCAN's NOVALUES, including the
// fact that each is refused by the commands it does not belong to -- and that the *type*
// check happens before the options are parsed, which is visible.
func TestScanTypeAndNoValues(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("MSET a 1 b 2")
	c.cmd("RPUSH l x")
	c.cmd("HSET h f v")

	if got := c.cmd("SCAN 0 TYPE list"); got != "[0 [l]]" {
		t.Errorf("SCAN 0 TYPE list -> %q; want [0 [l]]", got)
	}
	if got := c.cmd("SCAN 0 TYPE hash"); got != "[0 [h]]" {
		t.Errorf("SCAN 0 TYPE hash -> %q; want [0 [h]]", got)
	}
	// An unknown type name matches nothing rather than erroring: the set of type names is a
	// property of the server, not of the request.
	if got := c.cmd("SCAN 0 TYPE bogus"); got != "[0 []]" {
		t.Errorf("SCAN 0 TYPE bogus -> %q; want [0 []]", got)
	}
	if got := c.cmd("HSCAN h 0 NOVALUES"); got != "[0 [f]]" {
		t.Errorf("HSCAN h 0 NOVALUES -> %q; want [0 [f]]", got)
	}
	if got := c.cmd("HSCAN h 0"); got != "[0 [f v]]" {
		t.Errorf("HSCAN h 0 -> %q; want [0 [f v]]", got)
	}
	if got := c.cmd("HSCAN h 0 MATCH f* NOVALUES"); got != "[0 [f]]" {
		t.Errorf("HSCAN h 0 MATCH f* NOVALUES -> %q; want [0 [f]]", got)
	}
	if got := c.cmd("SSCAN h 0 NOVALUES"); got != "-WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Errorf("SSCAN on a hash -> %q; want WRONGTYPE", got)
	}
	if got := c.cmd("SSCAN l 0 TYPE string"); got != "-WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Errorf("SSCAN on a list -> %q; want WRONGTYPE (the type is checked before the options)", got)
	}
	if got := c.cmd("SADD s m"); got != ":1" {
		t.Fatalf("SADD -> %q", got)
	}
	if got := c.cmd("SSCAN s 0 TYPE string"); got != "-ERR syntax error" {
		t.Errorf("SSCAN ... TYPE -> %q; want a syntax error", got)
	}
}

// TestEncodingThresholdsAreConfigurable checks the CONFIG SET half of the thresholds: the
// parameter names Redis uses, the ziplist aliases sharing one value, and OBJECT ENCODING
// actually following what was set.
func TestEncodingThresholdsAreConfigurable(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// Redis 7.2's own defaults, read from a live one rather than from documentation --
		// note that the hash's is 512 and the list's is a negative byte budget.
		{"CONFIG GET hash-max-listpack-entries", "[hash-max-listpack-entries 512]"},
		{"CONFIG GET hash-max-listpack-value", "[hash-max-listpack-value 64]"},
		{"CONFIG GET list-max-listpack-size", "[list-max-listpack-size -2]"},
		{"CONFIG GET set-max-intset-entries", "[set-max-intset-entries 512]"},
		{"CONFIG GET set-max-listpack-entries", "[set-max-listpack-entries 128]"},
		{"CONFIG GET zset-max-listpack-entries", "[zset-max-listpack-entries 128]"},
		{"CONFIG GET hll-sparse-max-bytes", "[hll-sparse-max-bytes 3000]"},
		// The ziplist spelling is an alias over the same value, so setting one moves both.
		{"CONFIG SET hash-max-ziplist-entries 3", "+OK"},
		{"CONFIG GET hash-max-listpack-entries", "[hash-max-listpack-entries 3]"},
		{"CONFIG SET zset-max-listpack-entries 2", "+OK"},
		{"CONFIG GET zset-max-ziplist-entries", "[zset-max-ziplist-entries 2]"},

		// And the report follows: OBJECT ENCODING is what the thresholds are for.
		{"HSET h a 1 b 2 c 3", ":3"},
		{"OBJECT ENCODING h", "listpack"},
		{"HSET h d 4", ":1"},
		{"OBJECT ENCODING h", "hashtable"},
		{"ZADD z 1 a 2 b", ":2"},
		{"OBJECT ENCODING z", "listpack"},
		{"ZADD z 3 c", ":1"},
		{"OBJECT ENCODING z", "skiplist"},
		{"CONFIG SET list-max-listpack-size 2", "+OK"},
		{"RPUSH l a b", ":2"},
		{"OBJECT ENCODING l", "listpack"},
		{"RPUSH l c", ":3"},
		{"OBJECT ENCODING l", "quicklist"},

		// A negative value is meaningful only for the list's byte budget.
		{"CONFIG SET hash-max-listpack-entries -1",
			"-ERR CONFIG SET failed (possibly related to argument 'hash-max-listpack-entries') - " +
				"argument couldn't be parsed into an integer, or is out of range"},
		{"CONFIG SET list-max-listpack-size -5", "+OK"},

		// The two parameters that are accepted and genuinely tune nothing, because the
		// structures they name do not exist here. They report back what they were told.
		{"CONFIG SET stream-node-max-entries 50", "+OK"},
		{"CONFIG GET stream-node-max-entries", "[stream-node-max-entries 50]"},
		{"CONFIG SET list-compress-depth 2", "+OK"},
		{"CONFIG GET list-compress-depth", "[list-compress-depth 2]"},

		// The snapshot schedule. These three expectations were correct while there was no
		// snapshot mechanism -- an empty schedule was the literal truth, and a non-empty one
		// would have been a durability promise the server could not keep -- and they are
		// obsolete now that snapshots exist. The schedule is real, and the default is Redis's
		// own: measured on redis 7.2, `CONFIG GET save` answers `3600 1 300 100 60 10000`.
		{"CONFIG GET save", "[save 3600 1 300 100 60 10000]"},
		{`CONFIG SET save "3600 1"`, "+OK"},
		{"CONFIG GET save", "[save 3600 1]"},
		// Measured on redis 7.2: the empty string is accepted and means "no snapshots".
		{`CONFIG SET save ""`, "+OK"},
		{"CONFIG GET save", "[save ]"},
		// Measured on redis 7.2: a lone number, a non-numeric operand and a negative one are
		// all refused with this wording. A half-applied schedule would be a durability
		// setting that does not do what the operator wrote down.
		{`CONFIG SET save "900"`,
			"-ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters"},
		{`CONFIG SET save "abc 1"`,
			"-ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters"},
		{`CONFIG SET save "900 -1"`,
			"-ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters"},
		// dbfilename and dir read empty when no snapshot path was configured, which is how
		// this table already reports an unconfigured file. "." -- what filepath.Base and
		// filepath.Dir answer for an empty path -- is a real relative directory, so
		// reporting it would name a file this server has no intention of writing.
		{"CONFIG GET dbfilename", "[dbfilename ]"},
		{"CONFIG GET dir", "[dir ]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestHLLSparseMaxBytes checks that hll-sparse-max-bytes is wired to the real
// sparse/dense switchover rather than merely remembered: at 0 no sketch can stay sparse,
// so the very first PFADD produces a dense 12 KB value.
func TestHLLSparseMaxBytes(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("PFADD sparse a b c")
	if got := c.cmd("STRLEN sparse"); got == ":12304" {
		t.Errorf("a three-element sketch is dense (%s) before the threshold moved", got)
	}
	if got := c.cmd("CONFIG SET hll-sparse-max-bytes 0"); got != "+OK" {
		t.Fatalf("CONFIG SET hll-sparse-max-bytes -> %q", got)
	}
	c.cmd("PFADD dense a b c")
	if got := c.cmd("STRLEN dense"); got != ":12304" {
		t.Errorf("STRLEN of a sketch built with hll-sparse-max-bytes 0 = %s; want :12304 (dense)", got)
	}
	// And it still counts correctly, which is the point of the two encodings being
	// interchangeable.
	if got := c.cmd("PFCOUNT dense"); got != ":3" {
		t.Errorf("PFCOUNT of the dense sketch = %s; want :3", got)
	}
}

// TestRole covers ROLE's two shapes. A client library calls it to decide whether the
// connection it holds is writable, so the reply is typed where INFO's is text.
func TestRole(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("ROLE"); got != "[master :0 []]" {
		t.Errorf("ROLE on a master -> %q; want [master :0 []]", got)
	}
}

// TestClientReply covers CLIENT REPLY ON|OFF|SKIP. The asymmetry is the interface: ON
// acknowledges, OFF and SKIP answer with nothing at all, and SKIP swallows exactly one
// following reply.
func TestClientReply(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Sent as one pipeline, so the absent replies are absences in a known position rather
	// than a timeout: what comes back must be exactly the replies that were not suppressed.
	script := "SET a 1\r\nCLIENT REPLY OFF\r\nSET b 2\r\nSET c 3\r\nCLIENT REPLY ON\r\n" +
		"GET b\r\nCLIENT REPLY SKIP\r\nSET d 4\r\nGET d\r\n"
	if _, err := conn.Write([]byte(script)); err != nil {
		t.Fatalf("write: %v", err)
	}
	want := "+OK\r\n+OK\r\n$1\r\n2\r\n$1\r\n4\r\n"
	buf := make([]byte, len(want))
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	n, err := readFull(conn, buf)
	if err != nil {
		t.Fatalf("read: %v (got %q)", err, buf[:n])
	}
	if got := string(buf[:n]); got != want {
		t.Errorf("CLIENT REPLY pipeline replied %q; want %q", got, want)
	}
	// The suppressed writes still happened: only the acknowledgements were dropped.
	c := dialTx(t, addr)
	defer c.close()
	for _, k := range []string{"b", "c", "d"} {
		if got := c.cmd("EXISTS " + k); got != ":1" {
			t.Errorf("EXISTS %s after a suppressed write -> %q; want :1", k, got)
		}
	}
}

// readFull reads len(buf) bytes or returns what it got and the error that stopped it.
func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// TestMaxClientsRefusesAndCloses covers the connection bound: the client is told why and
// then hung up on, rather than being left to report "connection reset".
func TestMaxClientsRefusesAndCloses(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	admin := dialTx(t, addr)
	defer admin.close()
	if got := admin.cmd("CONFIG GET maxclients"); got != "[maxclients 10000]" {
		t.Errorf("default maxclients = %q; want 10000", got)
	}
	// One connection is already open (admin), so a limit of 1 refuses the next.
	if got := admin.cmd("CONFIG SET maxclients 1"); got != "+OK" {
		t.Fatalf("CONFIG SET maxclients -> %q", got)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	buf := make([]byte, 128)
	n, _ := conn.Read(buf)
	if got := string(buf[:n]); !strings.HasPrefix(got, "-ERR max number of clients reached") {
		t.Fatalf("a refused connection was told %q; want the max-clients error", got)
	}
	// And the socket is closed rather than left open: the next read ends.
	if _, err := conn.Read(buf); err == nil {
		t.Error("the refused connection was not closed")
	}

	admin.cmd("CONFIG SET maxclients 10000")
	if got := admin.cmd("INFO clients"); !strings.Contains(got, "rejected_connections:1") {
		t.Errorf("INFO clients does not report the refusal: %q", got)
	}
	if !strings.Contains(admin.cmd("INFO clients"), "maxclients:10000") {
		t.Error("INFO clients does not report maxclients")
	}
}

// TestIdleTimeoutExemptsLongLivedConnections is the half of the idle reaper that is easy to
// get wrong: a subscriber is silent *by design*, and reaping it would break the feature it
// is using.
func TestIdleTimeoutExemptsLongLivedConnections(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	admin := dialTx(t, addr)
	defer admin.close()
	if got := admin.cmd("CONFIG GET timeout"); got != "[timeout 0]" {
		t.Errorf("default timeout = %q; want 0 (off, as in Redis)", got)
	}

	sub := dialTx(t, addr)
	defer sub.close()
	if got := sub.cmd("SUBSCRIBE ch"); got != "[subscribe ch :1]" {
		t.Fatalf("SUBSCRIBE -> %q", got)
	}
	idle := dialTx(t, addr)
	defer idle.close()
	idle.cmd("PING")

	if got := admin.cmd("CONFIG SET timeout 1"); got != "+OK" {
		t.Fatalf("CONFIG SET timeout -> %q", got)
	}
	// The reaper runs once a second, so give it two rounds plus the timeout itself.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(admin.cmd("INFO clients"), "connected_clients:2") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	info := admin.cmd("INFO clients")
	if !strings.Contains(info, "connected_clients:2") {
		t.Fatalf("the idle client was not reaped: %q", info)
	}
	// The subscriber survived: it has been silent for as long as the reaped client was.
	if got := admin.cmd("PUBLISH ch hello"); got != ":1" {
		t.Errorf("PUBLISH after the reaper ran -> %q; want :1 (the subscriber must survive)", got)
	}
}

// TestLatencyHistogram covers the cumulative-histogram reply: a map of command name to
// {calls, histogram_usec}, where the counts are cumulative so any percentile can be read
// off it without the client summing anything.
func TestLatencyHistogram(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET k v")
	c.cmd("SET k v")
	// The call count is not asserted: the per-command statistics live on the command table,
	// which is process-wide, so every test that ran a SET before this one is counted in it.
	// The shape is what this checks -- and the shape is the whole point of the command.
	got := c.cmd("LATENCY HISTOGRAM set")
	if !strings.HasPrefix(got, "[set [calls :") || !strings.Contains(got, "histogram_usec [") {
		t.Errorf("LATENCY HISTOGRAM set -> %q; want a {calls, histogram_usec} map for SET", got)
	}
	// A command with no calls recorded is absent rather than reported as empty, which is
	// what lets a monitoring agent poll a fixed list.
	if got := c.cmd("LATENCY HISTOGRAM nosuchcommand"); got != "[]" {
		t.Errorf("LATENCY HISTOGRAM for an unknown command -> %q; want []", got)
	}

}

// TestDirtyChangesCounter covers INFO's rdb_changes_since_last_save: a write that changed
// nothing must not count as a change. Redis's own bitops test reads it around exactly this
// pair of SETBITs.
func TestDirtyChangesCounter(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	changes := func() string {
		for _, line := range strings.Split(c.cmd("INFO persistence"), "\n") {
			if strings.HasPrefix(line, "rdb_changes_since_last_save:") {
				return strings.TrimSpace(strings.TrimPrefix(line, "rdb_changes_since_last_save:"))
			}
		}
		t.Fatal("INFO persistence does not report rdb_changes_since_last_save")
		return ""
	}

	c.cmd("DEL foo")
	before := changes()
	// Creating the key counts, even though the bit it set was already 0: the value came into
	// existence.
	c.cmd("SETBIT foo 0 0")
	afterCreate := changes()
	if afterCreate == before {
		t.Error("creating a key with SETBIT did not count as a change")
	}
	// Setting the same bit to the same value changes nothing at all.
	c.cmd("SETBIT foo 0 0")
	if changes() != afterCreate {
		t.Error("a SETBIT that changed nothing counted as a change")
	}
	// Flipping it does, and so does growing the value without changing a bit.
	c.cmd("SETBIT foo 0 1")
	afterFlip := changes()
	if afterFlip == afterCreate {
		t.Error("flipping a bit did not count as a change")
	}
	c.cmd("SETBIT foo 90 0")
	if changes() == afterFlip {
		t.Error("growing the value did not count as a change")
	}
}
