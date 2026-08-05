package server

import "testing"

// TestExpireConditions covers the NX/XX/GT/LT flags on the whole expire family.
// The interesting cases are the asymmetric ones: a key with no TTL is treated as
// expiring infinitely far out, so GT can never fire on it while LT always does.
func TestExpireConditions(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// NX only sets a TTL on a key that has none.
		{"SET k v", "+OK"},
		{"EXPIRE k 100 NX", ":1"},
		{"EXPIRE k 200 NX", ":0"},
		{"TTL k", ":100"},
		// XX only replaces an existing TTL.
		{"EXPIRE k 200 XX", ":1"},
		{"TTL k", ":200"},
		{"SET perm v", "+OK"},
		{"EXPIRE perm 100 XX", ":0"},
		{"TTL perm", ":-1"},
		// GT only extends; LT only shortens.
		{"EXPIRE k 100 GT", ":0"},
		{"TTL k", ":200"},
		{"EXPIRE k 300 GT", ":1"},
		{"TTL k", ":300"},
		{"EXPIRE k 400 LT", ":0"},
		{"TTL k", ":300"},
		{"EXPIRE k 100 LT", ":1"},
		{"TTL k", ":100"},
		// A persistent key is infinitely far out: GT cannot beat it, LT always can.
		{"EXPIRE perm 100 GT", ":0"},
		{"TTL perm", ":-1"},
		{"EXPIRE perm 100 LT", ":1"},
		{"TTL perm", ":100"},
		// The flags work on every member of the family.
		{"PEXPIRE k 500000 GT", ":1"},
		{"TTL k", ":500"},
		{"EXPIREAT k 1 GT", ":0"},
		{"PEXPIREAT k 1 GT", ":0"},
		{"EXISTS k", ":1"},
		// Compatible combinations are accepted; incompatible ones are not.
		{"EXPIRE k 600 XX GT", ":1"},
		{"TTL k", ":600"},
		{"EXPIRE k 100 NX XX", "-ERR NX and XX, GT or LT options at the same time are not compatible"},
		{"EXPIRE k 100 NX GT", "-ERR NX and XX, GT or LT options at the same time are not compatible"},
		{"EXPIRE k 100 GT LT", "-ERR GT and LT options at the same time are not compatible"},
		{"EXPIRE k 100 ZZ", "-ERR Unsupported option ZZ"},
		{"EXPIRE k", "-ERR wrong number of arguments for 'expire' command"},
		// A missing key is still 0, flag or not.
		{"EXPIRE ghost 100 NX", ":0"},
		{"EXPIRE ghost 100", ":0"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestCopyAndRenameNX covers the two-key writes over the keyspace, including the
// cases that must not touch the destination at all.
func TestCopyAndRenameNX(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SET src v1", "+OK"},
		{"COPY src dst", ":1"},
		{"GET dst", "v1"},
		{"GET src", "v1"},
		// An existing destination is refused unless REPLACE is given.
		{"SET dst other", "+OK"},
		{"COPY src dst", ":0"},
		{"GET dst", "other"},
		{"COPY src dst REPLACE", ":1"},
		{"GET dst", "v1"},
		// A missing source copies nothing.
		{"COPY ghost dst2", ":0"},
		{"EXISTS dst2", ":0"},
		// One database only: DB 0 is a no-op, anything else is out of range.
		{"COPY src db0 DB 0", ":1"},
		{"GET db0", "v1"},
		// COPY into another database is a real copy now that there are 16 of them; the
		// isolation and the cross-database form are covered in databases_test.go.
		{"COPY src dst9 DB 99", "-ERR DB index is out of range"},
		{"COPY src dst9 DB", "-ERR syntax error"},
		{"COPY src dst9 BOGUS", "-ERR syntax error"},
		{"COPY src src", "-ERR source and destination objects are the same"},
		{"COPY src", "-ERR wrong number of arguments for 'copy' command"},
		// The TTL travels with the value.
		{"SETEX vol 100 v", "+OK"},
		{"COPY vol volcopy", ":1"},
		{"TTL volcopy", ":100"},
		// RENAMENX refuses an existing destination and errors on a missing source.
		{"SET a 1", "+OK"},
		{"SET b 2", "+OK"},
		{"RENAMENX a b", ":0"},
		{"GET b", "2"},
		{"RENAMENX a c", ":1"},
		{"EXISTS a", ":0"},
		{"GET c", "1"},
		{"RENAMENX ghost x", "-ERR no such key"},
		{"RENAMENX b b", ":0"},
		{"GET b", "2"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A copied collection is independent of its source, not an alias to it.
	c.cmd("RPUSH list a b c")
	if got := c.cmd("COPY list listcopy"); got != ":1" {
		t.Fatalf("COPY of a list = %q; want :1", got)
	}
	c.cmd("RPUSH list d")
	if got := c.cmd("LRANGE listcopy 0 -1"); got != "[a b c]" {
		t.Errorf("copy changed with its source: %q; want [a b c]", got)
	}
	c.cmd("ZADD z 1 one 2 two")
	c.cmd("COPY z zcopy")
	c.cmd("ZADD z 3 three")
	if got := c.cmd("ZRANGE zcopy 0 -1 WITHSCORES"); got != "[one 1 two 2]" {
		t.Errorf("zset copy = %q; want [one 1 two 2]", got)
	}
}

// TestUnlinkTouchAndRandomKey covers the remaining keyspace commands.
func TestUnlinkTouchAndRandomKey(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"RANDOMKEY", "(nil)"}, // empty keyspace
		{"MSET a 1 b 2 c 3", "+OK"},
		{"TOUCH a b missing", ":2"},
		{"TOUCH missing", ":0"},
		{"TOUCH", "-ERR wrong number of arguments for 'touch' command"},
		{"UNLINK a b missing", ":2"},
		{"EXISTS a b", ":0"},
		{"UNLINK missing", ":0"},
		{"UNLINK", "-ERR wrong number of arguments for 'unlink' command"},
		{"RANDOMKEY", "c"}, // the only key left
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestObjectCommand covers OBJECT's three subcommands. ENCODING reports the name
// Redis would use for a value of that shape, which is what a client inspects; the
// thresholds, not the internals, are what it is promising.
func TestObjectCommand(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	longVal := ""
	for i := 0; i < 50; i++ {
		longVal += "x"
	}

	cases := []struct{ cmd, want string }{
		{"SET n 12345", "+OK"},
		{"OBJECT ENCODING n", "int"},
		{"SET short hello", "+OK"},
		{"OBJECT ENCODING short", "embstr"},
		{"SET long " + longVal, "+OK"},
		{"OBJECT ENCODING long", "raw"},
		{"RPUSH l a b c", ":3"},
		{"OBJECT ENCODING l", "listpack"},
		{"HSET h f v", ":1"},
		{"OBJECT ENCODING h", "listpack"},
		{"SADD ints 1 2 3", ":3"},
		{"OBJECT ENCODING ints", "intset"},
		{"SADD strs a b", ":2"},
		{"OBJECT ENCODING strs", "listpack"},
		{"ZADD z 1 one", ":1"},
		{"OBJECT ENCODING z", "listpack"},
		{"OBJECT REFCOUNT n", ":1"},
		{"OBJECT IDLETIME n", ":0"},
		// Missing keys and malformed invocations.
		{"OBJECT ENCODING ghost", "-ERR no such key"},
		{"OBJECT REFCOUNT ghost", "-ERR no such key"},
		{"OBJECT IDLETIME ghost", "-ERR no such key"},
		{"OBJECT BOGUS n", "-ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try OBJECT HELP."},
		{"OBJECT ENCODING", "-ERR Unknown subcommand or wrong number of arguments for 'ENCODING'. Try OBJECT HELP."},
		{"OBJECT", "-ERR wrong number of arguments for 'object' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}
