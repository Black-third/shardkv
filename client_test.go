package shardkv_test

import (
	"bufio"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Black-third/shardkv"
)

// TestDoReturnsDecodedValues pins the mapping Do documents. Each case is a command whose
// reply is a different RESP type, because the type is what the caller has to assert on.
func TestDoReturnsDecodedValues(t *testing.T) {
	db := open(t, shardkv.Options{})
	mustOK(t, db, "SET", "str", "hello")
	mustOK(t, db, "RPUSH", "list", "a", "b")
	mustOK(t, db, "HSET", "hash", "f", "1")

	for _, tc := range []struct {
		name string
		args []string
		want any
	}{
		{"simple string", []string{"PING"}, "PONG"},
		{"bulk string", []string{"GET", "str"}, "hello"},
		{"integer", []string{"STRLEN", "str"}, int64(5)},
		{"null for a missing key", []string{"GET", "absent"}, nil},
		{"null array for a missing collection", []string{"LPOP", "absent", "2"}, nil},
		// A RESP2 connection receives a double as the text of a bulk string, which is what
		// every RESP2 client already parses a score out of.
		{"double as text under RESP2", []string{"INCRBYFLOAT", "float", "1.5"}, "1.5"},
	} {
		got, err := db.Do(tc.args...)
		if err != nil {
			t.Errorf("%s: %v: %v", tc.name, tc.args, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: %v = %#v; want %#v", tc.name, tc.args, got, tc.want)
		}
	}

	// The two collection shapes, which cannot be compared with ==.
	got, err := db.Do("LRANGE", "list", "0", "-1")
	if err != nil {
		t.Fatalf("LRANGE: %v", err)
	}
	arr, ok := got.([]any)
	if !ok || len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Errorf("LRANGE = %#v; want []any{\"a\", \"b\"}", got)
	}
	// Under RESP2 a hash is a flat array, not a map. That is the shape the protocol
	// specifies, and Do reports it rather than tidying it up -- Map is where the tidying
	// lives, so a caller that wants the raw reply gets the raw reply.
	got, err = db.Do("HGETALL", "hash")
	if err != nil {
		t.Fatalf("HGETALL: %v", err)
	}
	if arr, ok = got.([]any); !ok || len(arr) != 2 {
		t.Errorf("HGETALL under RESP2 = %#v; want a flat 2-element array", got)
	}
}

// TestErrorRepliesBecomeErrors checks the top-level convention: what the server refused is
// err, with the server's own sentence, so a caller can match on the prefix Redis clients
// match on.
func TestErrorRepliesBecomeErrors(t *testing.T) {
	db := open(t, shardkv.Options{})
	mustOK(t, db, "SET", "str", "hello")

	for _, tc := range []struct {
		args   []string
		prefix string
	}{
		{[]string{"LPUSH", "str", "x"}, "WRONGTYPE"},
		{[]string{"INCR", "str"}, "ERR value is not an integer"},
		{[]string{"GET"}, "ERR wrong number of arguments"},
		{[]string{"NOSUCHCOMMAND"}, "ERR unknown command"},
		{[]string{"EXPIRE", "str", "not-a-number"}, "ERR value is not an integer"},
	} {
		got, err := db.Do(tc.args...)
		if err == nil {
			t.Errorf("%v = %#v; want an error", tc.args, got)
			continue
		}
		if !strings.HasPrefix(err.Error(), tc.prefix) {
			t.Errorf("%v = %q; want a prefix of %q", tc.args, err, tc.prefix)
		}
		if got != nil {
			t.Errorf("%v returned both %#v and %v; the value must be nil on an error", tc.args, got, err)
		}
	}
}

// TestExecCarriesPerCommandErrorsAsValues is why an error reply is decoded as a value and
// only converted to Go's error at the top level.
//
// EXEC runs every queued command whether or not an earlier one failed, so its reply is an
// array in which any element may be an error *and the ones that succeeded still have
// results*. A decoder that turned the first error into its own error return would have to
// abandon the array and throw those results away.
func TestExecCarriesPerCommandErrorsAsValues(t *testing.T) {
	db := open(t, shardkv.Options{})
	mustOK(t, db, "SET", "str", "hello")
	mustOK(t, db, "MULTI")
	for _, args := range [][]string{
		{"INCR", "counter"},
		{"LPUSH", "str", "x"}, // WRONGTYPE at execution time, so it queues and then fails
		{"INCR", "counter"},
	} {
		if _, err := db.Do(args...); err != nil {
			t.Fatalf("queueing %v: %v", args, err)
		}
	}
	got, err := db.Do("EXEC")
	if err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	results, ok := got.([]any)
	if !ok || len(results) != 3 {
		t.Fatalf("EXEC = %#v; want a 3-element array", got)
	}
	if results[0] != int64(1) {
		t.Errorf("EXEC[0] = %#v; want 1", results[0])
	}
	// The failed element implements error, which is the only type a caller needs to know:
	// it never has to name the concrete type, so the decoder's type is not part of this
	// package's contract.
	failed, isErr := results[1].(error)
	if !isErr {
		t.Fatalf("EXEC[1] = %#v (%T); want a value implementing error", results[1], results[1])
	}
	if !strings.HasPrefix(failed.Error(), "WRONGTYPE") {
		t.Errorf("EXEC[1] = %q; want a WRONGTYPE error", failed)
	}
	// And the command after the failure still ran, which is the whole point.
	if results[2] != int64(2) {
		t.Errorf("EXEC[2] = %#v; want 2 -- the command after the failure must still have run",
			results[2])
	}
}

// TestTypedAccessors covers the conversion layer: one method per reply shape rather than
// one per command, so the same eight methods serve every command with that shape.
func TestTypedAccessors(t *testing.T) {
	db := open(t, shardkv.Options{})

	if err := db.OK("SET", "str", "hello"); err != nil {
		t.Fatalf("OK: %v", err)
	}
	if err := db.OK("SET"); err == nil {
		t.Error("OK on a command with a wrong arity reported no error")
	}

	if n, err := db.Int("STRLEN", "str"); err != nil || n != 5 {
		t.Errorf("Int(STRLEN) = %d, %v; want 5, nil", n, err)
	}
	if n, err := db.Int("EXISTS", "str"); err != nil || n != 1 {
		t.Errorf("Int(EXISTS) = %d, %v; want 1, nil", n, err)
	}

	if f, err := db.Float("ZADD", "z", "1.5", "m"); err != nil || f != 1 {
		t.Errorf("Float(ZADD) = %v, %v; want 1, nil", f, err)
	}
	if f, err := db.Float("ZSCORE", "z", "m"); err != nil || f != 1.5 {
		t.Errorf("Float(ZSCORE) = %v, %v; want 1.5, nil", f, err)
	}
	// The infinities a sorted set can really hold, spelled as Redis spells them rather
	// than in Go's syntax -- which is why Float does not use strconv.ParseFloat.
	if _, err := db.Do("ZADD", "z", "inf", "top"); err != nil {
		t.Fatalf("ZADD inf: %v", err)
	}
	if f, err := db.Float("ZSCORE", "z", "top"); err != nil || !math.IsInf(f, 1) {
		t.Errorf("Float(ZSCORE) of an infinite score = %v, %v; want +Inf, nil", f, err)
	}

	if b, err := db.Bool("SISMEMBER", "set", "x"); err != nil || b {
		t.Errorf("Bool(SISMEMBER) on a missing set = %v, %v; want false, nil", b, err)
	}
	if _, err := db.Int("SADD", "set", "x"); err != nil {
		t.Fatalf("SADD: %v", err)
	}
	if b, err := db.Bool("SISMEMBER", "set", "x"); err != nil || !b {
		t.Errorf("Bool(SISMEMBER) = %v, %v; want true, nil", b, err)
	}

	// The exists distinction, which is the reason Bytes has three results: an absent key
	// and a key holding "" are different states and the string "" cannot tell them apart.
	if err := db.OK("SET", "empty", ""); err != nil {
		t.Fatalf("SET to an empty value: %v", err)
	}
	v, ok, err := db.Bytes("GET", "empty")
	if err != nil || !ok || len(v) != 0 {
		t.Errorf("Bytes(GET) of an empty value = %q, %v, %v; want \"\", true, nil", v, ok, err)
	}
	v, ok, err = db.Bytes("GET", "absent")
	if err != nil || ok || v != nil {
		t.Errorf("Bytes(GET) of a missing key = %q, %v, %v; want nil, false, nil", v, ok, err)
	}

	if _, err := db.Int("RPUSH", "list", "a", "b", "c"); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	items, err := db.Strings("LRANGE", "list", "0", "-1")
	if err != nil || strings.Join(items, ",") != "a,b,c" {
		t.Errorf("Strings(LRANGE) = %v, %v; want [a b c]", items, err)
	}
	// A set's members come back as the same slice a list's do, so the caller's code does
	// not depend on which protocol version is in force.
	members, err := db.Strings("SMEMBERS", "set")
	if err != nil || len(members) != 1 || members[0] != "x" {
		t.Errorf("Strings(SMEMBERS) = %v, %v; want [x]", members, err)
	}

	if _, err := db.Int("HSET", "hash", "f", "1", "g", "2"); err != nil {
		t.Fatalf("HSET: %v", err)
	}
	fields, err := db.Map("HGETALL", "hash")
	if err != nil || len(fields) != 2 || fields["f"] != "1" || fields["g"] != "2" {
		t.Errorf("Map(HGETALL) = %v, %v; want {f:1 g:2}", fields, err)
	}

	// An accessor asked for a shape the reply does not have says so, naming the command
	// and the type that arrived, rather than returning a zero value.
	if _, err := db.Int("GET", "str"); err == nil {
		t.Error("Int on a non-numeric bulk reply reported no error")
	} else if !strings.Contains(err.Error(), "get") && !strings.Contains(err.Error(), "GET") {
		t.Errorf("the type error does not name the command: %v", err)
	}
	if _, err := db.Strings("GET", "str"); err == nil {
		t.Error("Strings on a scalar reply reported no error")
	}
}

// TestAccessorsReadBothProtocols is the reason each accessor absorbs the RESP2/RESP3
// difference: a caller that sends HELLO 3 -- an ordinary command, so no API is needed for
// it -- must not have to change its own code.
//
// HGETALL and ZSCORE are the two cases that matter, because they are among the few replies
// RESP3 reshaped rather than merely retagged: a map instead of a flat array, and a double
// instead of the text of one.
func TestAccessorsReadBothProtocols(t *testing.T) {
	db := open(t, shardkv.Options{})
	mustOK(t, db, "HSET", "hash", "f", "1", "g", "2")
	mustOK(t, db, "ZADD", "z", "1.5", "m")

	read := func(t *testing.T, c *shardkv.Client) {
		t.Helper()
		fields, err := c.Map("HGETALL", "hash")
		if err != nil || len(fields) != 2 || fields["f"] != "1" || fields["g"] != "2" {
			t.Errorf("Map(HGETALL) = %v, %v; want {f:1 g:2}", fields, err)
		}
		if f, err := c.Float("ZSCORE", "z", "m"); err != nil || f != 1.5 {
			t.Errorf("Float(ZSCORE) = %v, %v; want 1.5, nil", f, err)
		}
	}

	t.Run("RESP2", func(t *testing.T) { read(t, db.Client) })

	t.Run("RESP3", func(t *testing.T) {
		c := db.NewClient()
		defer c.Close()
		if _, err := c.Do("HELLO", "3"); err != nil {
			t.Fatalf("HELLO 3: %v", err)
		}
		// The reply shapes really did change, or the test above would prove nothing.
		got, err := c.Do("HGETALL", "hash")
		if err != nil {
			t.Fatalf("HGETALL: %v", err)
		}
		if _, isMap := got.(map[string]any); !isMap {
			t.Fatalf("under RESP3, HGETALL = %#v (%T); want a map", got, got)
		}
		if got, err = c.Do("ZSCORE", "z", "m"); err != nil {
			t.Fatalf("ZSCORE: %v", err)
		}
		if _, isFloat := got.(float64); !isFloat {
			t.Fatalf("under RESP3, ZSCORE = %#v (%T); want a float64", got, got)
		}
		read(t, c)
	})
}

// TestClientsAreIndependent checks that a Client really is a connection's worth of state
// and not a shared handle. Both cases below would be silent corruption if they leaked: one
// caller's SELECT moving another's keyspace, or one caller's MULTI swallowing another's
// commands.
func TestClientsAreIndependent(t *testing.T) {
	db := open(t, shardkv.Options{})
	other := db.NewClient()
	defer other.Close()

	// A SELECT belongs to one client.
	if err := other.OK("SELECT", "3"); err != nil {
		t.Fatalf("SELECT 3: %v", err)
	}
	if err := other.OK("SET", "scoped", "in-db-3"); err != nil {
		t.Fatalf("SET in database 3: %v", err)
	}
	if _, ok, err := db.Bytes("GET", "scoped"); err != nil || ok {
		t.Errorf("the default client sees database 3's key: ok=%v err=%v", ok, err)
	}

	// A MULTI belongs to one client: the other client's command must run immediately
	// rather than joining the open transaction.
	if err := other.OK("MULTI"); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if _, err := other.Do("INCR", "queued-counter"); err != nil {
		t.Fatalf("queueing INCR: %v", err)
	}
	if n, err := db.Int("INCR", "immediate-counter"); err != nil || n != 1 {
		t.Fatalf("the other client's INCR = %d, %v; it must not have been queued", n, err)
	}
	if _, err := other.Do("EXEC"); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
}

// TestConcurrentClientsAreRaceFree exercises the concurrency contract under the race
// detector: many clients in parallel, and the shared default client from several
// goroutines at once. The second half is the one worth having -- the default client
// serializes its callers rather than letting them interleave a session's state.
func TestConcurrentClientsAreRaceFree(t *testing.T) {
	db := open(t, shardkv.Options{})

	const goroutines, each = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			c := db.NewClient()
			defer c.Close()
			for i := 0; i < each; i++ {
				key := "own:" + strconv.Itoa(g)
				if _, err := c.Int("INCR", key); err != nil {
					t.Errorf("INCR %s: %v", key, err)
					return
				}
				// And the shared client, from every goroutine at once.
				if _, err := db.Int("INCR", "shared"); err != nil {
					t.Errorf("INCR shared: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if n, err := db.Int("GET", "shared"); err != nil || n != goroutines*each {
		t.Errorf("GET shared = %d, %v; want %d -- a lost increment means the default "+
			"client did not serialize its callers", n, err, goroutines*each)
	}
	for g := 0; g < goroutines; g++ {
		key := "own:" + strconv.Itoa(g)
		if n, err := db.Int("GET", key); err != nil || n != each {
			t.Errorf("GET %s = %d, %v; want %d", key, n, err, each)
		}
	}
}

// TestClientCloseIsIdempotent covers what a defer needs, and checks that closing one
// client leaves the DB and its other clients working.
func TestClientCloseIsIdempotent(t *testing.T) {
	db := open(t, shardkv.Options{})
	c := db.NewClient()
	if err := c.OK("SET", "k", "v"); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("the second Close: %v", err)
	}
	if _, ok, err := db.Bytes("GET", "k"); err != nil || !ok {
		t.Errorf("closing one client lost the dataset: ok=%v err=%v", ok, err)
	}
}

// TestEmptyCommandIsRejected checks the one argument error the facade itself produces,
// rather than passing an empty command to a server that has nothing to look up.
func TestEmptyCommandIsRejected(t *testing.T) {
	db := open(t, shardkv.Options{})
	if _, err := db.Do(); err == nil {
		t.Error("Do with no arguments reported no error")
	}
	if err := db.OK(); err == nil {
		t.Error("OK with no arguments reported no error")
	}
}

// TestSuppressedReplyIsNotAnError covers CLIENT REPLY, which withholds a reply rather than
// failing. It works here exactly as it does on a socket, and a caller must be able to tell
// "nothing was said" from "something went wrong".
func TestSuppressedReplyIsNotAnError(t *testing.T) {
	db := open(t, shardkv.Options{})
	c := db.NewClient()
	defer c.Close()

	if _, err := c.Do("CLIENT", "REPLY", "OFF"); err != nil {
		t.Fatalf("CLIENT REPLY OFF: %v", err)
	}
	got, err := c.Do("SET", "quiet", "1")
	if err != nil {
		t.Fatalf("SET with replies off: %v", err)
	}
	if got != nil {
		t.Errorf("SET with replies off = %#v; want nil", got)
	}
	if _, err = c.Do("CLIENT", "REPLY", "ON"); err != nil {
		t.Fatalf("CLIENT REPLY ON: %v", err)
	}
	// The suppressed write really happened; only its reply was dropped.
	if v, ok, err := c.Bytes("GET", "quiet"); err != nil || !ok || string(v) != "1" {
		t.Errorf("GET quiet = %q, %v, %v; want \"1\", true, nil", v, ok, err)
	}
}

// TestBinaryValuesRoundTrip checks the claim Do makes about its arguments: a Go string may
// hold arbitrary bytes, so nothing about the API needs a separate binary form.
func TestBinaryValuesRoundTrip(t *testing.T) {
	db := open(t, shardkv.Options{})
	value := string([]byte{0x00, 0xff, '\r', '\n', 0x80, 'a'})
	if err := db.OK("SET", "binary", value); err != nil {
		t.Fatalf("SET: %v", err)
	}
	got, ok, err := db.Bytes("GET", "binary")
	if err != nil || !ok {
		t.Fatalf("GET binary = %v, %v", ok, err)
	}
	if string(got) != value {
		t.Errorf("GET binary = %x; want %x", got, value)
	}
	if n, err := db.Int("STRLEN", "binary"); err != nil || n != int64(len(value)) {
		t.Errorf("STRLEN = %d, %v; want %d", n, err, len(value))
	}
}

func mustOK(t *testing.T, db *shardkv.DB, args ...string) {
	t.Helper()
	if _, err := db.Do(args...); err != nil {
		t.Fatalf("%v: %v", args, err)
	}
}

// BenchmarkDo and BenchmarkDoOverTCP are the pair the package's central claim rests on:
// the in-process client exists to remove a round trip, so the two numbers have to be
// measured against each other and not asserted.
//
// Both run the same command through the same server. The only difference is whether the
// bytes cross a socket.
func BenchmarkDo(b *testing.B) {
	db, err := shardkv.Open(shardkv.Options{})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := db.OK("SET", "k", "v"); err != nil {
			b.Fatalf("SET: %v", err)
		}
	}
}

func BenchmarkDoOverTCP(b *testing.B) {
	db, err := shardkv.Open(shardkv.Options{Addr: "127.0.0.1:0"})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	conn, err := net.Dial("tcp", db.Addr().String())
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	bw := bufio.NewWriter(conn)
	br := bufio.NewReader(conn)
	request := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := bw.WriteString(request); err != nil {
			b.Fatalf("write: %v", err)
		}
		if err := bw.Flush(); err != nil {
			b.Fatalf("flush: %v", err)
		}
		line, err := br.ReadString('\n')
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		if !strings.HasPrefix(line, "+OK") {
			b.Fatalf("SET answered %q", line)
		}
	}
}

// BenchmarkDoGet is the read side, since a read's reply is a bulk string and so pays the
// decode a status reply does not.
func BenchmarkDoGet(b *testing.B) {
	db, err := shardkv.Open(shardkv.Options{})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if err := db.OK("SET", "k", "v"); err != nil {
		b.Fatalf("SET: %v", err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, ok, err := db.Bytes("GET", "k")
		if err != nil || !ok || len(v) != 1 {
			b.Fatalf("GET = %q, %v, %v", v, ok, err)
		}
	}
}
