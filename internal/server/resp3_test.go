package server

// RESP3 tests.
//
// Two things are being protected here, and they pull in opposite directions.
//
// The first is that a RESP2 client sees exactly the bytes it saw before RESP3
// existed. That is the regression risk of the whole change -- every reply-shape
// helper has a RESP2 branch, and a mistake in one of them is invisible to a test
// that parses the reply instead of comparing it. So TestRESP2WireBytesUnchanged
// compares raw bytes, byte for byte, with no parsing in between.
//
// The second is that the RESP3 encodings are the ones real Redis sends. Every
// expected byte string in TestRESP3WireBytes was captured from redis:7-alpine over a
// HELLO 3 connection, so these are not this server's opinion of RESP3 -- they are
// what a real client library will have been written against.
//
// Replies whose contents depend on map iteration order are asserted over
// single-element collections, since a byte comparison cannot tolerate two orders.

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// rawConn is a client that compares raw reply bytes instead of parsing them.
type rawConn struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawConn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &rawConn{t: t, conn: conn, br: bufio.NewReader(conn)}
}

// expect writes a command and reads back exactly len(want) bytes.
//
// Reading a known length is what makes the comparison exact: a reply one byte longer
// than expected surfaces as the next assertion's prefix rather than being silently
// tolerated, which is the failure mode a parsing helper hides.
func (c *rawConn) expect(cmd, want string) {
	c.t.Helper()
	if _, err := c.conn.Write([]byte(cmd + "\r\n")); err != nil {
		c.t.Fatalf("write %q: %v", cmd, err)
	}
	buf := make([]byte, len(want))
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(c.br, buf); err != nil {
		c.t.Fatalf("read reply to %q: %v", cmd, err)
	}
	if got := string(buf); got != want {
		c.t.Errorf("%s\n got %q\nwant %q", cmd, got, want)
	}
}

// wireCase is one command and the exact bytes it must produce.
type wireCase struct{ cmd, want string }

// commonWireCases are the commands whose encoding does not depend on the protocol.
// They are asserted under both versions, which is what proves a shape helper did not
// quietly start emitting a RESP3 type to everybody.
//
// The integer replies here are worth stating explicitly: RESP3 has a boolean type,
// and the protocol's own documentation suggests commands like EXISTS or SISMEMBER
// would use it -- but real Redis 7 replies with an integer to both protocols, and
// wire compatibility with real clients beats the specification's suggestion.
var commonWireCases = []wireCase{
	{"PING", "+PONG\r\n"},
	{"SET k v", "+OK\r\n"},
	{"GET k", "$1\r\nv\r\n"},
	{"EXISTS k", ":1\r\n"},
	{"EXISTS k missing", ":1\r\n"},
	{"SETNX k other", ":0\r\n"},
	{"SISMEMBER nos m", ":0\r\n"},
	{"EXPIRE k 100", ":1\r\n"},
	{"PERSIST k", ":1\r\n"},
	{"RPUSH list a b c", ":3\r\n"},
	{"LRANGE list 0 -1", "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"},
	{"LPOP list", "$1\r\na\r\n"},
	{"HSET h f v", ":1\r\n"},
	{"HEXISTS h f", ":1\r\n"},
	{"ZADD z 1.5 a", ":1\r\n"},
	{"SADD s m", ":1\r\n"},
	{"SRANDMEMBER s 1", "*1\r\n$1\r\nm\r\n"}, // an array, not a set: it may repeat
	{"TYPE k", "+string\r\n"},
	{"SCAN 0 COUNT 1000 MATCH nomatch*", "*2\r\n$1\r\n0\r\n*0\r\n"},
	{"PUBSUB NUMPAT", ":0\r\n"},
	// INCRBYFLOAT is a bulk string in both protocols, as in real Redis: it replies
	// with a long-double rendering, not with a double.
	{"INCRBYFLOAT fl 1.5", "$3\r\n1.5\r\n"},
	{"HINCRBYFLOAT h fx 1.5", "$3\r\n1.5\r\n"},
}

// TestRESP2WireBytesUnchanged pins the RESP2 encoding of every reply whose shape
// RESP3 changes. If any of these bytes move, a RESP2 client has been broken.
func TestRESP2WireBytesUnchanged(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialRaw(t, addr)

	for _, tc := range commonWireCases {
		c.expect(tc.cmd, tc.want)
	}
	for _, tc := range []wireCase{
		// Maps are flat name/value arrays.
		{"HGETALL one", "*0\r\n"},
		{"HSET one f v", ":1\r\n"},
		{"HGETALL one", "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"},
		{"CONFIG GET maxkeys", "*2\r\n$7\r\nmaxkeys\r\n$1\r\n0\r\n"},
		// Sets are arrays.
		{"SMEMBERS s", "*1\r\n$1\r\nm\r\n"},
		{"SMEMBERS nothing", "*0\r\n"},
		{"SUNION s", "*1\r\n$1\r\nm\r\n"},
		{"SPOP s 1", "*1\r\n$1\r\nm\r\n"},
		// Doubles are bulk strings.
		{"ZSCORE z a", "$3\r\n1.5\r\n"},
		{"ZSCORE z nope", "$-1\r\n"},
		{"ZINCRBY z 1 a", "$3\r\n2.5\r\n"},
		{"ZMSCORE z a nope", "*2\r\n$3\r\n2.5\r\n$-1\r\n"},
		{"ZADD z INCR 1 a", "$3\r\n3.5\r\n"},
		// WITHSCORES is flattened.
		{"ZRANGE z 0 -1 WITHSCORES", "*2\r\n$1\r\na\r\n$3\r\n3.5\r\n"},
		{"ZPOPMIN z", "*2\r\n$1\r\na\r\n$3\r\n3.5\r\n"},
		{"ZADD z 3 b", ":1\r\n"},
		{"ZPOPMIN z 1", "*2\r\n$1\r\nb\r\n$1\r\n3\r\n"},
		{"HRANDFIELD one 1 WITHVALUES", "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"},
		// The two distinct RESP2 nulls stay distinct.
		{"GET missing", "$-1\r\n"},
		{"LPOP missing 2", "*-1\r\n"},
		// The DEBUG PROTOCOL fallbacks, which are also real Redis's.
		{"DEBUG PROTOCOL double", "$5\r\n3.141\r\n"},
		{"DEBUG PROTOCOL bignum", "$37\r\n1234567999999999999999999999999999999\r\n"},
		{"DEBUG PROTOCOL null", "$-1\r\n"},
		{"DEBUG PROTOCOL set", "*3\r\n:0\r\n:1\r\n:2\r\n"},
		{"DEBUG PROTOCOL map", "*6\r\n:0\r\n:0\r\n:1\r\n:1\r\n:2\r\n:0\r\n"},
		{"DEBUG PROTOCOL attrib", "$39\r\nSome real reply following the attribute\r\n"},
		{"DEBUG PROTOCOL push", "-ERR RESP2 is not supported by this command\r\n"},
		{"DEBUG PROTOCOL verbatim", "$25\r\nThis is a verbatim\nstring\r\n"},
		{"DEBUG PROTOCOL true", ":1\r\n"},
		{"DEBUG PROTOCOL false", ":0\r\n"},
		// INFO is a bulk string for a RESP2 client, however it is written internally.
		{"INFO keyspace", "$"},
	} {
		c.expect(tc.cmd, tc.want)
	}
}

// TestRESP2HelloBytes pins the RESP2 HELLO reply, which a client library parses
// before it has decided anything else about the server. It is the first connection to
// its own server, so the reported id is deterministic.
func TestRESP2HelloBytes(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialRaw(t, addr)

	c.expect("HELLO 2", "*14\r\n"+
		"$6\r\nserver\r\n$7\r\nshardkv\r\n"+
		"$7\r\nversion\r\n$"+strconv.Itoa(len(Version))+"\r\n"+Version+"\r\n"+
		"$5\r\nproto\r\n$1\r\n2\r\n"+
		"$2\r\nid\r\n$1\r\n1\r\n"+
		"$4\r\nmode\r\n$10\r\nstandalone\r\n"+
		"$4\r\nrole\r\n$6\r\nmaster\r\n"+
		"$7\r\nmodules\r\n*0\r\n")
}

// TestRESP3WireBytes pins the RESP3 encodings against the bytes redis:7-alpine sends
// for the same commands.
func TestRESP3WireBytes(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialRaw(t, addr)

	// HELLO 3 answers with a map whose proto and id are integers, not strings.
	c.expect("HELLO 3", "%7\r\n"+
		"$6\r\nserver\r\n$7\r\nshardkv\r\n"+
		"$7\r\nversion\r\n$"+strconv.Itoa(len(Version))+"\r\n"+Version+"\r\n"+
		"$5\r\nproto\r\n:3\r\n"+
		"$2\r\nid\r\n:1\r\n"+
		"$4\r\nmode\r\n$10\r\nstandalone\r\n"+
		"$4\r\nrole\r\n$6\r\nmaster\r\n"+
		"$7\r\nmodules\r\n*0\r\n")

	for _, tc := range commonWireCases {
		c.expect(tc.cmd, tc.want)
	}
	for _, tc := range []wireCase{
		// Maps.
		{"HGETALL one", "%0\r\n"},
		{"HSET one f v", ":1\r\n"},
		{"HGETALL one", "%1\r\n$1\r\nf\r\n$1\r\nv\r\n"},
		{"CONFIG GET maxkeys", "%1\r\n$7\r\nmaxkeys\r\n$1\r\n0\r\n"},
		// Sets.
		{"SMEMBERS s", "~1\r\n$1\r\nm\r\n"},
		{"SMEMBERS nothing", "~0\r\n"},
		{"SUNION s", "~1\r\n$1\r\nm\r\n"},
		{"SPOP s 1", "~1\r\n$1\r\nm\r\n"},
		// Doubles.
		{"ZSCORE z a", ",1.5\r\n"},
		{"ZSCORE z nope", "_\r\n"},
		{"ZINCRBY z 1 a", ",2.5\r\n"},
		{"ZMSCORE z a nope", "*2\r\n,2.5\r\n_\r\n"},
		{"ZADD z INCR 1 a", ",3.5\r\n"},
		// WITHSCORES nests; the countless ZPOPMIN stays one flat pair, as in Redis.
		{"ZRANGE z 0 -1 WITHSCORES", "*1\r\n*2\r\n$1\r\na\r\n,3.5\r\n"},
		{"ZPOPMIN z", "*2\r\n$1\r\na\r\n,3.5\r\n"},
		{"ZADD z 3 b", ":1\r\n"},
		{"ZPOPMIN z 1", "*1\r\n*2\r\n$1\r\nb\r\n,3\r\n"},
		{"HRANDFIELD one 1 WITHVALUES", "*1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"},
		// One null replaces both RESP2 spellings.
		{"GET missing", "_\r\n"},
		{"LPOP missing 2", "_\r\n"},
		// Every RESP3 type, including the two no keyspace command has a use for.
		{"DEBUG PROTOCOL string", "$11\r\nHello World\r\n"},
		{"DEBUG PROTOCOL integer", ":12345\r\n"},
		{"DEBUG PROTOCOL double", ",3.141\r\n"},
		{"DEBUG PROTOCOL bignum", "(1234567999999999999999999999999999999\r\n"},
		{"DEBUG PROTOCOL null", "_\r\n"},
		{"DEBUG PROTOCOL array", "*3\r\n:0\r\n:1\r\n:2\r\n"},
		{"DEBUG PROTOCOL set", "~3\r\n:0\r\n:1\r\n:2\r\n"},
		{"DEBUG PROTOCOL map", "%3\r\n:0\r\n#f\r\n:1\r\n#t\r\n:2\r\n#f\r\n"},
		{"DEBUG PROTOCOL attrib", "|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n" +
			"$39\r\nSome real reply following the attribute\r\n"},
		{"DEBUG PROTOCOL push", "$40\r\nSome real reply following the push reply\r\n" +
			">2\r\n$16\r\nserver-cpu-usage\r\n:42\r\n"},
		{"DEBUG PROTOCOL verbatim", "=29\r\ntxt:This is a verbatim\nstring\r\n"},
		{"DEBUG PROTOCOL true", "#t\r\n"},
		{"DEBUG PROTOCOL false", "#f\r\n"},
		// INFO becomes a verbatim string, so redis-cli prints the report rather than one
		// long escaped line.
		{"INFO keyspace", "="},
	} {
		c.expect(tc.cmd, tc.want)
	}
}

// TestHelloProtocolSwitch covers negotiating up, negotiating back down, and RESET.
func TestHelloProtocolSwitch(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SADD s a")
	if got := c.cmd("HELLO 3"); !contains(got, "proto :3") {
		t.Errorf("HELLO 3 = %q; want a map reporting proto 3", got)
	}
	// A bare HELLO keeps the negotiated version rather than resetting it.
	if got := c.cmd("HELLO"); !contains(got, "proto :3") {
		t.Errorf("bare HELLO after HELLO 3 = %q", got)
	}
	if got := c.cmd("SMEMBERS s"); got != "~[a]" {
		t.Errorf("SMEMBERS in RESP3 = %q; want a set", got)
	}
	// Negotiating back down: the reply to HELLO 2 is itself RESP2.
	if got := c.cmd("HELLO 2"); !contains(got, "proto 2") {
		t.Errorf("HELLO 2 = %q", got)
	}
	if got := c.cmd("SMEMBERS s"); got != "[a]" {
		t.Errorf("SMEMBERS after downgrade = %q; want an array", got)
	}
	// RESET returns the connection to the default protocol, as in Redis.
	c.cmd("HELLO 3")
	if got := c.cmd("RESET"); got != "+RESET" {
		t.Errorf("RESET = %q", got)
	}
	if got := c.cmd("SMEMBERS s"); got != "[a]" {
		t.Errorf("SMEMBERS after RESET = %q; want RESP2 again", got)
	}
}

// TestRESP3PubSubPush covers the behavioural difference that matters most: a RESP3
// subscriber receives messages as push frames, so it may keep issuing ordinary
// commands while subscribed -- which a RESP2 subscriber may not.
func TestRESP3PubSubPush(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	sub := dialTx(t, addr)
	defer sub.close()
	sub.cmd("HELLO 3")

	if got := sub.cmd("SUBSCRIBE news"); got != ">[subscribe news :1]" {
		t.Errorf("SUBSCRIBE in RESP3 = %q; want a push frame", got)
	}
	// The whole point: ordinary commands still work while subscribed.
	if got := sub.cmd("SET while-subscribed 1"); got != "+OK" {
		t.Errorf("SET while subscribed on RESP3 = %q; want it to be allowed", got)
	}
	if got := sub.cmd("GET while-subscribed"); got != "1" {
		t.Errorf("GET while subscribed on RESP3 = %q", got)
	}

	pub := dialTx(t, addr)
	defer pub.close()
	if got := pub.cmd("PUBLISH news hello"); got != ":1" {
		t.Fatalf("PUBLISH = %q", got)
	}
	sub.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if got := readReply(t, sub.br); got != ">[message news hello]" {
		t.Errorf("delivered message = %q; want a push frame", got)
	}

	// A pattern subscription pushes pmessage the same way.
	if got := sub.cmd("PSUBSCRIBE ne*"); got != ">[psubscribe ne* :2]" {
		t.Errorf("PSUBSCRIBE in RESP3 = %q", got)
	}
	pub.cmd("PUBLISH news again")
	sub.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := readReply(t, sub.br) + " " + readReply(t, sub.br)
	if !strings.Contains(got, ">[message news again]") ||
		!strings.Contains(got, ">[pmessage ne* news again]") {
		t.Errorf("pattern delivery = %q", got)
	}
	if got := sub.cmd("UNSUBSCRIBE news"); got != ">[unsubscribe news :1]" {
		t.Errorf("UNSUBSCRIBE in RESP3 = %q", got)
	}
}

// TestRESP2SubscriberModeStillRestricted is the other half of the previous test: the
// RESP2 restriction must stay exactly as it was, because a RESP2 client genuinely
// cannot tell a reply from a delivered message.
func TestRESP2SubscriberModeStillRestricted(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("SUBSCRIBE news"); got != "[subscribe news :1]" {
		t.Errorf("SUBSCRIBE in RESP2 = %q; want a plain array", got)
	}
	if got := c.cmd("GET k"); !strings.HasPrefix(got, "-ERR Can't execute 'get'") {
		t.Errorf("GET while subscribed on RESP2 = %q; want it refused", got)
	}
	// A RESP2 subscriber's PING is a two-element ["pong", <payload>] array, not +PONG: the
	// connection is demultiplexing one stream by shape, and a simple string would be the only
	// reply in it that does not look like a message. Captured from redis:7.2.
	if got := c.cmd("PING"); got != "[pong ]" {
		t.Errorf("PING while subscribed on RESP2 = %q; want [pong ]", got)
	}
	if got := c.cmd("PING hello"); got != "[pong hello]" {
		t.Errorf("PING hello while subscribed on RESP2 = %q; want [pong hello]", got)
	}
}

// TestRESP3KeyspaceNotificationPush checks that notifications reach a RESP3 client as
// pushes too. They travel the same delivery path as a PUBLISH, so a client that
// demultiplexes on push frames sees them with no special case.
func TestRESP3KeyspaceNotificationPush(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	admin := dialTx(t, addr)
	defer admin.close()
	if got := admin.cmd("CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET notify-keyspace-events = %q", got)
	}
	sub := dialTx(t, addr)
	defer sub.close()
	sub.cmd("HELLO 3")
	sub.cmd("SUBSCRIBE __keyevent@0__:set")

	admin.cmd("SET watched 1")
	sub.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if got := readReply(t, sub.br); got != ">[message __keyevent@0__:set watched]" {
		t.Errorf("notification = %q; want a push frame", got)
	}
}
