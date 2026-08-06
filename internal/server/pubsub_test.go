package server

import (
	"context"
	"io"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// subscribe issues a subscribe-family command and returns the confirmations, one per
// name, in the order the server sent them.
func subscribeCmd(t *testing.T, c *txConn, cmd string, n int) []string {
	t.Helper()
	c.conn.Write([]byte(cmd + "\r\n"))
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, readReply(t, c.br))
	}
	return out
}

// nextMessage reads one pushed message, failing if none arrives.
func nextMessage(t *testing.T, c *txConn) string {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.conn.SetReadDeadline(time.Time{})
	return readReply(t, c.br)
}

// TestSubscribeAndPublish covers the confirmations, the message frame, and the
// receiver count PUBLISH reports.
func TestSubscribeAndPublish(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	sub := dialTx(t, addr)
	defer sub.close()
	pub := dialTx(t, addr)
	defer pub.close()

	got := subscribeCmd(t, sub, "SUBSCRIBE news sports", 2)
	want := []string{"[subscribe news :1]", "[subscribe sports :2]"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("confirmation %d = %q; want %q", i, got[i], want[i])
		}
	}

	waitFor(t, "the subscriptions to register", func() bool {
		return pub.cmd("PUBSUB NUMPAT") == ":0" && pub.cmd("PUBLISH news hello") == ":1"
	})
	if got := nextMessage(t, sub); got != "[message news hello]" {
		t.Errorf("delivered %q; want [message news hello]", got)
	}
	// A channel nobody listens to reports zero receivers, and delivers nothing.
	if got := pub.cmd("PUBLISH weather rain"); got != ":0" {
		t.Errorf("PUBLISH to an empty channel = %q; want :0", got)
	}
}

// TestPublishFansOutToManySubscribers checks every subscriber gets its own copy, which
// is the property a shared message buffer would break.
func TestPublishFansOutToManySubscribers(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	const n = 12
	subs := make([]*txConn, n)
	for i := range subs {
		subs[i] = dialTx(t, addr)
		defer subs[i].close()
		subscribeCmd(t, subs[i], "SUBSCRIBE fanout", 1)
	}

	pub := dialTx(t, addr)
	defer pub.close()
	waitFor(t, "all subscribers to register", func() bool {
		return pub.cmd("PUBSUB NUMSUB fanout") == "[fanout :"+strconv.Itoa(n)+"]"
	})
	if got := pub.cmd("PUBLISH fanout go"); got != ":"+strconv.Itoa(n) {
		t.Fatalf("PUBLISH reported %q receivers; want %d", got, n)
	}
	for i, sub := range subs {
		if got := nextMessage(t, sub); got != "[message fanout go]" {
			t.Errorf("subscriber %d received %q", i, got)
		}
	}
}

// TestPatternSubscriptionOverlap covers overlapping patterns and the double delivery a
// client subscribed both ways must get -- Redis counts and delivers once per matching
// subscription, not once per client, so a client that asked twice hears twice.
func TestPatternSubscriptionOverlap(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	sub := dialTx(t, addr)
	defer sub.close()
	pub := dialTx(t, addr)
	defer pub.close()

	subscribeCmd(t, sub, "SUBSCRIBE news.tech", 1)
	if got := subscribeCmd(t, sub, "PSUBSCRIBE news.* *.tech", 2); got[1] != "[psubscribe *.tech :3]" {
		t.Errorf("psubscribe confirmation = %q; want a running total of 3", got[1])
	}

	waitFor(t, "the pattern subscriptions to register", func() bool {
		return pub.cmd("PUBSUB NUMPAT") == ":2"
	})
	// One channel subscription plus two matching patterns: three deliveries.
	if got := pub.cmd("PUBLISH news.tech launch"); got != ":3" {
		t.Fatalf("PUBLISH reported %q; want :3 (one channel + two patterns)", got)
	}
	seen := map[string]int{}
	for i := 0; i < 3; i++ {
		seen[nextMessage(t, sub)]++
	}
	for _, want := range []string{
		"[message news.tech launch]",
		"[pmessage news.* news.tech launch]",
		"[pmessage *.tech news.tech launch]",
	} {
		if seen[want] != 1 {
			t.Errorf("expected exactly one %q; got %d", want, seen[want])
		}
	}
	// A channel matching neither pattern reaches nobody.
	if got := pub.cmd("PUBLISH other.sports x"); got != ":0" {
		t.Errorf("PUBLISH to an unmatched channel = %q; want :0", got)
	}
}

// TestUnsubscribeAll covers the no-argument forms, which are what a client uses to
// leave subscriber mode, and the empty case that still has to answer.
func TestUnsubscribeAll(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	subscribeCmd(t, c, "SUBSCRIBE a b c", 3)
	subscribeCmd(t, c, "PSUBSCRIBE p.*", 1)

	// UNSUBSCRIBE with no arguments drops all three channels, leaving the pattern.
	replies := subscribeCmd(t, c, "UNSUBSCRIBE", 3)
	last := replies[2]
	if last != "[unsubscribe a :1]" && last != "[unsubscribe b :1]" && last != "[unsubscribe c :1]" {
		t.Errorf("final unsubscribe reply = %q; want a count of 1 (the pattern remains)", last)
	}
	// Still in subscriber mode: the pattern is left.
	if got := c.cmd("GET k"); !contains(got, "only (P|S)SUBSCRIBE") {
		t.Errorf("GET with a pattern subscription left = %q; want the subscriber-mode error", got)
	}

	if got := subscribeCmd(t, c, "PUNSUBSCRIBE", 1)[0]; got != "[punsubscribe p.* :0]" {
		t.Errorf("punsubscribe reply = %q; want a count of 0", got)
	}
	// Out of subscriber mode: ordinary commands work again.
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("GET after leaving subscriber mode = %q", got)
	}
	// Unsubscribing with nothing subscribed still gets one reply, naming no channel.
	if got := subscribeCmd(t, c, "UNSUBSCRIBE", 1)[0]; got != "[unsubscribe (nil) :0]" {
		t.Errorf("UNSUBSCRIBE with no subscriptions = %q; want a nil channel and :0", got)
	}
}

// TestSubscriberModeRejectsOtherCommands pins the RESP2 restriction: a subscribed
// connection cannot tell a reply from a message, so only the commands that need no
// correlation are allowed.
func TestSubscriberModeRejectsOtherCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	subscribeCmd(t, c, "SUBSCRIBE ch", 1)

	for _, cmd := range []string{"GET k", "SET k v", "DBSIZE", "INFO", "MULTI", "PUBLISH ch x"} {
		if got := c.cmd(cmd); !contains(got, "only (P|S)SUBSCRIBE") {
			t.Errorf("%q in subscriber mode = %q; want the subscriber-mode error", cmd, got)
		}
	}
	// The allowed ones still work.
	// A subscribed RESP2 connection's PING is the two-element ["pong", ""] array Redis sends
	// there rather than +PONG -- see writeSubscriberPong.
	if got := c.cmd("PING"); got != "[pong ]" {
		t.Errorf("PING in subscriber mode = %q; want [pong ]", got)
	}
	if got := c.cmd("RESET"); got != "+RESET" {
		t.Errorf("RESET in subscriber mode = %q", got)
	}
	// RESET left subscriber mode behind.
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("GET after RESET = %q; want the connection out of subscriber mode", got)
	}
}

// TestSubscribeRejectedInsideMulti covers the one subscribe-family case a transaction
// refuses: what the connection is allowed to run must not change halfway through a
// batch.
func TestSubscribeRejectedInsideMulti(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("MULTI")
	if got := c.cmd("SUBSCRIBE ch"); got != "-ERR SUBSCRIBE is not allowed in transactions" {
		t.Errorf("SUBSCRIBE inside MULTI = %q", got)
	}
	// And it poisoned the transaction, as a rejected queued command must.
	if got := c.cmd("EXEC"); !contains(got, "EXECABORT") {
		t.Errorf("EXEC after a rejected SUBSCRIBE = %q; want EXECABORT", got)
	}
}

// TestSlowSubscriberIsDroppedNotBlocking is the load-bearing test for the delivery
// contract. A subscriber that never drains its queue must not be able to hold up a
// publisher: PUBLISH has no acknowledgement, so a publisher waiting on one slow client
// would stall the connection it runs on and, when a transaction is publishing, the
// server's write ordering lock with it.
//
// It works at the registry level, the way the slow-replica test works at shipRaw's
// level: a session registered with a queue nobody drains, then more messages than the
// queue holds.
func TestSlowSubscriberIsDroppedNotBlocking(t *testing.T) {
	s := New(store.New(4))
	sess := s.newSession(nil)
	sub := sess.subscriberOf()
	s.addSubscription(s.channels, "slow", sess, sub.channels)

	// Fill the queue exactly, then keep going. Nothing may block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < pubsubQueue+50; i++ {
			s.publishMessage("slow", []byte("payload"))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the publisher blocked on a subscriber that never read")
	}

	select {
	case <-sub.dropped:
	default:
		t.Fatal("a subscriber past its queue bound was not dropped")
	}
	if got := s.pubsubDrops.Load(); got == 0 {
		t.Error("pubsub_dropped_subscribers was not counted")
	}
	// The messages that did fit are still there, in order: dropping a subscriber must
	// not disturb what was already queued for it.
	if got := len(sub.ch); got != pubsubQueue {
		t.Errorf("queued %d messages; want the full bound of %d", got, pubsubQueue)
	}
	if got := flatten(<-sub.ch); got != "message slow payload" {
		t.Errorf("first queued message = %q", got)
	}
}

// TestSlowSubscriberConnectionIsClosed is the end-to-end half: the drop has to reach
// the socket, because a closed connection is the only signal a RESP2 client can be
// given that it missed messages.
func TestSlowSubscriberConnectionIsClosed(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	slow := dialTx(t, addr)
	defer slow.close()
	subscribeCmd(t, slow, "SUBSCRIBE flood", 1)

	pub := dialTx(t, addr)
	defer pub.close()
	waitFor(t, "the subscriber to register", func() bool {
		return pub.cmd("PUBSUB NUMSUB flood") == "[flood :1]"
	})

	// Big payloads so the kernel's socket buffers fill quickly and the queue is what
	// actually overflows, rather than the test writing megabytes into the network.
	payload := make([]byte, 64<<10)
	for i := range payload {
		payload[i] = 'x'
	}
	start := time.Now()
	for i := 0; i < pubsubQueue*3; i++ {
		pub.conn.Write([]byte("PUBLISH flood " + string(payload) + "\r\n"))
		readReply(t, pub.br)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("publishing took %v; the publisher was being held up", elapsed)
	}
	waitFor(t, "the slow subscriber's connection to be closed", func() bool {
		return pub.cmd("PUBSUB NUMSUB flood") == "[flood :0]"
	})
	// The publisher's own connection is unharmed.
	if got := pub.cmd("PING"); got != "+PONG" {
		t.Errorf("the publisher's connection = %q after a subscriber was dropped", got)
	}
}

// TestPubSubIntrospection covers PUBSUB CHANNELS/NUMSUB/NUMPAT, including the
// bookkeeping a disconnect has to do: a channel whose last subscriber left must not
// linger.
func TestPubSubIntrospection(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	a := dialTx(t, addr)
	defer a.close()
	b := dialTx(t, addr)
	pub := dialTx(t, addr)
	defer pub.close()

	subscribeCmd(t, a, "SUBSCRIBE news.a news.b", 2)
	subscribeCmd(t, b, "SUBSCRIBE news.a other", 2)
	subscribeCmd(t, b, "PSUBSCRIBE news.*", 1)

	waitFor(t, "the subscriptions to register", func() bool {
		return pub.cmd("PUBSUB NUMSUB news.a") == "[news.a :2]"
	})
	if got := pub.cmd("PUBSUB NUMSUB news.b other absent"); got != "[news.b :1 other :1 absent :0]" {
		t.Errorf("PUBSUB NUMSUB = %q", got)
	}
	if got := pub.cmd("PUBSUB NUMPAT"); got != ":1" {
		t.Errorf("PUBSUB NUMPAT = %q; want :1", got)
	}
	if got := arrayFields(pub.cmd("PUBSUB CHANNELS news.*")); got != "news.a,news.b" {
		t.Errorf("PUBSUB CHANNELS news.* = %q", got)
	}
	if got := arrayFields(pub.cmd("PUBSUB CHANNELS")); got != "news.a,news.b,other" {
		t.Errorf("PUBSUB CHANNELS = %q", got)
	}

	// Disconnecting releases everything that connection held.
	b.close()
	waitFor(t, "the disconnected subscriber's channels to be released", func() bool {
		return arrayFields(pub.cmd("PUBSUB CHANNELS")) == "news.a,news.b" &&
			pub.cmd("PUBSUB NUMPAT") == ":0"
	})
	if got := pub.cmd("PUBSUB NUMSUB news.a"); got != "[news.a :1]" {
		t.Errorf("after a disconnect, PUBSUB NUMSUB news.a = %q; want :1", got)
	}
}

// TestPublishReachesReplicaSubscribers is the replication decision, tested. A client
// subscribed to a replica has to see a message published on the master, because
// otherwise "subscribe to a read replica" -- the whole reason to have replicas serve
// reads -- silently loses events.
func TestPublishReachesReplicaSubscribers(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	leaf, leafAddr, stopLeaf := startServer(t, store.New(8))
	defer stopLeaf()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)
	leaf.ReplicaOf(ctx, replicaAddr)

	mc := dialTx(t, masterAddr)
	defer mc.close()
	poll := dialTx(t, leafAddr)
	defer poll.close()
	mc.cmd("SET fence 1")
	waitFor(t, "the chain to attach", func() bool { return poll.cmd("GET fence") == "1" })

	rsub := dialTx(t, replicaAddr)
	defer rsub.close()
	subscribeCmd(t, rsub, "SUBSCRIBE events", 1)
	lsub := dialTx(t, leafAddr)
	defer lsub.close()
	subscribeCmd(t, lsub, "PSUBSCRIBE ev*", 1)

	waitFor(t, "the replica subscriptions to register", func() bool {
		return dialAndCount(t, replicaAddr, "PUBSUB NUMSUB events") == "[events :1]" &&
			dialAndCount(t, leafAddr, "PUBSUB NUMPAT") == ":1"
	})

	// Published on the master, with no local subscriber at all.
	if got := mc.cmd("PUBLISH events deployed"); got != ":0" {
		t.Errorf("PUBLISH on the master reported %q; want :0, its own local receivers", got)
	}
	if got := nextMessage(t, rsub); got != "[message events deployed]" {
		t.Errorf("the replica's subscriber received %q", got)
	}
	if got := nextMessage(t, lsub); got != "[pmessage ev* events deployed]" {
		t.Errorf("the chained replica's subscriber received %q", got)
	}
}

// dialAndCount runs one introspection command on a fresh connection, so a poll never
// has to share a connection that may be in subscriber mode.
func dialAndCount(t *testing.T, addr, cmd string) string {
	t.Helper()
	c := dialTx(t, addr)
	defer c.close()
	return c.cmd(cmd)
}

// TestPublishStreamedToReplicasNotPersisted pins both halves of the replication
// decision at once: a PUBLISH reaches the replica stream, and it never reaches the
// AOF. Replaying a message at startup would deliver it to subscribers that did not
// exist when it was sent, and would grow a log whose only job is to reconstruct the
// dataset with traffic that is not data.
func TestPublishStreamedToReplicasNotPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pubsub.aof")
	logf, err := aof.Open(path, aof.SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	s := New(store.New(4))
	s.AttachAOF(logf)
	next := tapReplica(t, s)
	sess := s.newSession(nil)
	w := resp.NewWriter(io.Discard)

	s.execute(sess, w, cmdArgs("SET", "before", "1"))
	if got := flatten(next()); got != "SET before 1" {
		t.Fatalf("first propagated command = %q", got)
	}
	s.execute(sess, w, cmdArgs("PUBLISH", "ch", "msg"))
	if got := flatten(next()); got != "PUBLISH ch msg" {
		t.Fatalf("PUBLISH propagated %q; want it streamed to replicas", got)
	}
	s.execute(sess, w, cmdArgs("SET", "after", "2"))
	if got := flatten(next()); got != "SET after 2" {
		t.Fatalf("third propagated command = %q", got)
	}
	if err := logf.Close(); err != nil {
		t.Fatal(err)
	}

	cmds, err := aof.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	var logged []string
	for _, cmd := range cmds {
		logged = append(logged, flatten(cmd))
	}
	want := []string{"SET before 1", "SET after 2"}
	if len(logged) != len(want) {
		t.Fatalf("AOF holds %v; want exactly %v", logged, want)
	}
	for i := range want {
		if logged[i] != want[i] {
			t.Errorf("AOF record %d = %q; want %q", i, logged[i], want[i])
		}
	}
}
