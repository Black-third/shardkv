package server

// Pub/Sub: channel and pattern subscriptions, message fan-out, and the
// subscriber-mode restriction RESP2 requires.
//
// The delivery contract mirrors the one shipRaw uses for replica feeds. Each
// subscriber owns a bounded queue drained by its own goroutine, and a publisher that
// finds a queue full drops that subscriber instead of waiting: PUBLISH is a
// fire-and-forget operation with no acknowledgement, so blocking the publisher (and
// with it the server's ordering lock, and every other client behind it) to serve one
// client that stopped reading trades a whole server for a single connection. Redis
// makes the same call with client-output-buffer-limit.

import (
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/Black-third/shardkv/internal/resp"
)

// pubsubQueue is how many pending messages one subscriber may fall behind by before
// the server hangs up on it. It is deliberately small: a subscriber this far behind
// is not reading, and buffering more only postpones the same decision while holding
// the memory.
const pubsubQueue = 256

// subscriberCommands are the commands a connection in subscriber mode may still run.
// RESP2 has no separate push type, so a subscribed client cannot tell a command's
// reply from a delivered message; the only safe surface is the one that changes
// subscriptions, plus the commands that need no reply correlation at all.
//
// The restriction is RESP2's, not the server's: a RESP3 connection receives messages
// as tagged push frames it can route without matching them against a pending reply,
// so it keeps the full command surface while subscribed (see execute). That is the
// single largest behavioural difference between the two protocols here.
var subscriberCommands = map[string]bool{
	"SUBSCRIBE":    true,
	"UNSUBSCRIBE":  true,
	"PSUBSCRIBE":   true,
	"PUNSUBSCRIBE": true,
	"PING":         true,
	"QUIT":         true,
	"RESET":        true,
}

// writeSubscriberPong answers PING for a RESP2 connection in subscriber mode: the
// two-element ["pong", <payload>] array Redis sends there, with an empty payload when the
// caller gave none. See the call site in executeCommand for why it lives here.
func writeSubscriberPong(w *resp.Writer, args [][]byte) {
	w.BeginPush()
	defer w.EndPush()
	w.WritePushHeader(2)
	w.WriteBulkString("pong")
	if len(args) > 1 {
		w.WriteBulk(args[1])
		return
	}
	w.WriteBulk([]byte{})
}

// subscribeCommands are the four commands that enter or leave subscriber mode. They
// are the ones a MULTI refuses, because what a connection is allowed to run must not
// change halfway through a batch.
var subscribeCommands = map[string]bool{
	"SUBSCRIBE":    true,
	"UNSUBSCRIBE":  true,
	"PSUBSCRIBE":   true,
	"PUNSUBSCRIBE": true,
}

func init() {
	registerSession("SUBSCRIBE", -2, cmdSubscribe)
	registerSession("UNSUBSCRIBE", -1, cmdUnsubscribe)
	registerSession("PSUBSCRIBE", -2, cmdPSubscribe)
	registerSession("PUNSUBSCRIBE", -1, cmdPUnsubscribe)
	register("PUBLISH", 3, false, cmdPublish)
	register("PUBSUB", -2, false, cmdPubSub)
}

// subscriber is one connection's Pub/Sub state: the bounded outbound queue a
// publisher hands messages to, the one-shot signal that ends the connection when it
// falls too far behind, and the names it is subscribed to.
//
// The two name sets are touched only by the owning connection's goroutine (the
// server's own registry is what publishers consult), so they need no lock.
type subscriber struct {
	ch      chan [][]byte
	dropped chan struct{}
	once    sync.Once

	channels map[string]struct{}
	patterns map[string]struct{}
}

func newSubscriber() *subscriber {
	return &subscriber{
		ch:       make(chan [][]byte, pubsubQueue),
		dropped:  make(chan struct{}),
		channels: make(map[string]struct{}),
		patterns: make(map[string]struct{}),
	}
}

// send queues a message without ever blocking. It reports false when the queue is
// full, which the caller answers by dropping the subscriber.
func (sub *subscriber) send(msg [][]byte) bool {
	select {
	case sub.ch <- msg:
		return true
	default:
		return false
	}
}

// drop marks the subscriber as too far behind, reporting whether this call was the one
// that did it. Only the first caller counts and logs the event: a subscriber stays
// registered for the moment it takes its connection goroutine to notice, and every
// publish in that window would otherwise report the same drop again.
func (sub *subscriber) drop() bool {
	first := false
	sub.once.Do(func() {
		close(sub.dropped)
		first = true
	})
	return first
}

// dropSubscriber hangs up on a subscriber that fell behind. The connection is closed
// here rather than left to the pump goroutine, because the reason a subscriber falls
// behind is that its socket is full -- which is precisely where the pump is blocked,
// unable to notice a signal. Closing the socket is what unblocks it.
//
// A closed connection is also the only thing a RESP2 client can be told: the protocol
// has no way to say "you missed messages", so a visible disconnect it can reconnect
// and resubscribe from is better than leaving it subscribed and silently short.
func (s *Server) dropSubscriber(sess *session) {
	if !sess.sub.drop() {
		return
	}
	if sess.conn != nil {
		sess.conn.Close()
	}
	s.pubsubDrops.Add(1)
	log.Printf("shardkv: subscriber %d too far behind, closing its connection", sess.id)
}

// subscriberOf returns the connection's subscriber, creating it on first use. Called
// only from the owning connection's goroutine.
func (sess *session) subscriberOf() *subscriber {
	if sess.sub == nil {
		sess.sub = newSubscriber()
	}
	return sess.sub
}

// pumpMessages writes delivered messages to the connection. It is started by the
// connection loop when the connection takes its first subscription and runs until
// the connection ends, so exactly one goroutine per connection exists at most.
//
// Both this goroutine and the command loop write to the same resp.Writer, so both
// take the session's writer lock; a message is written and flushed as a unit, never
// interleaved with a command reply.
func (s *Server) pumpMessages(sess *session, w *resp.Writer) {
	sub := sess.sub
	for {
		select {
		case <-sess.done:
			return
		case <-sub.dropped:
			sess.conn.Close() // forces the client to reconnect and resubscribe
			return
		case msg := <-sub.ch:
			sess.wmu.Lock()
			writePush(w, msg)
			// The write itself reports nothing: bufio's error is sticky, so a failure
			// mid-frame surfaces here, on the flush that makes the frame visible.
			err := w.Flush()
			sess.wmu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// writePush writes one out-of-band Pub/Sub frame: a RESP3 push (>) or, for a RESP2
// client, the plain array of bulk strings it has always received. Every element of
// every frame this server pushes is a bulk string except the subscription count,
// which writeSubscribeReply writes itself.
func writePush(w *resp.Writer, msg [][]byte) {
	// Out-of-band, so it is delivered even with CLIENT REPLY OFF in force. See
	// resp.Writer.BeginPush.
	w.BeginPush()
	defer w.EndPush()
	w.WritePushHeader(len(msg))
	for _, part := range msg {
		w.WriteBulk(part)
	}
}

// --- registry ----------------------------------------------------------------

// addSubscription registers sess under name in one of the two registries and reports
// whether it was newly added (a repeat SUBSCRIBE to the same channel is a no-op that
// still gets a confirmation, as in Redis).
func (s *Server) addSubscription(reg map[string]map[*session]struct{}, name string, sess *session, own map[string]struct{}) bool {
	if _, dup := own[name]; dup {
		return false
	}
	own[name] = struct{}{}
	s.subMu.Lock()
	if reg[name] == nil {
		reg[name] = make(map[*session]struct{})
	}
	reg[name][sess] = struct{}{}
	s.subMu.Unlock()
	return true
}

// removeSubscription unregisters sess from name, reporting whether it was subscribed.
func (s *Server) removeSubscription(reg map[string]map[*session]struct{}, name string, sess *session, own map[string]struct{}) bool {
	if _, ok := own[name]; !ok {
		return false
	}
	delete(own, name)
	s.subMu.Lock()
	if m := reg[name]; m != nil {
		delete(m, sess)
		if len(m) == 0 {
			delete(reg, name) // an empty channel must not linger in PUBSUB CHANNELS
		}
	}
	s.subMu.Unlock()
	return true
}

// unsubscribeSession removes every subscription the connection holds. It runs on
// disconnect and on RESET, and is a no-op for a connection that never subscribed.
func (s *Server) unsubscribeSession(sess *session) {
	sub := sess.sub
	if sub == nil {
		return
	}
	s.subMu.Lock()
	for name := range sub.channels {
		if m := s.channels[name]; m != nil {
			delete(m, sess)
			if len(m) == 0 {
				delete(s.channels, name)
			}
		}
	}
	for pat := range sub.patterns {
		if m := s.patterns[pat]; m != nil {
			delete(m, sess)
			if len(m) == 0 {
				delete(s.patterns, pat)
			}
		}
	}
	s.subMu.Unlock()

	sub.channels = make(map[string]struct{})
	sub.patterns = make(map[string]struct{})
	sess.nSub.Store(0)
	sess.nPSub.Store(0)
}

// publishMessage delivers payload to every subscriber of channel and to every
// subscriber whose pattern matches it, returning how many subscriptions received it
// (a client subscribed both directly and by pattern is counted twice, as Redis
// counts it).
//
// Slow subscribers are collected under the registry lock and dropped after releasing
// it, so closing a connection never happens with the lock held.
func (s *Server) publishMessage(channel string, payload []byte) int {
	chMsg := [][]byte{[]byte("message"), []byte(channel), payload}

	var slow []*session
	n := 0
	s.subMu.Lock()
	for sess := range s.channels[channel] {
		if sess.sub.send(chMsg) {
			n++
		} else {
			slow = append(slow, sess)
		}
	}
	for pat, subs := range s.patterns {
		if !globMatch(pat, channel) {
			continue
		}
		patMsg := [][]byte{[]byte("pmessage"), []byte(pat), []byte(channel), payload}
		for sess := range subs {
			if sess.sub.send(patMsg) {
				n++
			} else {
				slow = append(slow, sess)
			}
		}
	}
	s.subMu.Unlock()

	for _, sess := range slow {
		s.dropSubscriber(sess)
	}
	return n
}

// --- commands ----------------------------------------------------------------

// writeSubscribeReply writes the confirmation Redis sends for each channel or
// pattern named in a (un)subscribe command: the kind, the name, and the connection's
// resulting total number of subscriptions across both registries. Clients count that
// total down to know when they have left subscriber mode.
//
// It is a push frame in RESP3, not a reply: the confirmation is the same kind of
// out-of-band event as a delivered message (a server-initiated UNSUBSCRIBE would
// arrive with no command to attach it to), so RESP3 tags it the same way.
func writeSubscribeReply(w *resp.Writer, kind string, name []byte, total int) {
	w.BeginPush()
	defer w.EndPush()
	w.WritePushHeader(3)
	w.WriteBulkString(kind)
	w.WriteBulk(name)
	w.WriteInt(int64(total))
}

func (sess *session) subscriptionTotal() int {
	if sess.sub == nil {
		return 0
	}
	return len(sess.sub.channels) + len(sess.sub.patterns)
}

func cmdSubscribe(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	sub := sess.subscriberOf()
	for _, name := range args[1:] {
		s.addSubscription(s.channels, string(name), sess, sub.channels)
		sess.nSub.Store(int64(len(sub.channels)))
		writeSubscribeReply(w, "subscribe", name, sess.subscriptionTotal())
	}
	sess.subscribed = true // the connection loop starts the pump when it sees this
}

func cmdPSubscribe(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	sub := sess.subscriberOf()
	for _, pat := range args[1:] {
		s.addSubscription(s.patterns, string(pat), sess, sub.patterns)
		sess.nPSub.Store(int64(len(sub.patterns)))
		writeSubscribeReply(w, "psubscribe", pat, sess.subscriptionTotal())
	}
	sess.subscribed = true
}

// cmdUnsubscribe implements UNSUBSCRIBE [channel...]. With no channel it drops every
// channel subscription, one confirmation each; a connection with none still gets a
// single reply naming no channel, which is what tells a client the count is zero.
func cmdUnsubscribe(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	unsubscribe(s, sess, w, args, "unsubscribe", s.channels)
}

func cmdPUnsubscribe(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	unsubscribe(s, sess, w, args, "punsubscribe", s.patterns)
}

func unsubscribe(s *Server, sess *session, w *resp.Writer, args [][]byte, kind string, reg map[string]map[*session]struct{}) {
	sub := sess.subscriberOf()
	own := sub.channels
	counter := &sess.nSub
	if kind == "punsubscribe" {
		own, counter = sub.patterns, &sess.nPSub
	}

	names := args[1:]
	if len(names) == 0 {
		if len(own) == 0 {
			writeSubscribeReply(w, kind, nil, sess.subscriptionTotal())
			return
		}
		names = make([][]byte, 0, len(own))
		for name := range own {
			names = append(names, []byte(name))
		}
	}
	for _, name := range names {
		s.removeSubscription(reg, string(name), sess, own)
		counter.Store(int64(len(own)))
		writeSubscribeReply(w, kind, name, sess.subscriptionTotal())
	}
}

// cmdPublish implements PUBLISH channel message. It returns the number of
// subscribers that received the message on *this* server, which is what Redis
// reports: a count including replicas' subscribers would be a number the publisher
// could not act on, since it cannot know when the message got there.
//
// On a master the message is also streamed to the connected replicas so that a
// client subscribed to a replica sees messages published anywhere in the tree --
// what real Redis does by replicating PUBLISH. It is never written to the AOF: a
// message has no place in a log whose purpose is to reconstruct the dataset, and
// replaying it at startup would deliver it to subscribers that had not yet
// subscribed when it was sent.
//
// A replica does not relay a PUBLISH it received from a client, only the ones that
// arrive on its replication link (replicationLoop forwards those). Relaying both
// would deliver every message twice to a chained replica's subscribers.
func cmdPublish(s *Server, w *resp.Writer, args [][]byte) bool {
	n := s.publishMessage(string(args[1]), args[2])
	if !s.isReplica() {
		s.shipReplicas(args)
	}
	w.WriteInt(int64(n))
	return false // never dirty: no dataset state changed, so nothing to persist
}

// cmdPubSub implements PUBSUB CHANNELS [pattern] | NUMSUB [channel...] | NUMPAT.
func cmdPubSub(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "CHANNELS":
		if len(args) > 3 {
			w.WriteError("ERR wrong number of arguments for 'pubsub|channels' command")
			return false
		}
		pattern := ""
		if len(args) == 3 {
			pattern = string(args[2])
		}
		s.subMu.Lock()
		names := make([]string, 0, len(s.channels))
		for name := range s.channels {
			if pattern == "" || globMatch(pattern, name) {
				names = append(names, name)
			}
		}
		s.subMu.Unlock()
		writeStrings(w, names)

	case "NUMSUB":
		// A flat channel/count array, in the order asked, including channels nobody is
		// subscribed to (count 0) so a caller can index the reply against its request.
		w.WriteArrayHeader((len(args) - 2) * 2)
		s.subMu.Lock()
		for _, name := range args[2:] {
			w.WriteBulk(name)
			w.WriteInt(int64(len(s.channels[string(name)])))
		}
		s.subMu.Unlock()

	case "NUMPAT":
		// The number of distinct patterns, not of pattern subscriptions: two clients
		// watching news.* are one pattern the fan-out has to evaluate.
		s.subMu.Lock()
		n := len(s.patterns)
		s.subMu.Unlock()
		w.WriteInt(int64(n))

	case "HELP":
		writeSubcommandHelp(w, "PUBSUB", []string{
			"CHANNELS [<pattern>]",
			"    Return the currently active channels matching a <pattern> (default: '*').",
			"NUMPAT",
			"    Return number of subscriptions to patterns.",
			"NUMSUB [<channel> ...]",
			"    Return the number of subscribers for the specified channels, excluding",
			"    pattern subscriptions(default: no channels).",
		})

	default:
		writeUnknownSubcommand(w, "PUBSUB", args[1])
	}
	return false
}

// pubsubStats renders the Pub/Sub section of INFO.
func (s *Server) pubsubStats() string {
	s.subMu.Lock()
	channels, patterns := len(s.channels), len(s.patterns)
	s.subMu.Unlock()
	return "pubsub_channels:" + strconv.Itoa(channels) + "\r\n" +
		"pubsub_patterns:" + strconv.Itoa(patterns) + "\r\n" +
		"pubsub_dropped_subscribers:" + strconv.FormatInt(s.pubsubDrops.Load(), 10) + "\r\n"
}
