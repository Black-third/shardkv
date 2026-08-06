package server

// Blocking commands: BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BZPOPMIN, BZPOPMAX, BLMPOP
// and BZMPOP. The commands themselves are in commands_blocking.go; this file is the
// mechanism they share.
//
// # What a blocked client must not hold
//
// A blocked client holds nothing. It is not inside a shard lock (the store call that
// found the list empty has already returned), it is not inside propMu (the write path
// releases it before the decision to wait is taken), and it does not hold the
// connection's writer lock either, so a subscribed connection's message pump keeps
// running while the same connection waits on a list. The only state a waiter owns is
// its entry in the wait queues, which is a slice element under blockMu -- a leaf lock
// held for the length of a map lookup and never across a store call or a channel
// send that could block.
//
// # Wakeup, without polling
//
// Every write that can create data signals the keys it touched (see signalWaiters,
// called from the one write path in runWrite). The signal is a non-blocking send on
// the waiter's own single-slot channel: the pusher never waits, and a signal that
// arrives while the waiter is mid-retry is not lost, because the slot holds it until
// the waiter's next select. There is no timer, no tick and no rescan of the keyspace;
// a waiter that is never pushed to consumes exactly one parked goroutine.
//
// When no client is blocked anywhere, signalWaiters is a single atomic load. That is
// the whole cost the feature adds to the write path.
//
// # FIFO fairness
//
// Each key's queue is ordered by arrival and *only its head is ever woken*. That one
// rule is what makes the wakeup fair: a client behind the head is never told there
// might be data, so it cannot race the head for it. If the head then finds nothing --
// because a plain LPOP took the element in between -- it simply goes back to sleep at
// the head of the queue, and nobody behind it was disturbed.
//
// A waiter that *does* get served hands the queue on before returning: it leaves
// every queue it was in and signals the new head of each, because one push can carry
// several elements and the next waiter in line has to be told about the rest.
//
// The guarantee is over *blocked* clients, which is what Redis's fairness rule is
// about. A brand-new BLPOP arriving at the same instant as the data makes one
// opportunistic attempt before it joins any queue, and can therefore be served ahead
// of a client that was already waiting. Closing that window would mean serializing
// every arriving command behind a single lock -- which is exactly what a
// single-threaded server does, and exactly what this one is built not to do. A plain
// LPOP racing the same instant has always been able to take the element first, so the
// window is not new; it is the price of sharded concurrency, and it is bounded by the
// scheduling delay between a signal and the woken goroutine's next instruction.
//
// # Effect propagation
//
// A blocking command never reaches the AOF or a replica. What propagates is the pop
// that happened -- LPOP, RPOP, LMOVE, ZREM -- because a replica replaying BLPOP would
// block forever on a connection that has no client behind it. That is the same rule
// SPOP follows, and it is why these commands are registered with a blockFunc that
// returns an effect.

import (
	"errors"
	"math"
	"os"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// errUnblockedRoleChange is what a blocked client is told when the server it is
// blocked on stops being a master. Its blocking command is a write, so continuing to
// wait for one to succeed would be waiting for something that can no longer happen;
// Redis reports the same error for the same reason.
const errUnblockedRoleChange = "UNBLOCKED force unblock from blocking operation, " +
	"instance state changed (master -> replica?)"

// errUnblockedClient is what CLIENT UNBLOCK <id> ERROR delivers.
const errUnblockedClient = "UNBLOCKED client unblocked via CLIENT UNBLOCK"

// waiter is one blocked client. Everything on it is written once, before the waiter is
// registered, except the two channels -- which is what makes it safe for another
// connection's goroutine to signal it.
type waiter struct {
	// ready carries a wakeup: "a key you are waiting on may have data now". It has one
	// slot so a pusher never blocks and a signal is never lost.
	ready chan struct{}
	// forced carries an out-of-band end to the wait: an empty string means "reply as if
	// you had timed out", anything else is an error message to reply with. It is how
	// CLIENT UNBLOCK and a demotion to replica reach a client that is already asleep.
	forced chan string
	// keys are the queues this waiter sits in, each naming a key *in a database*: a
	// client blocked on "q" after SELECT 1 must not be woken by a push to "q" in
	// database 0.
	keys []dbKey
	id   int64
}

// blockRegister puts the waiter at the back of every one of its keys' queues.
func (s *Server) blockRegister(wt *waiter) {
	s.blockMu.Lock()
	for _, k := range wt.keys {
		s.blockQueues[k] = append(s.blockQueues[k], wt)
	}
	s.blockByID[wt.id] = wt
	s.blockMu.Unlock()
	s.blockedCount.Add(1)
}

// blockUnregister removes the waiter from every queue it is in. It runs on every exit
// path -- served, timed out, unblocked, disconnected, server shutting down -- so a
// queue can never retain a waiter whose goroutine is gone.
func (s *Server) blockUnregister(wt *waiter) {
	s.blockMu.Lock()
	for _, k := range wt.keys {
		q := s.blockQueues[k]
		for i, other := range q {
			if other == wt {
				s.blockQueues[k] = append(q[:i], q[i+1:]...)
				break
			}
		}
		if len(s.blockQueues[k]) == 0 {
			delete(s.blockQueues, k) // an empty queue must not linger in the registry
		}
	}
	delete(s.blockByID, wt.id)
	s.blockMu.Unlock()
	s.blockedCount.Add(-1)
}

// wakeKeys signals the earliest waiter on each of the given keys. Only the head is
// woken, which is what makes the wakeup FIFO: a client behind the head is never told
// there might be data, so it cannot race the head for it.
func (s *Server) wakeKeys(keys []dbKey) {
	var heads []*waiter
	s.blockMu.Lock()
	for _, k := range keys {
		if q := s.blockQueues[k]; len(q) > 0 {
			heads = append(heads, q[0])
		}
	}
	s.blockMu.Unlock()
	// Signal outside the registry lock: a send here cannot block (the slot is
	// single-buffered and dropped when full), but keeping the lock to a map lookup
	// means a pusher can never be delayed by a waiter.
	for _, wt := range heads {
		select {
		case wt.ready <- struct{}{}:
		default: // a wakeup is already pending; the waiter will retry once for both
		}
	}
}

// signalWaiters wakes the clients blocked on any key the given commands touched.
//
// The first line is the entire cost when nobody is blocked: one atomic load, before
// any key name is materialized. cmds is the same view of the write that drives WATCH
// invalidation and keyspace notifications -- the command itself, or, for a blocking
// command, the effect it had -- so the keys signalled are exactly the keys reported
// changed.
//
// Over-signalling is harmless: a woken waiter that finds nothing goes back to sleep.
// Under-signalling is a hang, which is why this is driven from affectedKeys, the same
// list invariant 7 requires to be complete for WATCH.
func (s *Server) signalWaiters(cmds [][][]byte) {
	if s.blockedCount.Load() == 0 {
		return
	}
	for _, cmd := range cmds {
		if len(cmd) == 0 {
			continue
		}
		name := strings.ToUpper(string(cmd[0]))
		// Three commands change the keyspace without naming a key, so there is no key-level
		// answer and every waiter in scope is told to look again. All three are rare
		// administrative commands, so the sweep costs nothing that matters.
		//
		//   - SWAPDB can make data *appear* under a key nothing was written to.
		//   - FLUSHALL and FLUSHDB make it disappear. That matters even though there is
		//     nothing to serve afterwards: a client blocked in XREADGROUP was waiting on a
		//     consumer *group*, and a flush destroyed it, so it is now waiting for something
		//     that can never arrive and has to be told (NOGROUP). A BLPOP waiter woken by
		//     the same sweep finds no list, and goes straight back to sleep -- which is
		//     what it did before, reached differently.
		//
		// FLUSHDB is scoped to the database it emptied; the other two are not scoped at all.
		switch name {
		case "SWAPDB", "FLUSHALL":
			s.wakeAll()
			continue
		case "FLUSHDB":
			s.wakeDatabase(s.db)
			continue
		}
		s.wakeKeys(s.affectedDBKeys(name, cmd))
	}
}

// wakeDatabase signals the head of every queue in one database, for a change that emptied
// or replaced that database's whole keyspace.
func (s *Server) wakeDatabase(db int) {
	var heads []*waiter
	s.blockMu.Lock()
	for k, q := range s.blockQueues {
		if k.db == db && len(q) > 0 {
			heads = append(heads, q[0])
		}
	}
	s.blockMu.Unlock()
	signalHeads(heads)
}

// wakeAll signals the head of every queue, for a change too sweeping to name keys for.
func (s *Server) wakeAll() {
	var heads []*waiter
	s.blockMu.Lock()
	for _, q := range s.blockQueues {
		if len(q) > 0 {
			heads = append(heads, q[0])
		}
	}
	s.blockMu.Unlock()
	signalHeads(heads)
}

// signalHeads delivers the wakeup, outside the registry lock for the reason wakeKeys gives:
// a send here cannot block (the slot is single-buffered and dropped when full), and keeping
// the lock to a map walk means a writer is never delayed by a waiter.
func signalHeads(heads []*waiter) {
	for _, wt := range heads {
		select {
		case wt.ready <- struct{}{}:
		default: // a wakeup is already pending; the waiter will retry once for both
		}
	}
}

// dbKeys tags key names with the database this view addresses.
func (s *Server) dbKeys(keys []string) []dbKey {
	out := make([]dbKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, dbKey{db: s.db, key: k})
	}
	return out
}

// unblockClient ends the wait of the client with the given id, reporting whether one
// was blocked. errMsg empty makes it reply as a timeout would; otherwise it replies
// with that error. It backs CLIENT UNBLOCK.
func (s *Server) unblockClient(id int64, errMsg string) bool {
	s.blockMu.Lock()
	wt := s.blockByID[id]
	s.blockMu.Unlock()
	if wt == nil {
		return false
	}
	select {
	case wt.forced <- errMsg:
		return true
	default:
		return true // already being unblocked by someone else
	}
}

// unblockAll ends every wait on the server with the same error. It runs when the
// server stops being a master: the blocking commands are writes, so a client waiting
// for one to succeed is now waiting for something that cannot happen.
func (s *Server) unblockAll(errMsg string) {
	if s.blockedCount.Load() == 0 {
		return
	}
	s.blockMu.Lock()
	waiters := make([]*waiter, 0, len(s.blockByID))
	for _, wt := range s.blockByID {
		waiters = append(waiters, wt)
	}
	s.blockMu.Unlock()
	for _, wt := range waiters {
		select {
		case wt.forced <- errMsg:
		default:
		}
	}
}

// blockedClients reports how many connections are waiting in a blocking command, for
// INFO's blocked_clients.
func (s *Server) blockedClients() int64 { return s.blockedCount.Load() }

// blockingKeys reports how many distinct keys have at least one client waiting on
// them, for INFO's total_blocking_keys.
func (s *Server) blockingKeys() int {
	s.blockMu.Lock()
	defer s.blockMu.Unlock()
	return len(s.blockQueues)
}

// dispatchBlocking is execute's entry point for a blocking command. It exists
// separately from dispatch because a blocking command is the one dataset command that
// needs the connection behind it, and dispatch deliberately has none.
// It reports whether the command ran, on the same contract dispatch uses.
func (s *Server) dispatchBlocking(sess *session, w *resp.Writer, cmd *command, args [][]byte) (ran bool) {
	s.totalCmds.Add(1)
	if !arityOK(cmd.arity, len(args)) {
		w.WriteError("ERR wrong number of arguments for '" + cmd.lowerName + "' command")
		reject(cmd)
		return false
	}
	if cmd.write && s.isReplica() {
		w.WriteError(errReadOnly)
		reject(cmd)
		return false
	}
	s.blockUntilServed(sess, w, cmd, args)
	return true
}

// blockUntilServed runs a blocking command on behalf of a client connection.
//
// The shape is attempt-then-wait, not wait-then-attempt: the overwhelmingly common
// case is a queue that already has an element, and that path must not pay for the
// wait machinery at all. Registration happens before the *second* attempt rather than
// after the first, which is what closes the lost-wakeup window -- a push landing
// between the first attempt and the registration is answered by the retry that
// immediately follows it.
func (s *Server) blockUntilServed(sess *session, w *resp.Writer, cmd *command, args [][]byte) {
	timeout, errMsg := cmd.block.waitFor(args)
	if errMsg != "" {
		w.WriteError(errMsg)
		return
	}

	served, wait := s.tryBlocking(cmd, w, args)
	if served || !wait {
		return // served, or the arguments were rejected and the error is already written
	}

	wt := &waiter{
		ready:  make(chan struct{}, 1),
		forced: make(chan string, 1),
		keys:   s.dbKeys(cmd.block.keys(args)),
		id:     sess.id,
	}
	// Everything from here on is time the client spends waiting rather than time the
	// server spends working, which is what the slow log and the per-command latency
	// statistics have to exclude. See session.blockedFor.
	parked := time.Now()
	defer func() { sess.blockedFor += time.Since(parked) }()
	s.blockRegister(wt)
	defer func() {
		s.blockUnregister(wt)
		// Hand the queue on. One push can carry several elements, and a waiter that was
		// woken for key A may have served itself from key B, so the new head of every
		// queue this waiter was in is told to look.
		s.wakeKeys(wt.keys)
	}()

	var deadline <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		deadline = timer.C
	}
	gone, stopWatching := watchClientGone(sess)
	defer stopWatching()

	// The reply to whatever this connection sent before may still be sitting in the
	// write buffer, and the client can be waiting for it before it sends anything
	// else. Flush before going to sleep, or a pipelining client and this server would
	// wait for each other. A flush that fails means the socket is already gone, so
	// there is nothing to wait on behalf of.
	if err := w.Flush(); err != nil {
		return
	}

	// Release the connection's writer lock for the duration of the wait. The command
	// loop holds it for subscribed connections so that the Pub/Sub pump cannot
	// interleave with a reply; holding it across a block would stall that pump for the
	// whole timeout.
	released := sess.unlockWriter()
	relock := func() {
		if released {
			sess.relockWriter()
			released = false
		}
	}
	defer relock()

	for {
		select {
		case <-wt.ready:
			relock()
			if s.retryBlocking(cmd, w, args) {
				return
			}
			// Nothing there after all: the element was taken between the signal and this
			// retry, or what appeared is not something this command can serve. Stay at the
			// head of the queue and wait again.
			released = sess.unlockWriter()

		case <-deadline:
			relock()
			cmd.block.empty(w)
			return

		case msg := <-wt.forced:
			relock()
			if msg == "" {
				cmd.block.empty(w) // CLIENT UNBLOCK <id>: as if the timeout had elapsed
			} else {
				w.WriteError(msg)
			}
			return

		case <-gone:
			// The client hung up. Nothing is written -- there is nobody to write to -- and
			// the deferred unregister takes this waiter out of every queue.
			return

		case <-sess.serveDone:
			// The server is shutting down. Same as a disconnect: the connection is about
			// to be closed under us.
			return
		}
	}
}

// tryBlocking runs one non-blocking attempt at a blocking command, through the same
// write path an ordinary write takes. If it serves data, that pop is a write like any
// other: totally ordered under propMu, persisted, replicated (as its effect), and
// reported to WATCHers and keyspace subscribers.
//
// served says the reply is written. wait says nothing could be served *and* nothing
// was written, so the caller may block; the third case -- nothing served because the
// arguments were rejected -- reports neither, since the error is already on the wire.
func (s *Server) tryBlocking(cmd *command, w *resp.Writer, args [][]byte) (served, wait bool) {
	if cmd.block.readOnly {
		// A read has nothing to order, persist or replicate, so it does not take the write
		// path at all. For such a command "it served the client" is exactly "it did not ask
		// to wait": either it answered with data, or it rejected the arguments and wrote the
		// error, and in both cases the caller is finished.
		_, wait = cmd.block.fn(s, w, args)
		return !wait, wait
	}
	served = s.runWrite(cmd, args, func() (bool, [][][]byte) {
		effect, w := cmd.block.fn(s, w, args)
		wait = w
		return len(effect) > 0, effect
	})
	return served, wait
}

// retryBlocking is one attempt made after a wakeup, reporting whether the client was
// served and may be released.
//
// It is not simply "run the command again", because a waiter is woken by *anything* that
// creates one of its keys -- and what created it may be the wrong kind of thing entirely. A
// client blocked in BZPOPMIN is woken by the SET in `ZADD k 0 x; DEL k; SET k v`, and
// running the command then would answer WRONGTYPE about a value that is already gone by the
// time the client reads the error, failing a command whose own arguments were never wrong.
//
// So the wakeup is filtered first: unless one of the keys this command waits on now holds a
// value of the type it can serve, nothing is attempted and the client stays at the head of
// its queue. Redis checks exactly this (getBlockedTypeByType, in handleClientsBlockedOnKey)
// and its own test pins it -- "ZADD + DEL + SET should not awake blocked client".
//
// Once a key of the right type is there the command runs normally and whatever it produces
// is delivered, error included. That distinction matters: BRPOPLPUSH onto a *string
// destination* must answer WRONGTYPE when the source finally receives an element, because
// the destination's type is a real problem with the command rather than a spurious wakeup.
func (s *Server) retryBlocking(cmd *command, w *resp.Writer, args [][]byte) bool {
	if !s.wakeupIsServiceable(cmd, args) {
		return false
	}
	// An error counts as an answer. The attempt has three outcomes -- served, nothing to
	// serve, and refused -- and only the middle one leaves the client waiting; a refusal has
	// already written the error, so continuing to wait would leave that error on the wire in
	// front of whatever the next attempt produced.
	errs0 := w.ErrorsWritten()
	served, _ := s.tryBlocking(cmd, w, args)
	return served || w.ErrorsWritten() != errs0
}

// wakeupIsServiceable reports whether any key the command waits on currently holds a value
// of the type that command can serve.
//
// A key that is absent counts as not serviceable: there is nothing there to hand over, so
// attempting the command would find nothing and the client would go back to sleep anyway.
// A spec with no declared type (there is none today) is always attempted, so adding a
// blocking command without one degrades to the old behaviour rather than to a hang.
//
// XREADGROUP opts out entirely (wakeOnAnyChange): what it waits on is a consumer group, and
// a key that vanished or changed type has taken the group with it.
func (s *Server) wakeupIsServiceable(cmd *command, args [][]byte) bool {
	if cmd.block.wantType == "" || cmd.block.wakeOnAnyChange {
		return true
	}
	for _, key := range cmd.block.keys(args) {
		if typ, ok := s.store.Type(key); ok && typ == cmd.block.wantType {
			return true
		}
	}
	return false
}

// waitFor reports how long this command is willing to wait, or the error to answer
// with. It is the one place that knows a spec may bring its own timeout parser.
func (b *blockSpec) waitFor(args [][]byte) (time.Duration, string) {
	if b.timeoutFn != nil {
		return b.timeoutFn(args)
	}
	return parseBlockTimeout(args, b.timeout)
}

// parseBlockTimeout parses a blocking command's timeout operand: fractional seconds,
// where 0 means "wait forever". idx is the operand's position, or -1 for the last
// argument -- BLPOP and its relatives put the timeout last, while the MPOP family
// puts it first, before the key count it cannot be placed after.
//
// The error messages are Redis's, because a client library matches on them.
func parseBlockTimeout(args [][]byte, idx int) (time.Duration, string) {
	if idx < 0 {
		idx = len(args) + idx
	}
	if idx < 1 || idx >= len(args) {
		return 0, "ERR timeout is not a float or out of range"
	}
	secs, ok := parseFloat(args[idx])
	if !ok || math.IsNaN(secs) {
		return 0, "ERR timeout is not a float or out of range"
	}
	if secs < 0 {
		return 0, "ERR timeout is negative"
	}
	// Redis expresses the timeout in milliseconds internally and refuses an operand
	// whose millisecond value would not fit an int64, so the same operands are refused
	// here and with the same message.
	if ms := secs * 1000; math.IsInf(ms, 0) || ms > float64(math.MaxInt64) {
		return 0, "ERR timeout is out of range"
	}
	// A deadline further out than a time.Duration can express (about 292 years) is
	// clamped rather than refused: Redis accepts such a timeout, and no client that
	// could still be connected to notice the difference will be.
	if secs >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64), ""
	}
	return time.Duration(secs * float64(time.Second)), ""
}

// watchClientGone reports on the returned channel if the client's socket fails while
// it is blocked.
//
// Nothing is reading that socket during a block: the connection's own goroutine is
// the reader, and it is parked in the wait. A client that hangs up would therefore
// leave a waiter queued and a goroutine parked on a key nobody will ever push to, so
// the socket has to be watched from somewhere. A peek is the way to watch it without
// consuming from it -- it returns an error when the connection breaks, and returns
// successfully (harmlessly) if the client pipelined another command behind this one,
// in which case that command is still there for the command loop to read afterwards.
//
// stop forces the peek out of its read by setting a deadline in the past, waits for
// it to finish, and clears the deadline again. That handshake is what makes the
// reader safe to use from the connection's goroutine again -- the peek is provably
// finished before stop returns -- and the deadline error is our own, so it is not
// mistaken for a disconnect.
func watchClientGone(sess *session) (gone <-chan struct{}, stop func()) {
	if sess.conn == nil || sess.rd == nil {
		return nil, func() {} // not a real connection (an internal or test session)
	}
	ch := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := sess.rd.WaitReadable(); err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			close(ch)
		}
	}()
	return ch, func() {
		// Both deadline calls can only fail on a connection that is already closed, which
		// is the one case in which the peek is certain to return on its own -- so their
		// errors are deliberately discarded rather than acted on. What matters is the
		// handshake in between: the peek is provably finished before this returns.
		_ = sess.conn.SetReadDeadline(time.Now())
		<-done
		_ = sess.conn.SetReadDeadline(time.Time{})
	}
}
