package server

import (
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	// The transaction commands are connection-control commands like any other: they
	// change what this one socket does next and touch no key. Registering them in the
	// table rather than special-casing them in execute is what makes the table
	// authoritative -- COMMAND COUNT, COMMAND INFO and the arity checks then cover them
	// for free, instead of reporting a server that has no MULTI.
	registerSession("MULTI", 1, cmdMulti)
	registerSession("EXEC", 1, cmdExec)
	registerSession("DISCARD", 1, cmdDiscard)
	registerSession("WATCH", -2, cmdWatch)
	registerSession("UNWATCH", 1, cmdUnwatch)
}

// execute is the per-connection entry point. It measures the command, then runs it.
//
// The measurement is here, around executeCommand, rather than inside the dispatcher:
// this is the one place that sees a client command whole -- including the ones that
// never reach dispatch at all (a queued command's QUEUED reply, a rejected arity, a
// connection-control command) -- and per-command statistics that silently omitted
// those would misreport exactly the cases an operator is chasing.
//
// The returned *command is what executeCommand resolved, or nil if the name was not a
// command at all. Passing it back rather than looking it up again is what keeps the
// recording allocation-free: a second lookup would mean upper-casing the name again.
func (s *Server) execute(sess *session, w *resp.Writer, args [][]byte) {
	start, errs0 := observeStart(w)
	sess.blockedFor = 0
	cmd := s.executeCommand(sess, w, args)
	if cmd != nil {
		s.observeCommand(sess, cmd, args, start, errs0, w)
	}
	// The one-shot cluster flag is consumed by the command that followed the ASKING that
	// set it. Gated on the flag itself, so a connection that never sends ASKING -- which
	// is every connection outside a slot migration -- pays one boolean test.
	if sess.asking {
		clearAsking(sess, args)
	}
}

// executeCommand enforces the connection-level gates (authentication and subscriber
// mode), runs connection-control commands (including the transaction commands)
// directly, queues commands while in MULTI, and otherwise dispatches normally.
func (s *Server) executeCommand(sess *session, w *resp.Writer, args [][]byte) *command {
	name := strings.ToUpper(string(args[0]))

	// Resolve the connection's database once, here, and work through that view for the
	// rest of the command. Every handler then addresses the right keyspace without
	// knowing databases exist, and the paths that must record *which* database a change
	// happened in -- propagation, WATCH, keyspace notifications, blocked-client wakeups
	// -- read it off the view. SELECT itself is a connection-control command and runs on
	// whichever view this is, since it only changes what the next command resolves to.
	s = s.forDB(int(sess.db.Load()))

	// The command as the table knows it, resolved once. It is what the statistics are
	// recorded against, so a command refused by one of the gates below is still counted
	// -- as a rejected call, which is exactly the distinction an operator wants when a
	// client is being turned away.
	cmd := commandTable[name]

	// Authentication comes before everything, including the transaction commands: a
	// connection that cannot run a command must not be able to queue it either.
	if s.needsAuth(sess) && !noAuthCommands[name] {
		w.WriteError(errNoAuth)
		return reject(cmd)
	}
	// Subscriber mode (RESP2 only) leaves just the subscribe family plus PING/QUIT/
	// RESET. A RESP2 connection cannot tell a reply apart from a pushed message, so
	// allowing ordinary commands would leave the client unable to demultiplex its own
	// stream. RESP3 delivers messages as push frames, so the restriction does not
	// apply: a RESP3 client may stay subscribed and keep issuing ordinary commands.
	if w.Proto() < resp.ProtoRESP3 && sess.inSubscriberMode() && !subscriberCommands[name] {
		w.WriteError("ERR Can't execute '" + strings.ToLower(name) +
			"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
		return reject(cmd)
	}
	sess.lastActive.Store(time.Now().UnixNano())
	if cmd != nil {
		// The interned table name, so recording it for CLIENT LIST costs no allocation.
		sess.lastCmd.Store(&cmd.lowerName)
	}
	// Every monitor sees the command from here: past the gates (so a refused
	// authentication is not echoed as though it had run) and before it executes, which is
	// the order Redis feeds its own monitors in and the order that makes a monitor useful
	// for watching a command that then blocks.
	s.feedMonitors(sess, s.db, args)

	// In cluster mode a key's slot decides which node may serve it, so the decision comes
	// before anything runs and before anything is queued -- a MULTI must not accumulate
	// commands this node has no right to apply. It is one atomic load when cluster mode
	// is off, which is every standalone server.
	if s.ClusterEnabled() && !s.clusterGate(sess, w, cmd, args) {
		return reject(cmd)
	}

	// A connection-control command acts on this socket, so it runs now even inside a
	// MULTI: queueing an AUTH or a CLIENT SETNAME would defer a decision the rest of
	// the queue may depend on. The one exception is the subscribe family, which Redis
	// refuses inside a transaction -- subscriber mode changes what the connection is
	// allowed to run, so it cannot take effect halfway through a batch.
	if cmd != nil && cmd.sess != nil {
		if sess.inMulti && subscribeCommands[name] {
			sess.queueErr = true
			w.WriteError("ERR " + name + " is not allowed in transactions")
			return reject(cmd)
		}
		if !arityOK(cmd.arity, len(args)) {
			w.WriteError("ERR wrong number of arguments for '" + cmd.lowerName + "' command")
			return reject(cmd)
		}
		cmd.sess(s, sess, w, args)
		return cmd
	}

	if sess.inMulti {
		// Validate now so EXEC can abort cleanly; queue for later execution.
		if cmd == nil {
			sess.queueErr = true
			w.WriteError("ERR unknown command '" + string(args[0]) + "'")
			return nil
		}
		if !arityOK(cmd.arity, len(args)) {
			sess.queueErr = true
			w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
			return reject(cmd)
		}
		sess.queued = append(sess.queued, args)
		w.WriteSimple("QUEUED")
		// Not counted as a call: the command has not run, and it will be counted when EXEC
		// runs it. Counting it here would double every command inside a transaction.
		return nil
	}

	// A blocking command is the one dataset command that needs the connection behind
	// it: to park without holding any lock, to notice the client hanging up while it
	// waits, and to be distinguished from the same command inside a MULTI -- which is
	// handled above, where it was queued, and where it must not block at all.
	if cmd != nil && cmd.block != nil {
		if !s.dispatchBlocking(sess, w, cmd, args) {
			return nil // refused before running; already counted as a rejection
		}
		return cmd
	}

	if !s.dispatch(w, args) {
		return nil
	}
	return cmd
}

// reject records a command the server refused before running it and reports that there
// is nothing further to measure. A rejected call has no duration worth recording -- it
// never reached a handler -- so it is counted separately, which is what lets INFO
// commandstats distinguish "this command is slow" from "clients keep sending it wrong".
func reject(cmd *command) *command {
	if cmd != nil {
		cmd.rejected.Add(1)
	}
	return nil
}

func cmdMulti(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if sess.inMulti {
		w.WriteError("ERR MULTI calls can not be nested")
		return
	}
	sess.inMulti = true
	sess.queued = nil
	sess.queueErr = false
	w.WriteSimple("OK")
}

func cmdExec(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	s.execExec(sess, w)
}

func cmdDiscard(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if !sess.inMulti {
		w.WriteError("ERR DISCARD without MULTI")
		return
	}
	sess.inMulti = false
	sess.queued = nil
	sess.queueErr = false
	s.unwatchAll(sess)
	w.WriteSimple("OK")
}

// cmdWatch registers optimistic locks on the given keys. It is refused inside a
// transaction: the point of WATCH is to observe changes made *before* EXEC runs, and a
// WATCH taken after the batch was opened has nothing left to guard.
func cmdWatch(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if sess.inMulti {
		w.WriteError("ERR WATCH inside MULTI is not allowed")
		return
	}
	for _, k := range args[1:] {
		s.watchKey(sess, string(k))
	}
	w.WriteSimple("OK")
}

func cmdUnwatch(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	s.unwatchAll(sess)
	w.WriteSimple("OK")
}

func (s *Server) execExec(sess *session, w *resp.Writer) {
	if !sess.inMulti {
		w.WriteError("ERR EXEC without MULTI")
		return
	}
	queued := sess.queued
	queueErr := sess.queueErr
	aborted := sess.dirty.Load()

	// Reset transaction + watch state before running, so the batch's own writes
	// don't mark this session dirty.
	sess.inMulti = false
	sess.queued = nil
	sess.queueErr = false
	s.unwatchAll(sess)

	if queueErr {
		w.WriteError("EXECABORT Transaction discarded because of previous errors.")
		return
	}
	if aborted {
		w.WriteNullArray() // a watched key changed
		return
	}

	// When propagation is active, hold propMu across the whole batch so the
	// transaction applies and propagates as an atomic, totally-ordered unit
	// (other writers cannot interleave), and ship it framed in MULTI...EXEC.
	propagating := s.propagating.Load()
	if propagating {
		s.propMu.Lock()
		defer func() {
			if propagating {
				s.propMu.Unlock()
			}
		}()
	}

	// One clock reading for the whole batch: the transaction applies as a single
	// instant, and reading the store's clock (not time.Now) keeps the deadlines it
	// propagates identical to the ones its handlers wrote to memory.
	now := s.store.Now()

	w.WriteArrayHeader(len(queued))
	var batch [][][]byte
	var changed [][][]byte
	for _, cmd := range queued {
		name := strings.ToUpper(string(cmd[0]))
		c := commandTable[name] // validated when queued
		// Each queued command is measured in its own right, as Redis measures it: EXEC's
		// own entry in commandstats then reads as the cost of the batch as a unit while the
		// members still report their individual latencies.
		cmdStart, cmdErrs := observeStart(w)
		dirty, effect := c.apply(s, w, cmd)
		c.record(time.Since(cmdStart), w.ErrorsWritten() != cmdErrs)
		if c.write && dirty {
			// A blocking command inside a transaction reports what it actually did, for the
			// same reason it does outside one: its own arguments name candidate keys, its
			// effect names the key that changed. (It cannot have blocked -- see apply.)
			observed := [][][]byte{cmd}
			if c.block != nil {
				observed = effect
			}
			for _, o := range observed {
				s.touchWatchers(o)
				s.notifyWrite(o)
			}
			changed = append(changed, observed...)
			if propagating {
				batch = append(batch, wireForm(cmd, effect, now)...)
			}
		}
	}
	if propagating {
		s.propagateBatch(batch)
		s.propMu.Unlock()
		propagating = false // the deferred unlock must not run twice
	}
	// Outside the ordering lock, for the reason runWrite gives: a woken client needs
	// propMu for its own pop, and a transaction can push to a key several clients are
	// blocked on.
	s.signalWaiters(changed)
}

// --- WATCH registry ----------------------------------------------------------

// watchKey registers an optimistic lock on key *in this view's database*. A client
// that SELECTs another database and writes the same key name changes a different key,
// so the registration is keyed by both.
func (s *Server) watchKey(sess *session, key string) {
	k := dbKey{db: s.db, key: key}
	if sess.watched == nil {
		sess.watched = make(map[dbKey]struct{})
	}
	if _, ok := sess.watched[k]; ok {
		return
	}
	sess.watched[k] = struct{}{}

	s.watchMu.Lock()
	if s.watchers[k] == nil {
		s.watchers[k] = make(map[*session]struct{})
	}
	s.watchers[k][sess] = struct{}{}
	s.watchMu.Unlock()
}

// unwatchAll removes all of the session's watches and clears its dirty flag.
func (s *Server) unwatchAll(sess *session) {
	if len(sess.watched) > 0 {
		s.watchMu.Lock()
		for k := range sess.watched {
			if m := s.watchers[k]; m != nil {
				delete(m, sess)
				if len(m) == 0 {
					delete(s.watchers, k)
				}
			}
		}
		s.watchMu.Unlock()
		sess.watched = nil
	}
	sess.dirty.Store(false)
}

// touchWatchers marks every session watching an affected key as dirty, so a
// pending EXEC will abort. FLUSHALL invalidates all watchers.
func (s *Server) touchWatchers(args [][]byte) {
	name := strings.ToUpper(string(args[0]))

	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if len(s.watchers) == 0 {
		return
	}
	// FLUSHALL empties every database, so every WATCH anywhere is invalidated;
	// FLUSHDB empties one, so only that database's watchers are.
	if name == "FLUSHALL" {
		for _, sessions := range s.watchers {
			for sess := range sessions {
				sess.dirty.Store(true)
			}
		}
		return
	}
	if name == "FLUSHDB" {
		for k, sessions := range s.watchers {
			if k.db != s.db {
				continue
			}
			for sess := range sessions {
				sess.dirty.Store(true)
			}
		}
		return
	}
	// SWAPDB replaces the contents of two whole databases, so every WATCH in either of
	// them is invalidated. There is no key-level answer available: a watcher of a key
	// that existed in neither database before the swap may now be watching a value.
	if name == "SWAPDB" && len(args) == 3 {
		i, ok1 := parseInt64(args[1])
		j, ok2 := parseInt64(args[2])
		if !ok1 || !ok2 {
			return // rejected by the handler, so nothing was swapped
		}
		for k, sessions := range s.watchers {
			if int64(k.db) != i && int64(k.db) != j {
				continue
			}
			for sess := range sessions {
				sess.dirty.Store(true)
			}
		}
		return
	}
	for _, k := range s.affectedDBKeys(name, args) {
		for sess := range s.watchers[k] {
			sess.dirty.Store(true)
		}
	}
}

// affectedDBKeys returns every (database, key) a write touches: affectedKeys tagged
// with the database the write happened in, plus the second database for the two
// commands that reach into one.
//
// MOVE and COPY ... DB are the only writes whose consequences leave the caller's
// database. Forgetting them here would leave a client watching -- or blocked on -- the
// destination key in the destination database with no idea it had appeared, which is
// invariant 7 one database over.
func (s *Server) affectedDBKeys(name string, args [][]byte) []dbKey {
	keys := affectedKeys(name, args)
	out := make([]dbKey, 0, len(keys)+1)
	for _, k := range keys {
		out = append(out, dbKey{db: s.db, key: k})
	}
	if other, key, ok := crossDBTarget(name, args); ok {
		out = append(out, dbKey{db: other, key: key})
	}
	return out
}

// crossDBTarget reports the other database a write reaches into, and the key it writes
// there: the destination of a MOVE, or of a COPY carrying a DB option. ok is false for
// every other command, and for these two when the option names the caller's own
// database (where the write is not cross-database at all).
func crossDBTarget(name string, args [][]byte) (db int, key string, ok bool) {
	switch name {
	case "MOVE":
		if len(args) != 3 {
			return 0, "", false
		}
		n, valid := parseInt64(args[2])
		if !valid {
			return 0, "", false
		}
		return int(n), string(args[1]), true
	case "COPY":
		if len(args) < 3 {
			return 0, "", false
		}
		for i := 3; i+1 < len(args); i++ {
			if !strings.EqualFold(string(args[i]), "DB") {
				continue
			}
			n, valid := parseInt64(args[i+1])
			if !valid {
				return 0, "", false
			}
			return int(n), string(args[2]), true
		}
	}
	return 0, "", false
}

// affectedKeys returns the keys a write command modifies, for WATCH invalidation.
//
// Every multi-key write must be listed here. The default -- the command's first
// argument -- is right for the large majority, but a command that also writes a
// second key (COPY, SMOVE, LMOVE, an interstore) would otherwise never invalidate a
// WATCH on that destination, and a transaction guarding it would commit on top of a
// concurrent change.
func affectedKeys(name string, args [][]byte) []string {
	switch name {
	case "MSET", "MSETNX":
		var ks []string
		for i := 1; i+1 < len(args); i += 2 {
			ks = append(ks, string(args[i]))
		}
		return ks
	case "DEL", "UNLINK", "TOUCH":
		ks := make([]string, 0, len(args)-1)
		for _, k := range args[1:] {
			ks = append(ks, string(k))
		}
		return ks
	case "XGROUP":
		// XGROUP <subcommand> <key> ...: the key is at argument 2, not 1, because the
		// subcommand takes the position every other command gives its first key. Missing
		// this would leave a client blocked on XREADGROUP unwoken by the XGROUP SETID an
		// XREADGROUP's own effect ships, and a WATCH on the stream unaware the group moved.
		if len(args) >= 3 {
			return []string{string(args[2])}
		}
		return nil
	case "BITOP":
		// BITOP <op> <dest> <src...>: only the destination is written, and it is at
		// argument 2 for the same reason.
		if len(args) >= 3 {
			return []string{string(args[2])}
		}
		return nil
	case "MIGRATE":
		// MIGRATE host port key|"" db timeout ... [KEYS key ...]: the first three arguments
		// are an address, so the default of "argument 1 is the key" would name the
		// destination *host* as the key -- and a WATCH on a key this command deletes would
		// never see it go. The keys are either the one at argument 3 or, when that is the
		// empty string, everything after a KEYS keyword, which is why this cannot be
		// expressed as a fixed first/last/step triple either.
		//
		// They are reported even for MIGRATE ... COPY, which deletes nothing. Over-reporting
		// costs a spurious WATCH invalidation; under-reporting loses a real one, and only
		// one of those two failures is silent.
		return migrateKeys(args)
	case "RENAME", "RENAMENX", "COPY", "SMOVE", "LMOVE", "RPOPLPUSH",
		"BLMOVE", "BRPOPLPUSH":
		// The first two arguments are source and destination for all of these; the
		// options that follow (COPY's REPLACE, LMOVE's directions, a blocking form's
		// timeout) are not keys. The blocking pair reports its keys through its effect,
		// which is the non-blocking LMOVE/RPOPLPUSH, so these two entries are belt and
		// braces -- but invariant 7 is about the list being complete, not about which
		// path happens to consult it today.
		if len(args) >= 3 {
			return []string{string(args[1]), string(args[2])}
		}
		return nil
	default:
		// SINTERSTORE and its siblings need no case of their own: the only key they
		// modify is the destination, which is already the first argument. Their
		// sources are read, not written, so a WATCH on one is not in conflict.
		if len(args) >= 2 {
			return []string{string(args[1])}
		}
		return nil
	}
}
