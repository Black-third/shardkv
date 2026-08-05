package server

// The redirect path: deciding, for every command a client sends, whether this node may
// serve it.
//
// This is the invariant cluster mode adds to CLAUDE.md's list: **a key's slot decides
// which node may serve it.** A node that answers for a slot it does not own does not
// produce an error -- it produces a second copy of the data, on a node no client will
// look at again once the slot map is consulted, and nothing anywhere reports it. That
// is the same failure shape as every other invariant in this codebase: silent
// divergence rather than a crash.
//
// # One source of truth for keys
//
// The keys are taken from commandKeys, which is the extraction COMMAND GETKEYS answers
// with and which falls through to affectedKeys -- the list WATCH depends on (invariant
// 7). That is deliberate and it is the same argument invariant 7 makes: a second list
// of "which arguments are keys", maintained for routing, would drift from the first,
// and the drift would be silent in both directions. A command missing from the routing
// list would be served by the wrong node; a command missing from the WATCH list would
// commit over a concurrent change. Sharing the extraction means a new multi-key command
// gets both for free, and a mistake in it is caught by either set of tests.
//
// # Cost when cluster mode is off
//
// One atomic load. clusterRedirect is called behind s.ClusterEnabled(), so a standalone
// server does not compute a slot, does not build a key slice, and does not touch the
// slot map -- the same discipline invariant 12 imposes on the observability hooks.

import (
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

// The redirection replies. Each is Redis's exact text, because a client library
// dispatches on the prefix and then parses the rest: MOVED and ASK carry a slot and an
// address a client is expected to act on without human help.
const (
	errCrossSlot   = "CROSSSLOT Keys in request don't hash to the same slot"
	errSlotUnbound = "CLUSTERDOWN Hash slot not served"
	errTryAgain    = "TRYAGAIN Multiple keys request during rehashing of slot"
)

// clusterRedirect reports the error this node must answer with instead of running the
// command, or "" when it may serve it.
//
// The order of the checks is Redis's, and each step is a different question:
//
//  1. Which slots does this command name? None -> serve (it is not a keyed command).
//  2. Do they all agree? No -> CROSSSLOT, because no single node owns both.
//  3. Does anyone own that slot? No -> CLUSTERDOWN.
//  4. Is the slot open (being migrated), and if so where are the keys?
//  5. Otherwise: mine -> serve, someone else's -> MOVED.
func (s *Server) clusterRedirect(sess *session, name string, args [][]byte) string {
	keys := s.redirectKeys(sess, name, args)
	if len(keys) == 0 {
		// A command that names no key belongs to whichever node the client sent it to:
		// PING, INFO, the Pub/Sub family, SCAN and the whole administrative surface act on
		// this node, not on a slot.
		return ""
	}
	slot := KeySlot(keys[0])
	for _, k := range keys[1:] {
		if KeySlot(k) != slot {
			return errCrossSlot
		}
	}

	cs := s.cluster
	info := cs.slot(slot)
	me := cs.myself()

	// MIGRATE is the command that moves keys out of an open slot, so it must run locally
	// on whichever side of the migration it was sent to -- redirecting it would send the
	// migration itself in a circle.
	if name == "MIGRATE" && (info.migrating != nil || info.importing != nil) {
		return ""
	}
	if info.owner == nil {
		return errSlotUnbound
	}

	// While a slot is open, "do I have this key?" is what decides between serving and
	// redirecting, so it is counted once here for the two checks below.
	var missing, existing int
	if info.migrating != nil || info.importing != nil {
		for _, k := range keys {
			if s.store.Exists(k) {
				existing++
			} else {
				missing++
			}
		}
	}
	multipleKeys := len(keys) > 1

	// The source of a migration: this node still owns the slot, but a key it has already
	// handed over is no longer here. That key is at the target, so the client is told
	// where -- with ASK rather than MOVED, because ownership has not moved yet and the
	// client must not update its routing table.
	if info.owner == me && info.migrating != nil && missing > 0 {
		if multipleKeys && existing > 0 {
			// Some of the command's keys are here and some have already gone. No single node
			// can serve it right now, and neither redirect would be true, so the client is
			// asked to retry once the slot has settled.
			return errTryAgain
		}
		return "ASK " + strconv.Itoa(slot) + " " + info.migrating.addr()
	}

	// The target of a migration: this node does not own the slot yet, but a client that
	// was ASKed here says so with ASKING, and the key it wants has already arrived.
	if info.importing != nil && sess != nil && sess.asking {
		if multipleKeys && missing > 0 {
			return errTryAgain
		}
		return ""
	}

	if info.owner == me {
		return ""
	}
	// A replica may serve reads for its master's slots to a client that opted in with
	// READONLY. Writes are refused on a replica anyway (errReadOnly), so this can only
	// ever widen reads.
	if sess != nil && sess.readReplica && me.isReplica() && me.replicaOf == info.owner.id {
		if cmd, ok := commandTable[name]; ok && !cmd.write {
			return ""
		}
	}
	return "MOVED " + strconv.Itoa(slot) + " " + info.owner.addr()
}

// redirectKeys returns the keys a command routes on.
//
// EXEC is the one command whose keys are not in its own arguments: a transaction is
// checked as a unit, against every key its queued commands name, because the batch runs
// on one node or not at all. Redis checks it the same way and for the same reason -- an
// EXEC whose members straddled two slots would be a transaction that could only ever
// half-apply.
func (s *Server) redirectKeys(sess *session, name string, args [][]byte) []string {
	if name != "EXEC" {
		return commandKeys(name, args)
	}
	if sess == nil || !sess.inMulti {
		return nil
	}
	var keys []string
	for _, queued := range sess.queued {
		keys = append(keys, commandKeys(strings.ToUpper(string(queued[0])), queued)...)
	}
	return keys
}

// clusterGate applies the redirect to a client command, writing the reply itself when
// the command must not run here. It reports whether the caller may proceed.
//
// A redirected command inside a transaction also has to poison the batch: the client is
// about to send more commands and then EXEC, and an EXEC that ran the rest of a batch
// whose first member had been redirected would apply a fragment of what the client
// asked for. Redis flags the transaction for a queued command and discards it outright
// for an EXEC, and so does this.
func (s *Server) clusterGate(sess *session, w *resp.Writer, cmd *command, args [][]byte) bool {
	name := strings.ToUpper(string(args[0]))
	// An unknown command or a wrong argument count is reported as such rather than
	// redirected: "unknown command" is the more useful answer, and a command whose
	// arguments did not parse has no reliable keys to route on. Redis checks both before
	// its own redirection for the same reason.
	if cmd == nil || !arityOK(cmd.arity, len(args)) {
		return true
	}
	errMsg := s.clusterRedirect(sess, name, args)
	if errMsg == "" {
		return true
	}
	if name == "EXEC" {
		sess.inMulti = false
		sess.queued = nil
		sess.queueErr = false
		s.unwatchAll(sess)
	} else if sess.inMulti {
		sess.queueErr = true
	}
	w.WriteError(errMsg)
	return false
}

// clearAsking consumes the one-shot ASKING flag, which is set for exactly one command.
//
// It survives ASKING itself (which sets it) and everything queued inside a MULTI: a
// client that sends ASKING before opening a transaction means the flag for the batch,
// and the batch's own redirect decision is taken at EXEC. Everything else consumes it,
// because ownership of the slot has not changed and a flag that persisted would let an
// importing node serve a slot it does not own -- exactly the split-brain the migration
// protocol exists to prevent.
func clearAsking(sess *session, args [][]byte) {
	if sess.inMulti || strings.EqualFold(string(args[0]), "ASKING") {
		return
	}
	sess.asking = false
}

// errClusterNoDB is the refusal every command that names a second database gets in
// cluster mode. A cluster is a partition of one keyspace across nodes: a second
// database would be a keyspace with no slots, so no node would be responsible for it
// and no client could be told where it lives.
func errClusterNoDB(name string) string {
	return "ERR " + name + " is not allowed in cluster mode"
}
