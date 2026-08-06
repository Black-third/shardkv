package server

import (
	"strconv"
)

// This file holds the wire forms of the commands whose own text is not replayable
// because it names a deadline *relative to now*: the expire family, SET's expiring
// options, SETEX/PSETEX and GETEX. Each is rewritten to an absolute deadline before it
// reaches the AOF or a replica, so a replay reconstructs the same expiry instant however
// much later it happens. Real Redis rewrites the same commands the same way
// (EXPIRE -> PEXPIREAT, SET EX -> SET PXAT). RESTORE's ttl operand is the same case and
// its renderer lives beside its handler, in commands_dump.go.
//
// The conversion needs a reading of the clock, and the load-bearing property of this
// file is that it does not take one: every builder here is given the absolute deadline
// its handler already resolved and already wrote to memory.
//
// That is not a stylistic preference. The rewrite used to re-derive the deadline from a
// *second* reading of the store's clock, taken after the handler had run. Both readings
// came from the same clock -- which is what the code claimed made them equal -- but they
// were two readings separated by the handler's own execution time, so the deadline on the
// wire was later than the deadline in memory by however long the write took, and every
// replica outlived its master on every volatile key. Nothing reported it, because both
// copies were internally consistent. Under a store clock that advances on each reading
// the skew is exact and testable; see TestExpiryDeadlineTakesExactlyOneClockReading.
//
// So each of these commands is registered with registerEffect and returns the form built
// here, from its own single reading (`o.atMs`, `deadlineMs(...)`). There is consequently
// no clock anywhere on the propagation path -- wireForm takes no time argument at all --
// which is what makes "one reading per command" true by construction rather than by two
// call sites agreeing to be careful.
//
// Conditional flags are dropped along the way. A command only produces an effect once the
// master has decided it applies, so re-evaluating NX/XX/GT/LT against the replica's own
// copy could only reject a write the master accepted.

// pexpireatForm renders the absolute PEXPIREAT that the whole expire family propagates
// as, and that GETEX's expiring form propagates as too.
func pexpireatForm(key []byte, atMs int64) [][]byte {
	return [][]byte{[]byte("PEXPIREAT"), key, []byte(strconv.FormatInt(atMs, 10))}
}

// setWireForm renders SET's canonical propagated shape: the value, plus at most an
// absolute PXAT deadline or KEEPTTL. o is the option tail its handler parsed, so atMs is
// the very instant the store was given.
//
// The conditional flags are dropped for the reason above, and GET is dropped because it
// only shapes the reply. What remains has to be exact about the TTL: a SET that reached
// memory with KEEPTTL must not clear the replica's TTL, and one with no expiry option
// must.
//
// The head is re-sliced with its capacity clamped, so appending the tail copies rather
// than writing into the caller's argument slice -- which belongs to the client's request
// buffer, and whose fourth element is still an option word the wire form must not carry.
func setWireForm(args [][]byte, o setOpts) [][]byte {
	head := args[:3:3]
	switch {
	case o.hasDeadline:
		return append(head, []byte("PXAT"), []byte(strconv.FormatInt(o.atMs, 10)))
	case o.keepTTL:
		return append(head, []byte("KEEPTTL"))
	}
	return head
}

// setexWireForm renders SETEX/PSETEX as the same absolute SET the expiring forms of SET
// propagate as, so a replay has one shape to reconstruct rather than three.
func setexWireForm(key, val []byte, atMs int64) [][]byte {
	return [][]byte{[]byte("SET"), key, val, []byte("PXAT"), []byte(strconv.FormatInt(atMs, 10))}
}

// getexWireForm renders GETEX as the expiry change it made, which is all a replica or an
// AOF needs: the value it returned was already in the dataset. Redis propagates the same
// PEXPIREAT/PERSIST pair. It is only reached when the expiry actually moved, so o.apply
// is implied.
func getexWireForm(key []byte, o getExOpts) [][]byte {
	if o.persist {
		return [][]byte{[]byte("PERSIST"), key}
	}
	return pexpireatForm(key, o.atMs)
}
