package server

import (
	"strconv"
	"strings"
	"time"
)

// propagationForm rewrites a write command into the form that should be
// persisted to the AOF and shipped to replicas. Relative-expiry commands are
// converted to absolute deadlines so a replica or an AOF replay reconstructs the
// same expiry instant regardless of when it applies the command (real Redis does
// the same: EXPIRE -> PEXPIREAT, SET EX -> SET PXAT). All other commands
// propagate verbatim.
//
// now must be the same clock the handler resolved its own deadline against (the
// store's), and the deadline arithmetic is the same overflow-checked deadlineMs,
// so the instant in memory and the instant on the wire cannot differ. An operand
// the handler rejected propagates verbatim, since nothing was applied.
//
// Conditional flags are dropped along the way. A command only reaches here once the
// master has decided it applies, so re-evaluating NX/XX/GT/LT against the replica's
// own copy could only reject a write the master accepted.
func propagationForm(args [][]byte, now time.Time) [][]byte {
	nowMs := now.UnixMilli()
	switch strings.ToUpper(string(args[0])) {
	case "EXPIRE":
		return expireatForm(args, nowMs, 1000, true)
	case "PEXPIRE":
		return expireatForm(args, nowMs, 1, true)
	case "EXPIREAT":
		return expireatForm(args, nowMs, 1000, false)
	case "PEXPIREAT":
		// Already absolute; only a trailing NX/XX/GT/LT flag needs stripping.
		if len(args) > 3 {
			return expireatForm(args, nowMs, 1, false)
		}
	case "SET":
		return setForm(args, nowMs)
	case "SETEX":
		// SETEX key seconds value -> SET key value PXAT deadline
		return setexForm(args, nowMs, 1000)
	case "PSETEX":
		return setexForm(args, nowMs, 1)
	case "GETEX":
		return getexForm(args, nowMs)
	case "RESTORE":
		return restoreForm(args, nowMs)
	}
	return args
}

// expireatForm rewrites one of the relative/second-granularity expire commands
// into the absolute PEXPIREAT the AOF and replicas should see.
func expireatForm(args [][]byte, nowMs, unitMs int64, rel bool) [][]byte {
	n, ok := parseInt64(args[2])
	if !ok {
		return args
	}
	atMs, ok := deadlineMs(nowMs, n, unitMs, rel)
	if !ok {
		return args
	}
	return pexpireat(args[1], atMs)
}

// setForm rewrites SET into its canonical propagated shape: the value, plus at
// most an absolute PXAT deadline or KEEPTTL.
//
// The conditional flags are dropped for the reason above, and GET is dropped
// because it only shapes the reply. What remains has to be exact about the TTL: a
// SET that reached memory with KEEPTTL must not clear the replica's TTL, and one
// with no expiry option must.
func setForm(args [][]byte, nowMs int64) [][]byte {
	o, errMsg := parseSetOpts(args, nowMs)
	if errMsg != "" {
		return args // rejected by the handler; nothing was applied
	}
	out := [][]byte{args[0], args[1], args[2]}
	switch {
	case o.hasDeadline:
		out = append(out, []byte("PXAT"), []byte(strconv.FormatInt(o.atMs, 10)))
	case o.keepTTL:
		out = append(out, []byte("KEEPTTL"))
	}
	return out
}

// setexForm rewrites SETEX/PSETEX (key, ttl, value) into the same absolute SET the
// expiring forms of SET propagate as, so replay has one shape to reconstruct.
func setexForm(args [][]byte, nowMs, unitMs int64) [][]byte {
	n, ok := parseInt64(args[2])
	if !ok {
		return args
	}
	atMs, ok := deadlineMs(nowMs, n, unitMs, true)
	if !ok {
		return args
	}
	return [][]byte{[]byte("SET"), args[1], args[3], []byte("PXAT"), []byte(strconv.FormatInt(atMs, 10))}
}

// getexForm rewrites GETEX into the expiry change it made, which is all a replica
// or an AOF needs: the value it returned was already in the dataset. Redis
// propagates the same PEXPIREAT/PERSIST pair.
func getexForm(args [][]byte, nowMs int64) [][]byte {
	o, errMsg := parseGetExOpts(args, nowMs)
	if errMsg != "" || !o.apply {
		return args
	}
	if o.persist {
		return [][]byte{[]byte("PERSIST"), args[1]}
	}
	return pexpireat(args[1], o.atMs)
}

func pexpireat(key []byte, atMs int64) [][]byte {
	return [][]byte{[]byte("PEXPIREAT"), key, []byte(strconv.FormatInt(atMs, 10))}
}
