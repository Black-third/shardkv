package server

import (
	"math"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("LPUSH", -3, true, cmdLPush)
	register("RPUSH", -3, true, cmdRPush)
	register("LPUSHX", -3, true, cmdLPushX)
	register("RPUSHX", -3, true, cmdRPushX)
	register("LPOP", -2, true, cmdLPop)
	register("RPOP", -2, true, cmdRPop)
	register("LRANGE", 4, false, cmdLRange)
	register("LLEN", 2, false, cmdLLen)
	register("LINDEX", 3, false, cmdLIndex)
	register("LSET", 4, true, cmdLSet)
	register("LINSERT", 5, true, cmdLInsert)
	register("LREM", 4, true, cmdLRem)
	register("LTRIM", 4, true, cmdLTrim)
	register("LPOS", -3, false, cmdLPos)
	register("RPOPLPUSH", 3, true, cmdRPopLPush)
	register("LMOVE", 5, true, cmdLMove)
}

func cmdLPush(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.LPush(string(args[1]), args[2:]...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return true
}

func cmdRPush(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.RPush(string(args[1]), args[2:]...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return true
}

// cmdLPushX and cmdRPushX push only onto a list that already exists, replying with 0 and
// creating nothing otherwise. The reply is the new length either way, so a caller can tell
// "I appended" from "there was no queue" without a second round trip.
//
// They report dirty only when they actually pushed -- the whole point of the conditional
// form is that it often does nothing, and a no-op that propagated would put an LPUSHX in
// the AOF that recreates nothing but costs a record to replay (invariant 2).
func cmdLPushX(s *Server, w *resp.Writer, args [][]byte) bool {
	return pushX(s, w, args, true)
}

func cmdRPushX(s *Server, w *resp.Writer, args [][]byte) bool {
	return pushX(s, w, args, false)
}

func pushX(s *Server, w *resp.Writer, args [][]byte, left bool) bool {
	var n int
	var err error
	if left {
		n, err = s.store.LPushX(string(args[1]), args[2:]...)
	} else {
		n, err = s.store.RPushX(string(args[1]), args[2:]...)
	}
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return n > 0
}

func cmdLPop(s *Server, w *resp.Writer, args [][]byte) bool {
	return listPop(s, w, args, true)
}

func cmdRPop(s *Server, w *resp.Writer, args [][]byte) bool {
	return listPop(s, w, args, false)
}

// listPop applies LPOP/RPOP in both their forms. Without a count the reply is the
// element itself (or a null bulk string); with one it is an array of up to count
// elements -- and a null *array* for a missing key, which is how a client tells
// "the list is gone" from "the list is there but you asked for zero".
//
// The count form propagates verbatim: it pops from a named end, so a replica
// applying it removes exactly the same elements.
func listPop(s *Server, w *resp.Writer, args [][]byte, left bool) bool {
	if len(args) > 3 {
		w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(string(args[0])) + "' command")
		return false
	}
	key := string(args[1])
	if len(args) == 2 {
		var v []byte
		var ok bool
		var err error
		if left {
			v, ok, err = s.store.LPop(key)
		} else {
			v, ok, err = s.store.RPop(key)
		}
		if err != nil {
			writeStoreErr(w, err)
			return false
		}
		if !ok {
			w.WriteNull()
			return false
		}
		w.WriteBulk(v)
		return true
	}

	count, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if count < 0 {
		w.WriteError("ERR value is out of range, must be positive")
		return false
	}
	vals, exists, err := s.store.LPopCount(key, count, left)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !exists {
		w.WriteNullArray()
		return false
	}
	w.WriteArrayHeader(len(vals))
	for _, v := range vals {
		w.WriteBulk(v)
	}
	return len(vals) > 0
}

func cmdLRange(s *Server, w *resp.Writer, args [][]byte) bool {
	start, ok1 := parseInt(args[2])
	stop, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	items, err := s.store.LRange(string(args[1]), start, stop)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(items))
	for _, v := range items {
		w.WriteBulk(v)
	}
	return false
}

func cmdLLen(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.LLen(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

func cmdLIndex(s *Server, w *resp.Writer, args [][]byte) bool {
	index, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	v, found, err := s.store.LIndex(string(args[1]), index)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !found {
		w.WriteNull()
		return false
	}
	w.WriteBulk(v)
	return false
}

func cmdLSet(s *Server, w *resp.Writer, args [][]byte) bool {
	index, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if err := s.store.LSet(string(args[1]), index, args[3]); err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteSimple("OK")
	return true
}

func cmdLInsert(s *Server, w *resp.Writer, args [][]byte) bool {
	var before bool
	switch strings.ToUpper(string(args[2])) {
	case "BEFORE":
		before = true
	case "AFTER":
	default:
		w.WriteError("ERR syntax error")
		return false
	}
	n, err := s.store.LInsert(string(args[1]), before, args[3], args[4])
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return n > 0
}

func cmdLRem(s *Server, w *resp.Writer, args [][]byte) bool {
	count, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	removed, err := s.store.LRem(string(args[1]), count, args[3])
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(removed))
	return removed > 0
}

// cmdLTrim replies OK even for a missing key or a range that selects nothing --
// LTRIM is defined as "make the list this range", and an empty result deletes the
// key.
func cmdLTrim(s *Server, w *resp.Writer, args [][]byte) bool {
	start, ok1 := parseInt(args[2])
	stop, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	changed, err := s.store.LTrim(string(args[1]), start, stop)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteSimple("OK")
	return changed
}

// cmdLPos implements LPOS key element [RANK n] [COUNT n] [MAXLEN n]. Without COUNT
// the reply is a single index or a null; with it, an array (empty when nothing
// matched, never a null).
func cmdLPos(s *Server, w *resp.Writer, args [][]byte) bool {
	rank, count, maxlen := 1, 0, 0
	hasCount := false
	if (len(args)-3)%2 != 0 {
		w.WriteError("ERR syntax error")
		return false
	}
	for i := 3; i < len(args); i += 2 {
		n, ok := parseInt(args[i+1])
		if !ok {
			w.WriteError("ERR value is not an integer or out of range")
			return false
		}
		switch strings.ToUpper(string(args[i])) {
		case "RANK":
			if n == 0 {
				w.WriteError("ERR RANK can't be zero: use 1 to start from the first match, " +
					"2 from the second ... or use negative to start from the end of the list")
				return false
			}
			// A negative rank counts from the tail, so the search negates it -- and the smallest
			// int64 negates to itself. Redis refuses that one value by name for exactly this
			// reason, and so does every other command here that takes a count it will negate
			// (see parseRandomCount).
			if n == math.MinInt {
				w.WriteError("ERR value is out of range, value must between " +
					"-9223372036854775807 and 9223372036854775807")
				return false
			}
			rank = n
		case "COUNT":
			if n < 0 {
				w.WriteError("ERR COUNT can't be negative")
				return false
			}
			count, hasCount = n, true
		case "MAXLEN":
			if n < 0 {
				w.WriteError("ERR MAXLEN can't be negative")
				return false
			}
			maxlen = n
		default:
			w.WriteError("ERR syntax error")
			return false
		}
	}
	// Without COUNT only the first match is wanted, whatever the list holds.
	limit := count
	if !hasCount {
		limit = 1
	}
	found, err := s.store.LPos(string(args[1]), args[2], rank, limit, maxlen)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !hasCount {
		if len(found) == 0 {
			w.WriteNull()
			return false
		}
		w.WriteInt(int64(found[0]))
		return false
	}
	w.WriteArrayHeader(len(found))
	for _, i := range found {
		w.WriteInt(int64(i))
	}
	return false
}

func cmdRPopLPush(s *Server, w *resp.Writer, args [][]byte) bool {
	return listMove(s, w, string(args[1]), string(args[2]), false, true)
}

// cmdLMove implements LMOVE src dst LEFT|RIGHT LEFT|RIGHT.
func cmdLMove(s *Server, w *resp.Writer, args [][]byte) bool {
	srcLeft, ok1 := listSide(args[3])
	dstLeft, ok2 := listSide(args[4])
	if !ok1 || !ok2 {
		w.WriteError("ERR syntax error")
		return false
	}
	return listMove(s, w, string(args[1]), string(args[2]), srcLeft, dstLeft)
}

func listSide(b []byte) (left, ok bool) {
	switch strings.ToUpper(string(b)) {
	case "LEFT":
		return true, true
	case "RIGHT":
		return false, true
	}
	return false, false
}

// listMove is the shared body of LMOVE and RPOPLPUSH. Both propagate verbatim: the
// ends are named in the command, so a replica moves the very same element. The
// destination is registered in affectedKeys, so a WATCH on it sees the conflict.
func listMove(s *Server, w *resp.Writer, src, dst string, srcLeft, dstLeft bool) bool {
	v, ok, err := s.store.LMove(src, dst, srcLeft, dstLeft)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteBulk(v)
	return true
}
