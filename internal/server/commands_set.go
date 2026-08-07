package server

import (
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("SADD", -3, true, cmdSAdd)
	register("SREM", -3, true, cmdSRem)
	register("SMEMBERS", 2, false, cmdSMembers)
	register("SISMEMBER", 3, false, cmdSIsMember)
	register("SMISMEMBER", -3, false, cmdSMIsMember)
	register("SCARD", 2, false, cmdSCard)
	registerEffect("SPOP", -2, cmdSPop)
	register("SRANDMEMBER", -2, false, cmdSRandMember)
	register("SMOVE", 4, true, cmdSMove)
	register("SINTER", -2, false, cmdSInter)
	register("SUNION", -2, false, cmdSUnion)
	register("SDIFF", -2, false, cmdSDiff)
	register("SINTERSTORE", -3, true, cmdSInterStore)
	register("SUNIONSTORE", -3, true, cmdSUnionStore)
	register("SDIFFSTORE", -3, true, cmdSDiffStore)
	register("SINTERCARD", -3, false, cmdSInterCard)
}

func cmdSAdd(s *Server, w *resp.Writer, args [][]byte) bool {
	added, err := s.store.SAdd(string(args[1]), byteStrings(args[2:])...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(added))
	return added > 0
}

func cmdSRem(s *Server, w *resp.Writer, args [][]byte) bool {
	removed, err := s.store.SRem(string(args[1]), byteStrings(args[2:])...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(removed))
	return removed > 0
}

func cmdSMembers(s *Server, w *resp.Writer, args [][]byte) bool {
	members, err := s.store.SMembers(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeSet(w, members)
	return false
}

func cmdSIsMember(s *Server, w *resp.Writer, args [][]byte) bool {
	ok, err := s.store.SIsMember(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(boolToInt(ok)))
	return false
}

// cmdSMIsMember answers one 0/1 per requested member, in order, so a client can
// test many memberships in one round trip.
func cmdSMIsMember(s *Server, w *resp.Writer, args [][]byte) bool {
	found, err := s.store.SMIsMember(string(args[1]), byteStrings(args[2:])...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(found))
	for _, ok := range found {
		w.WriteInt(int64(boolToInt(ok)))
	}
	return false
}

func cmdSCard(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.SCard(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

// cmdSPop implements SPOP key [count], and is the reason effect propagation exists.
// The members removed are chosen by map iteration order, so shipping "SPOP key 3"
// to a replica would have it remove three different members and diverge forever.
// What ships instead is the SREM of exactly the members this master removed -- the
// same rewrite real Redis performs.
func cmdSPop(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	if len(args) > 3 {
		w.WriteError("ERR wrong number of arguments for 'spop' command")
		return nil
	}
	count, single := 1, len(args) == 2
	if !single {
		n, ok := parseInt(args[2])
		if !ok {
			w.WriteError("ERR value is not an integer or out of range")
			return nil
		}
		if n < 0 {
			w.WriteError("ERR value is out of range, must be positive")
			return nil
		}
		count = n
	}
	members, exists, err := s.store.SPop(string(args[1]), count)
	if err != nil {
		writeStoreErr(w, err)
		return nil
	}
	switch {
	case single && len(members) == 0:
		w.WriteNull() // no key: a bare SPOP replies with a null, not an empty array
	case single:
		w.WriteBulkString(members[0])
	case !exists:
		w.WriteSetHeader(0) // SPOP with a count on a missing key is an empty set
	default:
		writeSet(w, members)
	}
	if len(members) == 0 {
		return nil
	}
	effect := [][]byte{[]byte("SREM"), args[1]}
	for _, m := range members {
		effect = append(effect, []byte(m))
	}
	return [][][]byte{effect}
}

// cmdSRandMember implements SRANDMEMBER key [count]. Nothing is propagated: it is a
// read, and a replica running it would draw different members.
//
// Its reply stays an array in RESP3 while SMEMBERS becomes a set, because a negative
// count draws with replacement: the reply can legitimately contain the same member
// twice, which is not a set.
func cmdSRandMember(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args) > 3 {
		w.WriteError("ERR wrong number of arguments for 'srandmember' command")
		return false
	}
	if len(args) == 2 {
		members, err := s.store.SRandMember(string(args[1]), 1)
		if err != nil {
			writeStoreErr(w, err)
			return false
		}
		if len(members) == 0 {
			w.WriteNull()
			return false
		}
		w.WriteBulkString(members[0])
		return false
	}
	count, errMsg := parseRandomCount(args[2], false)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	members, err := s.store.SRandMember(string(args[1]), count)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeStrings(w, members)
	return false
}

func cmdSMove(s *Server, w *resp.Writer, args [][]byte) bool {
	moved, err := s.store.SMove(string(args[1]), string(args[2]), string(args[3]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(boolToInt(moved)))
	return moved
}

func cmdSInter(s *Server, w *resp.Writer, args [][]byte) bool {
	return setCombine(s, w, store.SetInter, args)
}

func cmdSUnion(s *Server, w *resp.Writer, args [][]byte) bool {
	return setCombine(s, w, store.SetUnion, args)
}

func cmdSDiff(s *Server, w *resp.Writer, args [][]byte) bool {
	return setCombine(s, w, store.SetDiff, args)
}

func setCombine(s *Server, w *resp.Writer, op store.SetOp, args [][]byte) bool {
	members, err := s.store.SCombine(op, 0, byteStrings(args[1:])...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeSet(w, members)
	return false
}

func cmdSInterStore(s *Server, w *resp.Writer, args [][]byte) bool {
	return setCombineStore(s, w, store.SetInter, args)
}

func cmdSUnionStore(s *Server, w *resp.Writer, args [][]byte) bool {
	return setCombineStore(s, w, store.SetUnion, args)
}

func cmdSDiffStore(s *Server, w *resp.Writer, args [][]byte) bool {
	return setCombineStore(s, w, store.SetDiff, args)
}

// setCombineStore applies one of the *STORE variants. It propagates verbatim: the
// result is a set, whose contents do not depend on the order the sources were
// iterated in, so a replica recomputing it arrives at the same value.
//
// An empty result still counts as a change when it *deleted* the destination, which
// a replica has to be told about; an empty result over a destination that never
// existed changes nothing and propagates nothing.
func setCombineStore(s *Server, w *resp.Writer, op store.SetOp, args [][]byte) bool {
	n, changed, err := s.store.SCombineStore(op, string(args[1]), byteStrings(args[2:])...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return changed
}

// cmdSInterCard implements SINTERCARD numkeys key [key ...] [LIMIT n], which counts
// the intersection without building the reply. LIMIT stops the scan early, so
// "is the overlap at least n" costs only n.
func cmdSInterCard(s *Server, w *resp.Writer, args [][]byte) bool {
	numKeys, ok := parseInt(args[1])
	if !ok || numKeys <= 0 {
		w.WriteError("ERR numkeys should be greater than 0")
		return false
	}
	if numKeys > len(args)-2 {
		w.WriteError("ERR Number of keys can't be greater than number of args")
		return false
	}
	keys := byteStrings(args[2 : 2+numKeys])
	limit := 0
	rest := args[2+numKeys:]
	switch {
	case len(rest) == 0:
	case len(rest) == 2 && strings.EqualFold(string(rest[0]), "LIMIT"):
		// Redis reports a non-numeric LIMIT with the same message as a negative one.
		n, ok := parseInt(rest[1])
		if !ok || n < 0 {
			w.WriteError("ERR LIMIT can't be negative")
			return false
		}
		limit = n
	default:
		w.WriteError("ERR syntax error")
		return false
	}
	members, err := s.store.SCombine(store.SetInter, limit, keys...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(len(members)))
	return false
}

// byteStrings converts command arguments to the strings the store's key and member
// APIs take.
func byteStrings(args [][]byte) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, string(a))
	}
	return out
}

// writeStrings writes a flat array of bulk strings.
func writeStrings(w *resp.Writer, items []string) {
	w.WriteArrayHeader(len(items))
	for _, it := range items {
		w.WriteBulkString(it)
	}
}
