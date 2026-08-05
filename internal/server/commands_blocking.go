package server

// The blocking list and sorted-set commands, and the two non-blocking ones they are
// built on (LMPOP, ZMPOP).
//
// Each blocking command is a pair: a non-blocking attempt (the blockFunc) and a
// description of how to wait for it to become possible (the blockSpec). The attempt
// is the whole command -- argument validation, the pop, the reply, and the effect to
// propagate -- and it is the only part that ever touches the store. blocking.go owns
// everything about waiting, so a handler here cannot get the concurrency wrong; it
// either serves and says so, rejects the arguments, or reports that it would like to
// wait.
//
// What propagates is always the pop that happened, never the command: a replica
// replaying BLPOP would wait forever on a connection with no client behind it.

import (
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	// The timeout is the last argument for the historical blocking commands and the
	// first for the MPOP family, which cannot put it after a variable-length key list.
	registerBlocking("BLPOP", -3, &blockSpec{
		fn: blockingListPop(true), keys: keysBeforeTimeout, timeout: -1, empty: writeNullArray,
	})
	registerBlocking("BRPOP", -3, &blockSpec{
		fn: blockingListPop(false), keys: keysBeforeTimeout, timeout: -1, empty: writeNullArray,
	})
	registerBlocking("BLMOVE", 6, &blockSpec{
		fn: blockingLMove, keys: firstKeyOnly, timeout: -1, empty: writeNullArray,
	})
	registerBlocking("BRPOPLPUSH", 4, &blockSpec{
		fn: blockingRPopLPush, keys: firstKeyOnly, timeout: -1, empty: writeNullArray,
	})
	registerBlocking("BZPOPMIN", -3, &blockSpec{
		fn: blockingZPop(true), keys: keysBeforeTimeout, timeout: -1, empty: writeNullArray,
	})
	registerBlocking("BZPOPMAX", -3, &blockSpec{
		fn: blockingZPop(false), keys: keysBeforeTimeout, timeout: -1, empty: writeNullArray,
	})
	registerBlocking("BLMPOP", -5, &blockSpec{
		fn: blockingLMPop, keys: mpopKeysAfterTimeout, timeout: 1, empty: writeNullArray,
	})
	registerBlocking("BZMPOP", -5, &blockSpec{
		fn: blockingZMPop, keys: mpopKeysAfterTimeout, timeout: 1, empty: writeNullArray,
	})

	// The non-blocking halves, which are ordinary write commands. LMPOP propagates
	// verbatim (it names the end it pops from, so a replica removes the same elements);
	// ZMPOP propagates the ZREM of the members it removed, because members tied at the
	// boundary score are chosen by the index's ordering of equal scores.
	register("LMPOP", -4, true, cmdLMPop)
	registerEffect("ZMPOP", -4, cmdZMPop)
}

// writeNullArray is the "nothing to serve" reply every command here shares: the null
// array in RESP2 ($-1's sibling *-1, which is what Redis sends even for the
// single-value BLMOVE) and the one null in RESP3.
func writeNullArray(w *resp.Writer) { w.WriteNullArray() }

// keysBeforeTimeout is the key list of a command shaped "NAME key [key...] timeout".
func keysBeforeTimeout(args [][]byte) []string {
	keys := make([]string, 0, len(args)-2)
	for _, k := range args[1 : len(args)-1] {
		keys = append(keys, string(k))
	}
	return keys
}

// firstKeyOnly is the key list of BLMOVE and BRPOPLPUSH: only the source can unblock
// them. An element arriving at the destination is of no use to a command that is
// waiting to move something *into* it.
func firstKeyOnly(args [][]byte) []string { return []string{string(args[1])} }

// mpopKeysAfterTimeout is the key list of BLMPOP/BZMPOP, whose number of keys is an
// operand: "NAME timeout numkeys key [key...] <dir> [COUNT n]".
//
// Only the count is parsed here, not the direction keyword, so the one function serves
// both the list and the sorted-set form. (An earlier version reused the full parser
// with the list keywords and quietly returned no keys for BZMPOP, whose direction is
// MIN or MAX -- a waiter with no keys is a waiter nothing can ever wake.)
func mpopKeysAfterTimeout(args [][]byte) []string {
	numkeys, ok := parseInt(args[2])
	if !ok || numkeys <= 0 || 3+numkeys > len(args) {
		return nil
	}
	keys := make([]string, 0, numkeys)
	for _, k := range args[3 : 3+numkeys] {
		keys = append(keys, string(k))
	}
	return keys
}

// --- BLPOP / BRPOP -----------------------------------------------------------

// blockingListPop is the non-blocking half of BLPOP and BRPOP: it walks the keys in
// the order given and pops from the first non-empty list.
//
// The keys are examined one at a time rather than under one combined lock. That is
// Redis's own semantics -- the command is defined as "the first of these that has
// something" -- and a multi-key lock here would serialize unrelated queues for no
// gain in what a client can observe.
//
// A wrong-type key is only an error once the scan reaches it, again as in Redis: BLPOP
// over a ready list and a string pops from the list and never looks at the string.
func blockingListPop(left bool) blockFunc {
	return func(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
		for _, k := range args[1 : len(args)-1] {
			key := string(k)
			var (
				v   []byte
				ok  bool
				err error
			)
			if left {
				v, ok, err = s.store.LPop(key)
			} else {
				v, ok, err = s.store.RPop(key)
			}
			if err != nil {
				writeStoreErr(w, err)
				return nil, false
			}
			if !ok {
				continue
			}
			w.WriteArrayHeader(2)
			w.WriteBulk(k)
			w.WriteBulk(v)
			return [][][]byte{{[]byte(popCommand(left)), k}}, false
		}
		return nil, true
	}
}

func popCommand(left bool) string {
	if left {
		return "LPOP"
	}
	return "RPOP"
}

// --- BLMOVE / BRPOPLPUSH -----------------------------------------------------

// blockingLMove is the non-blocking half of BLMOVE src dst LEFT|RIGHT LEFT|RIGHT
// timeout. The directions are validated before anything can block, so a syntax error
// is reported immediately rather than after the timeout.
func blockingLMove(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
	srcLeft, ok1 := listSide(args[3])
	dstLeft, ok2 := listSide(args[4])
	if !ok1 || !ok2 {
		w.WriteError("ERR syntax error")
		return nil, false
	}
	return blockingMove(s, w, args[1], args[2], srcLeft, dstLeft,
		[][]byte{[]byte("LMOVE"), args[1], args[2], args[3], args[4]})
}

func blockingRPopLPush(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
	return blockingMove(s, w, args[1], args[2], false, true,
		[][]byte{[]byte("RPOPLPUSH"), args[1], args[2]})
}

// blockingMove is the shared body. The effect is the non-blocking move naming both
// ends explicitly, so a replica moves the very same element; the destination is in
// affectedKeys, so a WATCH on it sees the conflict and a client blocked on it is
// woken.
func blockingMove(s *Server, w *resp.Writer, src, dst []byte, srcLeft, dstLeft bool,
	effect [][]byte) ([][][]byte, bool) {

	v, ok, err := s.store.LMove(string(src), string(dst), srcLeft, dstLeft)
	if err != nil {
		writeStoreErr(w, err)
		return nil, false
	}
	if !ok {
		return nil, true
	}
	w.WriteBulk(v)
	return [][][]byte{effect}, false
}

// --- BZPOPMIN / BZPOPMAX -----------------------------------------------------

// blockingZPop is the non-blocking half of BZPOPMIN and BZPOPMAX. The reply is
// [key, member, score], with the score as a RESP3 double (a bulk string in RESP2), as
// in Redis.
//
// It propagates the ZREM of the member it removed rather than a ZPOPMIN: members tied
// at the boundary score are separated only by the index's ordering of equal scores, so
// a replica running ZPOPMIN itself could remove a different one.
func blockingZPop(fromMin bool) blockFunc {
	return func(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
		for _, k := range args[1 : len(args)-1] {
			popped, err := s.store.ZPop(string(k), 1, fromMin)
			if err != nil {
				writeStoreErr(w, err)
				return nil, false
			}
			if len(popped) == 0 {
				continue
			}
			m := popped[0]
			w.WriteArrayHeader(3)
			w.WriteBulk(k)
			w.WriteBulk([]byte(m.Member))
			w.WriteDouble(m.Score)
			return [][][]byte{{[]byte("ZREM"), k, []byte(m.Member)}}, false
		}
		return nil, true
	}
}

// --- LMPOP / BLMPOP / ZMPOP / BZMPOP -----------------------------------------

// mpopWords are the two direction keywords of an MPOP-family command.
type mpopWords struct{ first, second string }

var (
	mpopListWords = mpopWords{"LEFT", "RIGHT"}
	mpopZSetWords = mpopWords{"MIN", "MAX"}
)

// mpopOpts are the parsed operands shared by LMPOP, BLMPOP, ZMPOP and BZMPOP.
type mpopOpts struct {
	keys  [][]byte
	first bool // LEFT / MIN rather than RIGHT / MAX
	count int
}

// parseMPop parses "numkeys key [key...] <dir> [COUNT n]" starting at args[start].
//
// The error messages are Redis's, including the surprising one: a numkeys that is not
// a positive integer reports "numkeys should be greater than 0" whatever was wrong
// with it, while a key count that does not match the keys given is a plain syntax
// error, because the direction keyword is then not where it has to be.
func parseMPop(args [][]byte, start int, words mpopWords) (mpopOpts, string) {
	var o mpopOpts
	o.count = 1

	numkeys, ok := parseInt(args[start])
	if !ok || numkeys <= 0 {
		return o, "ERR numkeys should be greater than 0"
	}
	dirIdx := start + 1 + numkeys
	if dirIdx >= len(args) {
		return o, "ERR syntax error"
	}
	o.keys = args[start+1 : dirIdx]

	switch {
	case strings.EqualFold(string(args[dirIdx]), words.first):
		o.first = true
	case strings.EqualFold(string(args[dirIdx]), words.second):
		o.first = false
	default:
		return o, "ERR syntax error"
	}

	rest := args[dirIdx+1:]
	switch len(rest) {
	case 0:
	case 2:
		if !strings.EqualFold(string(rest[0]), "COUNT") {
			return o, "ERR syntax error"
		}
		n, ok := parseInt(rest[1])
		if !ok || n <= 0 {
			return o, "ERR count should be greater than 0"
		}
		o.count = n
	default:
		return o, "ERR syntax error"
	}
	return o, ""
}

// cmdLMPop implements LMPOP numkeys key [key...] LEFT|RIGHT [COUNT count]: pop up to
// count elements from the first of the keys that holds a non-empty list. The reply is
// [key, [elements]], or a null array when none of them had anything.
func cmdLMPop(s *Server, w *resp.Writer, args [][]byte) bool {
	effect, wait := listMPop(s, w, args, 1)
	if wait {
		w.WriteNullArray()
	}
	return len(effect) > 0
}

// blockingLMPop is BLMPOP: the same command with a timeout in front of it.
func blockingLMPop(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
	return listMPop(s, w, args, 2)
}

// listMPop is the shared body of LMPOP and BLMPOP.
//
// The effect is an LPOP/RPOP with the number of elements actually popped, which is
// exact: the end is named, so a replica removes the same elements, and the count is
// what happened rather than what was asked for. LMPOP itself is registered as an
// ordinary verbatim-propagating write, but BLMPOP has to propagate an effect anyway,
// so both take the same path and there is one shape for a replay to reconstruct.
func listMPop(s *Server, w *resp.Writer, args [][]byte, start int) ([][][]byte, bool) {
	o, errMsg := parseMPop(args, start, mpopListWords)
	if errMsg != "" {
		w.WriteError(errMsg)
		return nil, false
	}
	for _, k := range o.keys {
		vals, _, err := s.store.LPopCount(string(k), o.count, o.first)
		if err != nil {
			writeStoreErr(w, err)
			return nil, false
		}
		if len(vals) == 0 {
			continue
		}
		w.WriteArrayHeader(2)
		w.WriteBulk(k)
		w.WriteArrayHeader(len(vals))
		for _, v := range vals {
			w.WriteBulk(v)
		}
		effect := [][]byte{[]byte(popCommand(o.first)), k, []byte(strconv.Itoa(len(vals)))}
		return [][][]byte{effect}, false
	}
	return nil, true
}

// cmdZMPop implements ZMPOP numkeys key [key...] MIN|MAX [COUNT count].
func cmdZMPop(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	effect, wait := zsetMPop(s, w, args, 1)
	if wait {
		w.WriteNullArray()
	}
	return effect
}

// blockingZMPop is BZMPOP.
func blockingZMPop(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
	return zsetMPop(s, w, args, 2)
}

// zsetMPop is the shared body of ZMPOP and BZMPOP. The reply is
// [key, [[member, score], ...]] -- nested in *both* protocols, unlike the WITHSCORES
// ranges, with the score a double only in RESP3. That is what Redis sends.
func zsetMPop(s *Server, w *resp.Writer, args [][]byte, start int) ([][][]byte, bool) {
	o, errMsg := parseMPop(args, start, mpopZSetWords)
	if errMsg != "" {
		w.WriteError(errMsg)
		return nil, false
	}
	for _, k := range o.keys {
		popped, err := s.store.ZPop(string(k), o.count, o.first)
		if err != nil {
			writeStoreErr(w, err)
			return nil, false
		}
		if len(popped) == 0 {
			continue
		}
		w.WriteArrayHeader(2)
		w.WriteBulk(k)
		w.WriteArrayHeader(len(popped))
		for _, m := range popped {
			w.WriteArrayHeader(2)
			w.WriteBulk([]byte(m.Member))
			w.WriteDouble(m.Score)
		}
		return [][][]byte{zremEffect(k, popped)}, false
	}
	return nil, true
}

func zremEffect(key []byte, popped []store.ZMember) [][]byte {
	effect := make([][]byte, 0, len(popped)+2)
	effect = append(effect, []byte("ZREM"), key)
	for _, m := range popped {
		effect = append(effect, []byte(m.Member))
	}
	return effect
}
