package server

// Reply shapes that differ between RESP2 and RESP3.
//
// RESP3 did not only add encodings for values RESP2 could already express -- it
// changed the *shape* of several replies, because the new types carry meaning a flat
// array cannot. HGETALL is a map, SMEMBERS is a set, a score is a double, and a
// WITHSCORES range is a list of pairs rather than a flattened one. A client library
// that negotiated RESP3 decodes those shapes directly into the language's map, set
// and float types, which is the entire reason the protocol exists.
//
// Every helper here writes both shapes from one call site. That is deliberate: the
// alternative -- a `if w.Proto() >= 3` at each of the forty-odd places a collection
// is written -- is exactly how a RESP2 client ends up receiving a stray `%` after
// someone adds a command and copies the wrong branch. A handler states what the
// reply *is* (a map, a set, a run of pairs) and the encoding follows from that.

import "github.com/Black-third/shardkv/internal/resp"

// writeSet writes an unordered collection of distinct strings: a RESP3 set (~), a
// RESP2 array. SMEMBERS, SPOP with a count and the SINTER/SUNION/SDIFF family all
// return sets; SRANDMEMBER does not, because a negative count lets it repeat
// members, and a set that reported duplicates would be lying about its own type.
func writeSet(w *resp.Writer, items []string) {
	w.WriteSetHeader(len(items))
	for _, it := range items {
		w.WriteBulkString(it)
	}
}

// writeMapStrings writes a flat name/value sequence as a map: a RESP3 map (%), a
// RESP2 array of twice as many elements. flat must have even length; an odd tail
// would announce a pair the caller cannot complete, so it is dropped rather than
// written past the announced length.
func writeMapStrings(w *resp.Writer, flat []string) {
	n := len(flat) / 2
	w.WriteMapHeader(n)
	for i := 0; i < n*2; i++ {
		w.WriteBulkString(flat[i])
	}
}

// writeMapBulks is writeMapStrings for values the store already hands back as bytes
// (HGETALL), so a hash reply costs no string conversion.
func writeMapBulks(w *resp.Writer, flat [][]byte) {
	n := len(flat) / 2
	w.WriteMapHeader(n)
	for i := 0; i < n*2; i++ {
		w.WriteBulk(flat[i])
	}
}

// writePairsHeader writes the outer header for a reply of n two-element pairs and
// reports whether each pair needs a header of its own.
//
// RESP3 nests: `ZRANGE k 0 -1 WITHSCORES` is n arrays of [member, score]. RESP2
// flattens the same data into 2n elements and lets the client pair them up. The
// return value is what lets one loop emit both, and it is a bool rather than a
// second helper because the loop body -- which values go in which order -- is
// identical in the two protocols.
func writePairsHeader(w *resp.Writer, n int) (nested bool) {
	if w.Proto() >= resp.ProtoRESP3 {
		w.WriteArrayHeader(n)
		return true
	}
	w.WriteArrayHeader(n * 2)
	return false
}
