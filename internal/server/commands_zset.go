package server

import (
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("ZADD", -4, true, cmdZAdd)
	register("ZINCRBY", 4, true, cmdZIncrBy)
	register("ZSCORE", 3, false, cmdZScore)
	register("ZMSCORE", -3, false, cmdZMScore)
	register("ZREM", -3, true, cmdZRem)
	register("ZCARD", 2, false, cmdZCard)
	register("ZCOUNT", 4, false, cmdZCount)
	register("ZLEXCOUNT", 4, false, cmdZLexCount)
	register("ZRANK", 3, false, cmdZRank)
	register("ZREVRANK", 3, false, cmdZRevRank)
	register("ZRANGE", -4, false, cmdZRange)
	register("ZREVRANGE", -4, false, cmdZRevRange)
	register("ZRANGEBYSCORE", -4, false, cmdZRangeByScore)
	register("ZREVRANGEBYSCORE", -4, false, cmdZRevRangeByScore)
	register("ZREMRANGEBYRANK", 4, true, cmdZRemRangeByRank)
	register("ZREMRANGEBYSCORE", 4, true, cmdZRemRangeByScore)
	register("ZRANDMEMBER", -2, false, cmdZRandMember)
	registerEffect("ZPOPMIN", -2, cmdZPopMin)
	registerEffect("ZPOPMAX", -2, cmdZPopMax)
}

// parseZAddOpts consumes ZADD's flag prefix, returning the options, the index of
// the first score/member pair, and the RESP error message to reply with (or "").
//
// The flags stop at the first argument that is not one of them, which is what makes
// a member literally named "CH" reachable: it can only appear where a member is
// expected, after a score.
func parseZAddOpts(args [][]byte) (store.ZAddOptions, int, string) {
	var o store.ZAddOptions
	i := 2
opts:
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			o.NX = true
		case "XX":
			o.XX = true
		case "GT":
			o.GT = true
		case "LT":
			o.LT = true
		case "CH":
			o.CH = true
		case "INCR":
			o.Incr = true
		default:
			break opts
		}
	}
	switch {
	case o.NX && o.XX:
		return o, i, "ERR XX and NX options at the same time are not compatible"
	case o.NX && (o.GT || o.LT), o.GT && o.LT:
		return o, i, "ERR GT, LT, and/or NX options at the same time are not compatible"
	}
	if n := len(args) - i; n == 0 || n%2 != 0 {
		return o, i, "ERR wrong number of arguments for 'zadd' command"
	}
	if o.Incr && len(args)-i != 2 {
		return o, i, "ERR INCR option supports a single increment-element pair"
	}
	return o, i, ""
}

// cmdZAdd implements ZADD with its full flag set. Every score is validated before
// anything is applied, as Redis does, so a malformed pair late in the command
// cannot leave the first half of it written.
func cmdZAdd(s *Server, w *resp.Writer, args [][]byte) bool {
	o, first, errMsg := parseZAddOpts(args)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	members := make([]store.ZMember, 0, (len(args)-first)/2)
	for i := first; i < len(args); i += 2 {
		score, ok := parseScore(args[i])
		if !ok {
			w.WriteError("ERR value is not a valid float")
			return false
		}
		members = append(members, store.ZMember{Member: string(args[i+1]), Score: score})
	}

	// INCR turns ZADD into a conditional ZINCRBY that replies with the new score,
	// or a null when the flags rejected the update.
	if o.Incr {
		score, applied, err := s.store.ZIncrBy(string(args[1]), members[0].Member, members[0].Score, o)
		if err != nil {
			writeStoreErr(w, err)
			return false
		}
		if !applied {
			w.WriteNull()
			return false
		}
		w.WriteDouble(score)
		return true
	}

	added, changed, err := s.store.ZAddMulti(string(args[1]), o, members)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if o.CH {
		w.WriteInt(int64(changed))
	} else {
		w.WriteInt(int64(added))
	}
	return changed > 0
}

func cmdZIncrBy(s *Server, w *resp.Writer, args [][]byte) bool {
	delta, ok := parseScore(args[2])
	if !ok {
		w.WriteError("ERR value is not a valid float")
		return false
	}
	score, _, err := s.store.ZIncrBy(string(args[1]), string(args[3]), delta, store.ZAddOptions{})
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteDouble(score)
	return true
}

// cmdZScore replies with the score as a double: a RESP3 double type, and in RESP2
// the same text as a bulk string -- the encoding every RESP2 client already parses a
// score out of, so its bytes are unchanged.
func cmdZScore(s *Server, w *resp.Writer, args [][]byte) bool {
	score, ok, err := s.store.ZScore(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteDouble(score)
	return false
}

// cmdZMScore answers one element per requested member, a null for each one the set
// does not hold.
func cmdZMScore(s *Server, w *resp.Writer, args [][]byte) bool {
	scores, present, err := s.store.ZMScore(string(args[1]), byteStrings(args[2:])...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(scores))
	for i, score := range scores {
		if !present[i] {
			w.WriteNull()
			continue
		}
		w.WriteDouble(score)
	}
	return false
}

func cmdZRem(s *Server, w *resp.Writer, args [][]byte) bool {
	removed, err := s.store.ZRemMulti(string(args[1]), byteStrings(args[2:])...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(removed))
	return removed > 0
}

func cmdZCard(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.ZCard(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

func cmdZRank(s *Server, w *resp.Writer, args [][]byte) bool {
	rank, ok, err := s.store.ZRank(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteInt(int64(rank))
	return false
}

func cmdZRevRank(s *Server, w *resp.Writer, args [][]byte) bool {
	rank, ok, err := s.store.ZRevRank(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteInt(int64(rank))
	return false
}

func cmdZRange(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRangeByRank(s, w, args, false)
}

func cmdZRevRange(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRangeByRank(s, w, args, true)
}

// zRangeByRank is the shared body of ZRANGE and ZREVRANGE: the same index rules
// applied to the ascending or the descending order.
func zRangeByRank(s *Server, w *resp.Writer, args [][]byte, rev bool) bool {
	withScores := false
	switch {
	case len(args) == 4:
	case len(args) == 5 && strings.EqualFold(string(args[4]), "WITHSCORES"):
		withScores = true
	default:
		w.WriteError("ERR syntax error")
		return false
	}
	start, ok1 := parseInt(args[2])
	stop, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	var members []store.ZMember
	var err error
	if rev {
		members, err = s.store.ZRevRange(string(args[1]), start, stop)
	} else {
		members, err = s.store.ZRange(string(args[1]), start, stop)
	}
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeZMembers(w, members, withScores)
	return false
}

func cmdZRangeByScore(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRangeByScore(s, w, args, false)
}

func cmdZRevRangeByScore(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRangeByScore(s, w, args, true)
}

// zRangeByScore is the shared body of ZRANGEBYSCORE and ZREVRANGEBYSCORE. The
// reverse form takes its bounds the other way round (max first), which is a
// deliberate part of the Redis interface rather than an oversight, so the operands
// are swapped here before the range is built.
func zRangeByScore(s *Server, w *resp.Writer, args [][]byte, rev bool) bool {
	minArg, maxArg := args[2], args[3]
	if rev {
		minArg, maxArg = maxArg, minArg
	}
	r, ok := parseScoreRange(minArg, maxArg)
	if !ok {
		w.WriteError("ERR min or max is not a float")
		return false
	}
	withScores, offset, count, errMsg := parseRangeTail(args[4:])
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	members, err := s.store.ZRangeByScore(string(args[1]), r, offset, count, rev)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeZMembers(w, members, withScores)
	return false
}

// parseRangeTail parses the [WITHSCORES] [LIMIT offset count] tail the by-score
// range commands accept in either order, returning the RESP error message to reply
// with or "". A count of -1 means "everything from the offset on", which is also
// the default when no LIMIT is given.
func parseRangeTail(tail [][]byte) (withScores bool, offset, count int, errMsg string) {
	count = -1
	for i := 0; i < len(tail); i++ {
		switch {
		case strings.EqualFold(string(tail[i]), "WITHSCORES"):
			withScores = true
		case strings.EqualFold(string(tail[i]), "LIMIT"):
			if i+2 >= len(tail) {
				return false, 0, 0, "ERR syntax error"
			}
			o, ok1 := parseInt(tail[i+1])
			c, ok2 := parseInt(tail[i+2])
			if !ok1 || !ok2 {
				return false, 0, 0, "ERR value is not an integer or out of range"
			}
			offset, count = o, c
			i += 2
		default:
			return false, 0, 0, "ERR syntax error"
		}
	}
	return withScores, offset, count, ""
}

func cmdZCount(s *Server, w *resp.Writer, args [][]byte) bool {
	r, ok := parseScoreRange(args[2], args[3])
	if !ok {
		w.WriteError("ERR min or max is not a float")
		return false
	}
	n, err := s.store.ZCount(string(args[1]), r)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

func cmdZLexCount(s *Server, w *resp.Writer, args [][]byte) bool {
	r, ok := parseLexRange(args[2], args[3])
	if !ok {
		w.WriteError("ERR min or max not valid string range item")
		return false
	}
	n, err := s.store.ZLexCount(string(args[1]), r)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

func cmdZRemRangeByRank(s *Server, w *resp.Writer, args [][]byte) bool {
	start, ok1 := parseInt(args[2])
	stop, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	removed, err := s.store.ZRemRangeByRank(string(args[1]), start, stop)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(removed))
	return removed > 0
}

func cmdZRemRangeByScore(s *Server, w *resp.Writer, args [][]byte) bool {
	r, ok := parseScoreRange(args[2], args[3])
	if !ok {
		w.WriteError("ERR min or max is not a float")
		return false
	}
	removed, err := s.store.ZRemRangeByScore(string(args[1]), r)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(removed))
	return removed > 0
}

// cmdZRandMember implements ZRANDMEMBER key [count [WITHSCORES]]. It is a read, so
// nothing propagates -- as with SRANDMEMBER, a replica running it would draw
// different members.
func cmdZRandMember(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args) > 4 {
		w.WriteError("ERR syntax error")
		return false
	}
	if len(args) == 2 {
		members, err := s.store.ZRandMember(string(args[1]), 1)
		if err != nil {
			writeStoreErr(w, err)
			return false
		}
		if len(members) == 0 {
			w.WriteNull()
			return false
		}
		w.WriteBulk([]byte(members[0].Member))
		return false
	}
	count, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	withScores := false
	if len(args) == 4 {
		if !strings.EqualFold(string(args[3]), "WITHSCORES") {
			w.WriteError("ERR syntax error")
			return false
		}
		withScores = true
	}
	members, err := s.store.ZRandMember(string(args[1]), count)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeZMembers(w, members, withScores)
	return false
}

func cmdZPopMin(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	return zPop(s, w, args, true)
}

func cmdZPopMax(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	return zPop(s, w, args, false)
}

// zPop is the shared body of ZPOPMIN and ZPOPMAX. The end is named, so which
// members go is determined by the scores -- except when several members tie at the
// boundary score, where the choice comes down to the index's ordering of equal
// scores. It therefore propagates the ZREM of the members actually removed, which
// is exact whether or not there was a tie.
func zPop(s *Server, w *resp.Writer, args [][]byte, fromMin bool) [][][]byte {
	if len(args) > 3 {
		w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(string(args[0])) + "' command")
		return nil
	}
	count, hasCount := 1, len(args) == 3
	if hasCount {
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
	popped, err := s.store.ZPop(string(args[1]), count, fromMin)
	if err != nil {
		writeStoreErr(w, err)
		return nil
	}
	// The two forms differ in RESP3 exactly as they do in Redis: the count form is a
	// list of [member, score] pairs, while the countless form is one flat pair -- it
	// pops a single member, so there is no list for a client to iterate. RESP2
	// flattens both, which is why this reply is written here rather than by
	// writeZMembers.
	if hasCount {
		writeZMembers(w, popped, true)
	} else {
		w.WriteArrayHeader(len(popped) * 2)
		for _, m := range popped {
			w.WriteBulk([]byte(m.Member))
			w.WriteDouble(m.Score)
		}
	}
	if len(popped) == 0 {
		return nil
	}
	effect := [][]byte{[]byte("ZREM"), args[1]}
	for _, m := range popped {
		effect = append(effect, []byte(m.Member))
	}
	return [][][]byte{effect}
}

// parseScoreRange parses a min/max operand pair, honouring the "(" prefix that makes
// a bound exclusive and the infinities that make one unbounded.
func parseScoreRange(minArg, maxArg []byte) (store.ScoreRange, bool) {
	var r store.ScoreRange
	var ok bool
	if r.Min, r.MinExcl, ok = parseScoreBound(minArg); !ok {
		return r, false
	}
	if r.Max, r.MaxExcl, ok = parseScoreBound(maxArg); !ok {
		return r, false
	}
	return r, true
}

func parseScoreBound(b []byte) (score float64, excl, ok bool) {
	if len(b) > 0 && b[0] == '(' {
		score, ok = parseScore(b[1:])
		return score, true, ok
	}
	score, ok = parseScore(b)
	return score, false, ok
}

// parseLexRange parses a lexicographic min/max pair: "-" and "+" for the extremes,
// "[member" for an inclusive bound and "(member" for an exclusive one. Anything else
// -- including a bare member -- is invalid, as in Redis.
func parseLexRange(minArg, maxArg []byte) (store.LexRange, bool) {
	var r store.LexRange
	var ok bool
	if r.Min, r.MinExcl, r.MinInf, ok = parseLexBound(minArg, "-"); !ok {
		return r, false
	}
	if r.Max, r.MaxExcl, r.MaxInf, ok = parseLexBound(maxArg, "+"); !ok {
		return r, false
	}
	return r, true
}

func parseLexBound(b []byte, infinity string) (member string, excl, inf, ok bool) {
	if len(b) == 0 {
		return "", false, false, false
	}
	if string(b) == infinity {
		return "", false, true, true
	}
	switch b[0] {
	case '[':
		return string(b[1:]), false, false, true
	case '(':
		return string(b[1:]), true, false, true
	}
	return "", false, false, false
}

// writeZMembers writes a sorted-set reply: the members alone, or member/score pairs
// when scores are wanted.
//
// WITHSCORES is the reply RESP3 reshapes most visibly: RESP2 flattens the pairs into
// 2n elements and leaves the client to re-pair them and parse each score out of a
// bulk string, while RESP3 sends n two-element arrays whose second element is a
// double. Both come out of one loop (see writePairsHeader), so the two encodings
// cannot drift apart.
func writeZMembers(w *resp.Writer, members []store.ZMember, withScores bool) {
	if withScores {
		nested := writePairsHeader(w, len(members))
		for _, m := range members {
			if nested {
				w.WriteArrayHeader(2)
			}
			w.WriteBulk([]byte(m.Member))
			w.WriteDouble(m.Score)
		}
		return
	}
	w.WriteArrayHeader(len(members))
	for _, m := range members {
		w.WriteBulk([]byte(m.Member))
	}
}
