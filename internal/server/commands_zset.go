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
	register("ZRANK", -3, false, cmdZRank)
	register("ZREVRANK", -3, false, cmdZRevRank)
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
		// A trailing score with no member is a *syntax* error, not an arity error: the
		// command has enough arguments to be a ZADD, it is the score/member pairing that is
		// wrong. Redis words it that way and its own variadic test asserts on it.
		return o, i, "ERR syntax error"
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

// cmdZRank and cmdZRevRank implement ZRANK/ZREVRANK key member [WITHSCORE].
//
// WITHSCORE turns the reply from a bare integer into a [rank, score] pair, which saves the
// round trip a caller would otherwise make to ZSCORE -- and saves it having to reconcile
// two reads of a set that may have changed in between. A member that is not in the set
// answers with a *null array* rather than a null string in that form, because the shape
// that is missing is the pair; Redis makes the same distinction, and a client that parsed
// the two-element reply would choke on a null bulk string where an array was promised.
func cmdZRank(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRank(s, w, args, false)
}

func cmdZRevRank(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRank(s, w, args, true)
}

func zRank(s *Server, w *resp.Writer, args [][]byte, rev bool) bool {
	withScore := false
	switch {
	case len(args) == 3:
	case len(args) == 4 && strings.EqualFold(string(args[3]), "WITHSCORE"):
		withScore = true
	default:
		w.WriteError("ERR syntax error")
		return false
	}
	key, member := string(args[1]), string(args[2])
	var rank int
	var ok bool
	var err error
	if rev {
		rank, ok, err = s.store.ZRevRank(key, member)
	} else {
		rank, ok, err = s.store.ZRank(key, member)
	}
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		if withScore {
			w.WriteNullArray()
		} else {
			w.WriteNull()
		}
		return false
	}
	if !withScore {
		w.WriteInt(int64(rank))
		return false
	}
	// The score comes from a second lookup, which is a second acquisition of the shard's
	// read lock: the pair may therefore describe two instants if the member's score moved
	// in between. Redis reads both under its single thread. Widening the store's rank API
	// to return the score is the fix; until then the window is one concurrent ZADD wide and
	// the rank, not the score, is the answer the command is named for.
	score, _, err := s.store.ZScore(key, member)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(2)
	w.WriteInt(int64(rank))
	w.WriteDouble(score)
	return false
}

// cmdZRange implements the whole of Redis 6.2's ZRANGE:
//
//	ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
//
// It subsumes ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX and ZREVRANGEBYLEX,
// all of which remain because a decade of client code sends them. What it does *not* do is
// reimplement their bound rules: the selection goes through the same zRangeSpec and
// store.selectRange that ZRANGESTORE uses, so "what does this set of options select" has
// one answer and the two commands cannot drift apart.
func cmdZRange(s *Server, w *resp.Writer, args [][]byte) bool {
	// WITHSCORES is stripped before the options are parsed: it describes the reply rather
	// than the selection, and it is the one option that may follow LIMIT.
	tail := args[4:]
	withScores := false
	if len(tail) > 0 && strings.EqualFold(string(tail[len(tail)-1]), "WITHSCORES") {
		withScores = true
		tail = tail[:len(tail)-1]
	}
	// The BYLEX/WITHSCORES conflict is refused before the bounds are parsed, because the
	// bounds cannot be parsed until it is settled which *kind* of bound they are -- and a
	// caller who asked for both got the option combination wrong, not the bounds. Redis
	// reports the same thing in the same order: `ZRANGE k 0 -1 BYLEX WITHSCORES` complains
	// about the options, not about "0" being an invalid member.
	if withScores {
		for _, a := range tail {
			if strings.EqualFold(string(a), "BYLEX") {
				// A lexicographic range is only meaningful when every member shares one score, so
				// there is nothing for the scores to tell the caller.
				w.WriteError("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
				return false
			}
		}
	}
	sel, errMsg := zRangeSpec(args[2], args[3], tail)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	members, err := s.store.ZRangeSelect(string(args[1]), sel)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeZMembers(w, members, withScores)
	return false
}

// cmdZRevRange implements ZREVRANGE key start stop [WITHSCORES], which is ZRANGE ... REV
// with no other options.
func cmdZRevRange(s *Server, w *resp.Writer, args [][]byte) bool {
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
	members, err := s.store.ZRevRange(string(args[1]), start, stop)
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
		w.WriteBulkString(members[0].Member)
		return false
	}
	// WITHSCORES first, for the reason cmdHRandField reads WITHVALUES first: it halves the
	// range the count may take.
	withScores := false
	if len(args) == 4 {
		if !strings.EqualFold(string(args[3]), "WITHSCORES") {
			w.WriteError("ERR syntax error")
			return false
		}
		withScores = true
	}
	count, errMsg := parseRandomCount(args[2], withScores)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
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
			w.WriteBulkString(m.Member)
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

// parseLexRange parses a lexicographic min/max pair: "-" and "+" for the two extremes,
// "[member" for an inclusive bound and "(member" for an exclusive one. Anything else --
// including a bare member -- is invalid, as in Redis.
//
// Either extreme is accepted at either end. `ZRANGEBYLEX key + -` is not a mistake to
// reject: it is a lower bound above every member and an upper bound below every one, so it
// selects nothing, and that is the answer Redis gives.
func parseLexRange(minArg, maxArg []byte) (store.LexRange, bool) {
	var r store.LexRange
	var ok bool
	if r.Min, ok = parseLexBound(minArg); !ok {
		return r, false
	}
	if r.Max, ok = parseLexBound(maxArg); !ok {
		return r, false
	}
	return r, true
}

func parseLexBound(b []byte) (store.LexBound, bool) {
	if len(b) == 0 {
		return store.LexBound{}, false
	}
	switch string(b) {
	case "-":
		return store.LexBound{NegInf: true}, true
	case "+":
		return store.LexBound{PosInf: true}, true
	}
	switch b[0] {
	case '[':
		return store.LexBound{Member: string(b[1:])}, true
	case '(':
		return store.LexBound{Member: string(b[1:]), Excl: true}, true
	}
	return store.LexBound{}, false
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
			w.WriteBulkString(m.Member)
			w.WriteDouble(m.Score)
		}
		return
	}
	w.WriteArrayHeader(len(members))
	for _, m := range members {
		w.WriteBulkString(m.Member)
	}
}

// --- lexicographic ranges, the general ZRANGE, and ZRANGESTORE -----------------

func init() {
	register("ZRANGEBYLEX", -4, false, cmdZRangeByLex)
	register("ZREVRANGEBYLEX", -4, false, cmdZRevRangeByLex)
	register("ZREMRANGEBYLEX", 4, true, cmdZRemRangeByLex)
	register("ZRANGESTORE", -5, true, cmdZRangeStore)
	register("ZUNIONSTORE", -4, true, cmdZUnionStore)
	register("ZINTERSTORE", -4, true, cmdZInterStore)
	register("ZDIFFSTORE", -4, true, cmdZDiffStore)
	register("ZUNION", -3, false, cmdZUnion)
	register("ZINTER", -3, false, cmdZInter)
	register("ZDIFF", -3, false, cmdZDiff)
	register("ZINTERCARD", -3, false, cmdZInterCard)
}

func cmdZRangeByLex(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRangeByLex(s, w, args, false)
}

func cmdZRevRangeByLex(s *Server, w *resp.Writer, args [][]byte) bool {
	return zRangeByLex(s, w, args, true)
}

// zRangeByLex is the shared body of ZRANGEBYLEX and ZREVRANGEBYLEX. As with the by-score
// pair the reverse form takes its bounds the other way round (max first), which is part of
// Redis's interface rather than an oversight, so the operands are swapped before the range
// is built.
//
// Neither form accepts WITHSCORES: a lexicographic range is only meaningful when every
// member shares one score, so the scores carry no information the caller does not already
// have. Redis refuses it too.
func zRangeByLex(s *Server, w *resp.Writer, args [][]byte, rev bool) bool {
	minArg, maxArg := args[2], args[3]
	if rev {
		minArg, maxArg = maxArg, minArg
	}
	r, ok := parseLexRange(minArg, maxArg)
	if !ok {
		w.WriteError(errBadLexRange)
		return false
	}
	offset, count, errMsg := parseLimitTail(args[4:])
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	members, err := s.store.ZRangeByLex(string(args[1]), r, offset, count, rev)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeZMembers(w, members, false)
	return false
}

func cmdZRemRangeByLex(s *Server, w *resp.Writer, args [][]byte) bool {
	r, ok := parseLexRange(args[2], args[3])
	if !ok {
		w.WriteError(errBadLexRange)
		return false
	}
	removed, err := s.store.ZRemRangeByLex(string(args[1]), r)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(removed))
	return removed > 0
}

// errBadLexRange is Redis's message for a lexicographic bound that is not "-", "+",
// "[member" or "(member". A bare member is refused rather than assumed inclusive, because
// the two readings select different sets and guessing would be silently wrong.
const errBadLexRange = "ERR min or max not valid string range item"

// parseLimitTail parses the "[LIMIT offset count]" tail the lex range commands accept. A
// count of -1 means "everything from the offset on", which is also the default.
func parseLimitTail(tail [][]byte) (offset, count int, errMsg string) {
	count = -1
	if len(tail) == 0 {
		return 0, count, ""
	}
	if len(tail) != 3 || !strings.EqualFold(string(tail[0]), "LIMIT") {
		return 0, 0, "ERR syntax error"
	}
	o, ok1 := parseInt(tail[1])
	c, ok2 := parseInt(tail[2])
	if !ok1 || !ok2 {
		return 0, 0, "ERR value is not an integer or out of range"
	}
	return o, c, ""
}

// --- the general ZRANGE selection ---------------------------------------------

// zRangeSpec parses the bound-shape options ZRANGE and ZRANGESTORE share:
//
//	[BYSCORE | BYLEX] [REV] [LIMIT offset count]
//
// minArg and maxArg are the two positional bounds. Their *meaning* depends on the options,
// which is why this is one function rather than three: REV swaps which operand is the
// lower bound, and BYSCORE/BYLEX decide whether they are scores, members or ranks.
//
// LIMIT is refused for a rank range, with Redis's own message. A rank range already names
// exactly the slice it wants, so a second offset applied on top of it would silently
// select something else -- and Redis's error names the two option pairs it works with,
// which is what tells a caller how to fix it.
func zRangeSpec(minArg, maxArg []byte, tail [][]byte) (store.ZRangeSelector, string) {
	var sel store.ZRangeSelector
	sel.Count = -1

	hasLimit := false
	byScore, byLex := false, false
	for i := 0; i < len(tail); i++ {
		switch strings.ToUpper(string(tail[i])) {
		case "BYSCORE":
			byScore = true
		case "BYLEX":
			byLex = true
		case "REV":
			sel.Rev = true
		case "LIMIT":
			if i+2 >= len(tail) {
				return sel, "ERR syntax error"
			}
			o, ok1 := parseInt(tail[i+1])
			c, ok2 := parseInt(tail[i+2])
			if !ok1 || !ok2 {
				return sel, "ERR value is not an integer or out of range"
			}
			sel.Offset, sel.Count, hasLimit = o, c, true
			i += 2
		default:
			return sel, "ERR syntax error"
		}
	}
	if byScore && byLex {
		return sel, "ERR syntax error"
	}
	if hasLimit && !byScore && !byLex {
		return sel, "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
	}

	// REV reverses which positional operand is the lower bound for the score and lex forms
	// -- `ZRANGE k (c (a BYLEX REV` reads high-to-low -- while for a rank range REV instead
	// reverses the order the indexes count in, which selectRange applies.
	lo, hi := minArg, maxArg
	if sel.Rev && (byScore || byLex) {
		lo, hi = hi, lo
	}
	switch {
	case byScore:
		sel.By = store.ZRangeByScore
		r, ok := parseScoreRange(lo, hi)
		if !ok {
			return sel, "ERR min or max is not a float"
		}
		sel.Score = r
	case byLex:
		sel.By = store.ZRangeByLex
		r, ok := parseLexRange(lo, hi)
		if !ok {
			return sel, errBadLexRange
		}
		sel.Lex = r
	default:
		sel.By = store.ZRangeByRank
		start, ok1 := parseInt(minArg)
		stop, ok2 := parseInt(maxArg)
		if !ok1 || !ok2 {
			return sel, "ERR value is not an integer or out of range"
		}
		sel.Start, sel.Stop = start, stop
	}
	return sel, ""
}

// cmdZRangeStore implements
// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count].
//
// It writes two keys, so it is listed in affectedKeys (invariant 7) -- which is also what
// gives it the cluster cross-slot check for free (invariant 13).
func cmdZRangeStore(s *Server, w *resp.Writer, args [][]byte) bool {
	sel, errMsg := zRangeSpec(args[3], args[4], args[5:])
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	n, err := s.store.ZRangeStore(string(args[1]), string(args[2]), sel)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	// Always dirty: the destination is replaced whatever the range selected, and an empty
	// selection deletes it -- a deletion the AOF and every replica have to perform too.
	return true
}

// --- the sorted-set algebra ---------------------------------------------------

func cmdZUnionStore(s *Server, w *resp.Writer, args [][]byte) bool {
	return zCombineStore(s, w, args, store.ZCombineUnion)
}

func cmdZInterStore(s *Server, w *resp.Writer, args [][]byte) bool {
	return zCombineStore(s, w, args, store.ZCombineInter)
}

func cmdZDiffStore(s *Server, w *resp.Writer, args [][]byte) bool {
	return zCombineStore(s, w, args, store.ZCombineDiff)
}

func cmdZUnion(s *Server, w *resp.Writer, args [][]byte) bool {
	return zCombine(s, w, args, store.ZCombineUnion)
}

func cmdZInter(s *Server, w *resp.Writer, args [][]byte) bool {
	return zCombine(s, w, args, store.ZCombineInter)
}

func cmdZDiff(s *Server, w *resp.Writer, args [][]byte) bool {
	return zCombine(s, w, args, store.ZCombineDiff)
}

// zCombineStore is the body of ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE: the destination is the
// first argument, the inputs follow a numkeys count. The reply is how many members were
// stored, and 0 means the destination was deleted.
func zCombineStore(s *Server, w *resp.Writer, args [][]byte, kind store.ZCombineKind) bool {
	ops, agg, errMsg := parseZCombine(args, 2, kind, strings.ToLower(string(args[0])), false)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	n, err := s.store.ZCombineStore(string(args[1]), kind, ops, agg)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	// Always dirty, for the reason ZRANGESTORE is: an empty result deletes the destination.
	return true
}

// zCombine is the body of ZUNION/ZINTER/ZDIFF, the non-storing forms. They exist so a
// caller can read a combination without writing a key it would then have to delete -- and
// so they are reads, refused by nothing on a replica.
func zCombine(s *Server, w *resp.Writer, args [][]byte, kind store.ZCombineKind) bool {
	ops, agg, errMsg := parseZCombine(args, 1, kind, strings.ToLower(string(args[0])), true)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	// WITHSCORES may only be the final argument, and parseZCombine has already accepted it
	// as a trailing option, so re-reading it here is how the reply shape is decided.
	withScores := strings.EqualFold(string(args[len(args)-1]), "WITHSCORES")
	members, err := s.store.ZCombine(kind, ops, agg)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeZMembers(w, members, withScores)
	return false
}

// parseZCombine reads the "numkeys key [key...] [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX]"
// shape at args[numkeysIdx], returning one input per key.
//
// name is the command's own name, because Redis puts it inside the "at least 1 input key"
// message. allowWithScores admits the trailing WITHSCORES of the non-storing forms; the
// storing forms refuse it, since there is no reply for the scores to appear in.
//
// WEIGHTS and AGGREGATE are refused for the difference, as they are in Redis: a difference
// reports the first input's own scores, so a weight would multiply a score the caller can
// see and an aggregation would have nothing to aggregate.
func parseZCombine(args [][]byte, numkeysIdx int, kind store.ZCombineKind, name string, allowWithScores bool) ([]store.ZSetOp, store.ZAggregate, string) {
	agg := store.ZAggSum
	if numkeysIdx >= len(args) {
		return nil, agg, "ERR syntax error"
	}
	numkeys, ok := parseInt(args[numkeysIdx])
	if !ok {
		return nil, agg, "ERR value is not an integer or out of range"
	}
	if numkeys < 1 {
		return nil, agg, "ERR at least 1 input key is needed for '" + name + "' command"
	}
	first := numkeysIdx + 1
	if first+numkeys > len(args) {
		return nil, agg, "ERR syntax error"
	}
	ops := make([]store.ZSetOp, 0, numkeys)
	for _, k := range args[first : first+numkeys] {
		ops = append(ops, store.ZSetOp{Key: string(k), Weight: 1})
	}

	for i := first + numkeys; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "WEIGHTS":
			if kind == store.ZCombineDiff || i+numkeys >= len(args) {
				return nil, agg, "ERR syntax error"
			}
			for j := 0; j < numkeys; j++ {
				weight, ok := parseScore(args[i+1+j])
				if !ok {
					return nil, agg, "ERR weight value is not a float"
				}
				ops[j].Weight = weight
			}
			i += numkeys
		case "AGGREGATE":
			if kind == store.ZCombineDiff || i+1 >= len(args) {
				return nil, agg, "ERR syntax error"
			}
			switch strings.ToUpper(string(args[i+1])) {
			case "SUM":
				agg = store.ZAggSum
			case "MIN":
				agg = store.ZAggMin
			case "MAX":
				agg = store.ZAggMax
			default:
				return nil, agg, "ERR syntax error"
			}
			i++
		case "WITHSCORES":
			// Only as the final argument: it changes the reply's shape rather than the
			// computation, so anything after it would be an option applied to a reply already
			// described.
			if !allowWithScores || i != len(args)-1 {
				return nil, agg, "ERR syntax error"
			}
		default:
			return nil, agg, "ERR syntax error"
		}
	}
	return ops, agg, ""
}

// cmdZInterCard implements ZINTERCARD numkeys key [key...] [LIMIT n], the size of an
// intersection without building it. LIMIT 0 means "no limit"; a caller that only wants to
// know whether two large sets overlap passes 1 and the walk stops at the first hit.
func cmdZInterCard(s *Server, w *resp.Writer, args [][]byte) bool {
	numkeys, ok := parseInt(args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if numkeys < 1 {
		w.WriteError("ERR at least 1 input key is needed for 'zintercard' command")
		return false
	}
	if 2+numkeys > len(args) {
		w.WriteError("ERR syntax error")
		return false
	}
	keys := byteStrings(args[2 : 2+numkeys])
	limit := 0
	if rest := args[2+numkeys:]; len(rest) > 0 {
		if len(rest) != 2 || !strings.EqualFold(string(rest[0]), "LIMIT") {
			w.WriteError("ERR syntax error")
			return false
		}
		// A LIMIT that is not a number and one that is negative get the same message, which
		// is Redis's: both are "the limit you gave is not a limit", and its own test asserts
		// on the LIMIT prefix for each.
		n, ok := parseInt(rest[1])
		if !ok || n < 0 {
			w.WriteError("ERR LIMIT can't be negative")
			return false
		}
		limit = n
	}
	n, err := s.store.ZInterCard(keys, limit)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}
