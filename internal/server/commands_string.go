package server

import (
	"math"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("SET", -3, true, cmdSet)
	register("GET", 2, false, cmdGet)
	register("GETEX", -2, true, cmdGetEx)
	register("INCR", 2, true, cmdIncr)
	register("DECR", 2, true, cmdDecr)
	register("INCRBY", 3, true, cmdIncrBy)
	register("DECRBY", 3, true, cmdDecrBy)
	registerEffect("INCRBYFLOAT", 3, cmdIncrByFloat)
	register("MSET", -3, true, cmdMSet)
	register("MSETNX", -3, true, cmdMSetNX)
	register("MGET", -2, false, cmdMGet)
	register("SETNX", 3, true, cmdSetNX)
	register("SETEX", 4, true, cmdSetEx)
	register("PSETEX", 4, true, cmdPSetEx)
	register("GETSET", 3, true, cmdGetSet)
	register("GETDEL", 2, true, cmdGetDel)
	register("APPEND", 3, true, cmdAppend)
	register("STRLEN", 2, false, cmdStrLen)
	register("SETRANGE", 4, true, cmdSetRange)
	register("GETRANGE", 4, false, cmdGetRange)
	register("SUBSTR", 4, false, cmdGetRange) // legacy alias
}

// setOpts is the parsed option tail of SET. atMs is only meaningful with
// hasDeadline, and is already absolute: the relative EX/PX operand is resolved
// against the caller-supplied clock reading, so the handler and propagationForm
// derive the identical instant from the identical arithmetic.
type setOpts struct {
	nx, xx      bool
	get         bool
	keepTTL     bool
	hasDeadline bool
	atMs        int64
}

// parseSetOpts parses SET's options, returning the RESP error message to reply
// with or "" when they are valid. NX and XX are mutually exclusive, and an expiry
// option excludes both KEEPTTL and a second expiry option -- Redis rejects each of
// those combinations rather than letting the last one win.
func parseSetOpts(args [][]byte, nowMs int64) (setOpts, string) {
	var o setOpts
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			if o.xx {
				return o, "ERR syntax error"
			}
			o.nx = true
		case "XX":
			if o.nx {
				return o, "ERR syntax error"
			}
			o.xx = true
		case "GET":
			o.get = true
		case "KEEPTTL":
			if o.hasDeadline {
				return o, "ERR syntax error"
			}
			o.keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if o.hasDeadline || o.keepTTL || i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			unitMs, rel := expireUnit(string(args[i]))
			n, ok := parseInt64(args[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			// Redis rejects a non-positive operand for every one of the four,
			// absolute included: a deadline of zero or less can only mean a key that
			// is already gone.
			if n <= 0 {
				return o, "ERR invalid expire time in 'set' command"
			}
			atMs, ok := deadlineMs(nowMs, n, unitMs, rel)
			if !ok {
				return o, "ERR invalid expire time in 'set' command"
			}
			o.hasDeadline, o.atMs = true, atMs
			i++
		default:
			return o, "ERR syntax error"
		}
	}
	return o, ""
}

// expireUnit maps an EX/PX/EXAT/PXAT option name to the unit of its operand in
// milliseconds and whether that operand is relative to now.
func expireUnit(name string) (unitMs int64, rel bool) {
	switch strings.ToUpper(name) {
	case "EX":
		return 1000, true
	case "PX":
		return 1, true
	case "EXAT":
		return 1000, false
	default: // PXAT
		return 1, false
	}
}

// cmdSet implements SET with its full option set. It resolves any relative expiry
// against the store's clock -- the same one propagationForm uses -- so the deadline
// in memory equals the deadline on the wire.
func cmdSet(s *Server, w *resp.Writer, args [][]byte) bool {
	o, errMsg := parseSetOpts(args, s.store.Now().UnixMilli())
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	opts := store.SetOptions{
		NX:          o.nx,
		XX:          o.xx,
		Get:         o.get,
		KeepTTL:     o.keepTTL,
		HasDeadline: o.hasDeadline,
	}
	if o.hasDeadline {
		opts.Deadline = time.UnixMilli(o.atMs)
	}
	old, oldOK, set, err := s.store.SetWithOptions(string(args[1]), args[2], opts)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	switch {
	case o.get && oldOK:
		w.WriteBulk(old)
	case o.get, !set:
		// GET reports the absent previous value, and a plain NX/XX that did not fire
		// reports the same null -- the client's signal that nothing was written.
		w.WriteNull()
	default:
		w.WriteSimple("OK")
	}
	return set
}

func cmdSetEx(s *Server, w *resp.Writer, args [][]byte) bool {
	return setex(s, w, args, "setex", 1000)
}

func cmdPSetEx(s *Server, w *resp.Writer, args [][]byte) bool {
	return setex(s, w, args, "psetex", 1)
}

// setex applies SETEX/PSETEX: a SET whose TTL operand comes before the value.
// unitMs is that operand's unit; name only shapes the error reply. Like SET it
// resolves the deadline against the store's clock, so the absolute SET this
// propagates as carries the very instant written to memory.
func setex(s *Server, w *resp.Writer, args [][]byte, name string, unitMs int64) bool {
	n, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if n <= 0 {
		w.WriteError("ERR invalid expire time in '" + name + "' command")
		return false
	}
	atMs, ok := deadlineMs(s.store.Now().UnixMilli(), n, unitMs, true)
	if !ok {
		w.WriteError("ERR invalid expire time in '" + name + "' command")
		return false
	}
	s.store.SetDeadline(string(args[1]), args[3], time.UnixMilli(atMs))
	w.WriteSimple("OK")
	return true
}

// getExOpts is the parsed option of GETEX. apply says whether the expiry is
// touched at all (GETEX with no option is a plain read); persist means the TTL is
// removed rather than replaced.
type getExOpts struct {
	apply   bool
	persist bool
	atMs    int64
}

// parseGetExOpts parses GETEX's single optional argument, returning the RESP error
// message to reply with or "" when it is valid.
func parseGetExOpts(args [][]byte, nowMs int64) (getExOpts, string) {
	var o getExOpts
	if len(args) == 2 {
		return o, ""
	}
	switch strings.ToUpper(string(args[2])) {
	case "PERSIST":
		if len(args) != 3 {
			return o, "ERR syntax error"
		}
		o.apply, o.persist = true, true
		return o, ""
	case "EX", "PX", "EXAT", "PXAT":
		if len(args) != 4 {
			return o, "ERR syntax error"
		}
		unitMs, rel := expireUnit(string(args[2]))
		n, ok := parseInt64(args[3])
		if !ok {
			return o, "ERR value is not an integer or out of range"
		}
		if n <= 0 {
			return o, "ERR invalid expire time in 'getex' command"
		}
		atMs, ok := deadlineMs(nowMs, n, unitMs, rel)
		if !ok {
			return o, "ERR invalid expire time in 'getex' command"
		}
		o.apply, o.atMs = true, atMs
		return o, ""
	}
	return o, "ERR syntax error"
}

// cmdGetEx reads a string and rewrites its expiry in the same step. It reports
// dirty only when the expiry actually moved: GETEX with no option, or a PERSIST on
// a key that never expired, changes nothing and must not reach the AOF.
func cmdGetEx(s *Server, w *resp.Writer, args [][]byte) bool {
	o, errMsg := parseGetExOpts(args, s.store.Now().UnixMilli())
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	var deadline time.Time
	if o.apply && !o.persist {
		deadline = time.UnixMilli(o.atMs)
	}
	v, ok, changed, err := s.store.GetEx(string(args[1]), deadline, o.apply)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteBulk(v)
	return changed
}

func cmdSetRange(s *Server, w *resp.Writer, args [][]byte) bool {
	offset, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	n, err := s.store.SetRange(string(args[1]), offset, args[3])
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return len(args[3]) > 0 // an empty value only reports the current length
}

func cmdGetRange(s *Server, w *resp.Writer, args [][]byte) bool {
	start, ok1 := parseInt(args[2])
	end, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	v, err := s.store.GetRange(string(args[1]), start, end)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteBulk(emptyIfNil(v))
	return false
}

// cmdIncrByFloat adds a float delta to a string, replying with the new value as a
// bulk string (not an integer).
//
// It propagates the result rather than the increment, as Redis does: replaying an
// addition depends on the replica reproducing the master's floating-point
// arithmetic bit for bit, while replaying the sum cannot drift at all. KEEPTTL is
// what keeps that rewrite honest -- the increment preserved the key's TTL, so the
// SET standing in for it must not clear the replica's.
func cmdIncrByFloat(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	// An infinite *operand* parses -- "inf" is a valid float, and Redis's string2ld
	// accepts it while rejecting NaN -- so the infinity is caught on the *result*, where
	// the error names what actually went wrong ("increment would produce NaN or Infinity")
	// rather than blaming the operand for being unparseable when it parsed fine.
	// parseScore has precisely those semantics: infinities in, NaN out.
	delta, ok := parseScore(args[2])
	if !ok {
		w.WriteError("ERR value is not a valid float")
		return nil
	}
	f, err := s.store.IncrByFloat(string(args[1]), delta)
	if err != nil {
		writeStoreErr(w, err)
		return nil
	}
	val := []byte(formatFloat(f))
	w.WriteBulk(val)
	return [][][]byte{{[]byte("SET"), args[1], val, []byte("KEEPTTL")}}
}

func cmdIncrBy(s *Server, w *resp.Writer, args [][]byte) bool {
	delta, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	n, err := s.store.Incr(string(args[1]), delta)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return true
}

func cmdDecrBy(s *Server, w *resp.Writer, args [][]byte) bool {
	delta, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	// MinInt64 has no positive counterpart, so negating it would wrap and turn a
	// decrement into a huge decrement in the other direction.
	if delta == math.MinInt64 {
		w.WriteError("ERR increment or decrement would overflow")
		return false
	}
	n, err := s.store.Incr(string(args[1]), -delta)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return true
}

func cmdSetNX(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.store.SetNX(string(args[1]), args[2]) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdGetSet(s *Server, w *resp.Writer, args [][]byte) bool {
	old, ok, err := s.store.GetSet(string(args[1]), args[2])
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if ok {
		w.WriteBulk(old)
	} else {
		w.WriteNull()
	}
	return true
}

func cmdGetDel(s *Server, w *resp.Writer, args [][]byte) bool {
	v, ok, err := s.store.GetDel(string(args[1]))
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

func cmdAppend(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.Append(string(args[1]), args[2])
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return true
}

func cmdStrLen(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.StrLen(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

// cmdGet reads the string at a key. It uses the store's combined lookup rather
// than a Type check followed by a Get: one shard-lock acquisition on the hottest
// read path, and no window in which the type check and the read see different
// states.
func cmdGet(s *Server, w *resp.Writer, args [][]byte) bool {
	v, ok, err := s.store.GetString(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if ok {
		w.WriteBulk(v)
	} else {
		w.WriteNull()
	}
	return false
}

func cmdIncr(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.Incr(string(args[1]), 1)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return true
}

func cmdDecr(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.Incr(string(args[1]), -1)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return true
}

func cmdMSet(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args)%2 != 1 {
		w.WriteError("ERR wrong number of arguments for 'mset' command")
		return false
	}
	for i := 1; i < len(args); i += 2 {
		s.store.Set(string(args[i]), args[i+1], 0)
	}
	w.WriteSimple("OK")
	return true
}

// cmdMSetNX sets every pair only if none of the keys exists, replying 1 when it wrote
// and 0 when it did not.
//
// It is dirty only when it actually wrote, so a refused MSETNX propagates nothing --
// and when it does write, it propagates itself rather than an effect: the command is
// deterministic, and a replica applying the same MSETNX against the same dataset makes
// the same decision.
func cmdMSetNX(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args)%2 != 1 {
		w.WriteError("ERR wrong number of arguments for 'msetnx' command")
		return false
	}
	pairs := make([][2][]byte, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		pairs = append(pairs, [2][]byte{args[i], args[i+1]})
	}
	if s.store.MSetNX(pairs) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdMGet(s *Server, w *resp.Writer, args [][]byte) bool {
	w.WriteArrayHeader(len(args) - 1)
	for _, k := range args[1:] {
		if v, ok := s.store.Get(string(k)); ok {
			w.WriteBulk(v)
		} else {
			w.WriteNull()
		}
	}
	return false
}
