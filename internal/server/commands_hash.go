package server

import (
	"math"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("HSET", -4, true, cmdHSet)
	register("HGET", 3, false, cmdHGet)
	register("HDEL", -3, true, cmdHDel)
	register("HGETALL", 2, false, cmdHGetAll)
	register("HLEN", 2, false, cmdHLen)
	register("HKEYS", 2, false, cmdHKeys)
	register("HVALS", 2, false, cmdHVals)
	register("HEXISTS", 3, false, cmdHExists)
	register("HMGET", -3, false, cmdHMGet)
	register("HSTRLEN", 3, false, cmdHStrLen)
	register("HSETNX", 4, true, cmdHSetNX)
	register("HINCRBY", 4, true, cmdHIncrBy)
	registerEffect("HINCRBYFLOAT", 4, cmdHIncrByFloat)
	register("HRANDFIELD", -2, false, cmdHRandField)
	// HMSET is HSET's predecessor: identical behaviour, a +OK reply instead of a count.
	// Deprecated since 4.0 and still what a great deal of code sends.
	register("HMSET", -4, true, cmdHMSet)
}

func cmdHSet(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args)%2 != 0 {
		w.WriteError("ERR wrong number of arguments for 'hset' command")
		return false
	}
	pairs := make([][2][]byte, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		pairs = append(pairs, [2][]byte{args[i], args[i+1]})
	}
	created, err := s.store.HSet(string(args[1]), pairs...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(created))
	return true
}

// cmdHMSet is HSET with Redis's older reply: +OK rather than the number of fields
// created. It exists because HMSET is what a decade of client code and examples send, and
// because a server that answers "unknown command" to it looks broken rather than modern.
//
// The arity is -4 rather than HSET's -3: HMSET has no single-pair-less form at all, so
// `HMSET key` is an arity error while `HSET key` is one too, by the same rule stated
// differently.
func cmdHMSet(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args)%2 != 0 {
		w.WriteError("ERR wrong number of arguments for 'hmset' command")
		return false
	}
	pairs := make([][2][]byte, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		pairs = append(pairs, [2][]byte{args[i], args[i+1]})
	}
	if _, err := s.store.HSet(string(args[1]), pairs...); err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteSimple("OK")
	return true
}

func cmdHGet(s *Server, w *resp.Writer, args [][]byte) bool {
	v, ok, err := s.store.HGet(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteBulk(v)
	return false
}

func cmdHDel(s *Server, w *resp.Writer, args [][]byte) bool {
	fields := make([]string, 0, len(args)-2)
	for _, f := range args[2:] {
		fields = append(fields, string(f))
	}
	n, err := s.store.HDel(string(args[1]), fields...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return n > 0
}

// cmdHGetAll replies with the hash as a map: a RESP3 map type, and the flat
// field/value array a RESP2 client pairs up itself. It is the canonical example of a
// reply RESP3 reshapes -- a RESP3 client decodes it straight into a dictionary
// instead of walking a list two elements at a time.
func cmdHGetAll(s *Server, w *resp.Writer, args [][]byte) bool {
	flat, err := s.store.HGetAll(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeMapBulks(w, flat)
	return false
}

func cmdHLen(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.HLen(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

func cmdHKeys(s *Server, w *resp.Writer, args [][]byte) bool {
	fields, err := s.store.HKeys(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(fields))
	for _, f := range fields {
		w.WriteBulk([]byte(f))
	}
	return false
}

func cmdHVals(s *Server, w *resp.Writer, args [][]byte) bool {
	vals, err := s.store.HVals(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(vals))
	for _, v := range vals {
		w.WriteBulk(v)
	}
	return false
}

func cmdHExists(s *Server, w *resp.Writer, args [][]byte) bool {
	ok, err := s.store.HExists(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(boolToInt(ok)))
	return false
}

// cmdHMGet always replies with one element per requested field, a null for each
// one the hash does not hold -- including every field of a missing key.
func cmdHMGet(s *Server, w *resp.Writer, args [][]byte) bool {
	fields := make([]string, 0, len(args)-2)
	for _, f := range args[2:] {
		fields = append(fields, string(f))
	}
	vals, err := s.store.HMGet(string(args[1]), fields...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(vals))
	for _, v := range vals {
		w.WriteBulk(v) // a nil value is the null bulk string
	}
	return false
}

func cmdHStrLen(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.HStrLen(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

func cmdHSetNX(s *Server, w *resp.Writer, args [][]byte) bool {
	set, err := s.store.HSetNX(string(args[1]), string(args[2]), args[3])
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(boolToInt(set)))
	return set
}

func cmdHIncrBy(s *Server, w *resp.Writer, args [][]byte) bool {
	delta, ok := parseInt64(args[3])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	n, err := s.store.HIncrBy(string(args[1]), string(args[2]), delta)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return true
}

// cmdHIncrByFloat propagates the resulting field value as an HSET rather than
// shipping the increment, for the same reason INCRBYFLOAT does: replaying a sum
// cannot drift, while replaying an addition relies on the replica reproducing the
// master's floating-point arithmetic exactly.
func cmdHIncrByFloat(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	delta, ok := parseScore(args[3])
	if !ok {
		w.WriteError("ERR value is not a valid float")
		return nil
	}
	// An infinite *operand* is refused here, and named as the operand. That is where
	// HINCRBYFLOAT differs from INCRBYFLOAT, which lets an infinity through and reports it
	// against the result: Redis checks the operand in one and not the other, with two
	// different messages, and its own hash test asserts on this one. The result is still
	// checked in the store, which is what catches inf + -inf.
	if math.IsInf(delta, 0) || math.IsNaN(delta) {
		w.WriteError("ERR value is NaN or Infinity")
		return nil
	}
	// The stored text is what is replied and what is propagated -- see cmdIncrByFloat for
	// why re-formatting the float here was wrong.
	_, val, err := s.store.HIncrByFloat(string(args[1]), string(args[2]), delta)
	if err != nil {
		writeStoreErr(w, err)
		return nil
	}
	w.WriteBulk([]byte(val))
	return [][][]byte{{[]byte("HSET"), args[1], args[2], []byte(val)}}
}

// cmdHRandField implements HRANDFIELD key [count [WITHVALUES]]. It is a read, so
// nothing is propagated -- which is the point: the fields are drawn at random, and
// a replica asked to run the same command would answer with different ones.
func cmdHRandField(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args) > 4 {
		w.WriteError("ERR syntax error")
		return false
	}
	// Without a count the reply is a single field (or a null), not an array.
	if len(args) == 2 {
		fields, err := s.store.HRandField(string(args[1]), 1, false)
		if err != nil {
			writeStoreErr(w, err)
			return false
		}
		if len(fields) == 0 {
			w.WriteNull()
			return false
		}
		w.WriteBulk(fields[0])
		return false
	}
	// WITHVALUES is read before the count, because it halves the range the count may take:
	// the reply carries a value per field, so Redis refuses at half the magnitude.
	withValues := false
	if len(args) == 4 {
		if !strings.EqualFold(string(args[3]), "WITHVALUES") {
			w.WriteError("ERR syntax error")
			return false
		}
		withValues = true
	}
	count, errMsg := parseRandomCount(args[2], withValues)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	out, err := s.store.HRandField(string(args[1]), count, withValues)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !withValues {
		w.WriteArrayHeader(len(out))
		for _, v := range out {
			w.WriteBulk(v)
		}
		return false
	}
	// WITHVALUES is a run of field/value pairs, which RESP3 nests and RESP2 flattens.
	// It is not a map: a negative count draws with replacement, so the same field can
	// appear twice and a map reply would silently collapse the duplicates.
	nested := writePairsHeader(w, len(out)/2)
	for i := 0; i+1 < len(out); i += 2 {
		if nested {
			w.WriteArrayHeader(2)
		}
		w.WriteBulk(out[i])
		w.WriteBulk(out[i+1])
	}
	return false
}
