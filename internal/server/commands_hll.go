package server

// The HyperLogLog commands: PFADD, PFCOUNT, PFMERGE, plus PFDEBUG and PFSELFTEST.
//
// A HyperLogLog is a string, and its bytes are Redis's -- see
// internal/store/hyperloglog.go for the format and the estimator. That means these
// commands interoperate with the string commands the way Redis's do: GET returns the
// sketch, SET replaces it, and a sketch copied out of one server with GET can be SET
// into another (or into real Redis) and counted to the same number.
//
// All three propagate verbatim. PFADD and PFMERGE are pure functions of their arguments
// and the registers they read -- the hash is seeded by a constant, and a merge is a
// per-register maximum -- so a replica applying the same command reaches the same bytes.
// This is the case where verbatim propagation is *better* than shipping an effect: the
// effect would be a 12KB SET.

import (
	"errors"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("PFADD", -2, true, cmdPFAdd)
	register("PFCOUNT", -2, false, cmdPFCount)
	register("PFMERGE", -2, true, cmdPFMerge)
	register("PFDEBUG", -3, false, cmdPFDebug)
	register("PFSELFTEST", 1, false, cmdPFSelfTest)
}

// errNotHLL is the message Redis sends for a string that is not a HyperLogLog. It is a
// WRONGTYPE, not an ERR, because that is what it is: the key holds a string, but not a
// string of this kind.
const errNotHLL = "WRONGTYPE Key is not a valid HyperLogLog string value."

func writeHLLErr(w *resp.Writer, err error) {
	if errors.Is(err, store.ErrNotHLL) {
		w.WriteError(errNotHLL)
		return
	}
	writeStoreErr(w, err)
}

// cmdPFAdd implements PFADD key [element ...].
//
// With no elements it creates an empty sketch and reports 1 if it did, which is the one
// way to get an empty HyperLogLog into the keyspace. The reply is 1 whenever the value
// changed at all -- a register moved, or the key was created -- and 0 when every element
// was already accounted for.
func cmdPFAdd(s *Server, w *resp.Writer, args [][]byte) bool {
	updated, err := s.store.PFAdd(string(args[1]), args[2:])
	if err != nil {
		writeHLLErr(w, err)
		return false
	}
	w.WriteInt(int64(boolToInt(updated)))
	return updated
}

// cmdPFCount implements PFCOUNT key [key ...]: the estimated cardinality of one sketch,
// or of the union of several.
func cmdPFCount(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.PFCount(byteStrings(args[1:]))
	if err != nil {
		writeHLLErr(w, err)
		return false
	}
	w.WriteInt(n)
	return false
}

// cmdPFMerge implements PFMERGE destkey [sourcekey ...]. The destination is part of the
// union when it already exists, so a merge is accumulative.
func cmdPFMerge(s *Server, w *resp.Writer, args [][]byte) bool {
	changed, err := s.store.PFMerge(string(args[1]), byteStrings(args[2:]))
	if err != nil {
		writeHLLErr(w, err)
		return false
	}
	w.WriteSimple("OK")
	return changed
}

// cmdPFDebug implements PFDEBUG GETREG|ENCODING|TODENSE key.
//
// Redis's PFDEBUG is a test hook, and it is worth having for the same reason its
// PFSELFTEST is: the interesting failures in a HyperLogLog are silent, so being able to
// look at the registers of a running server is the difference between "the count is
// wrong" and "register 9312 is wrong".
//
// TODENSE writes, so it is refused on a replica by being routed through the write path;
// it is registered as a read because the other two subcommands are, and it propagates
// nothing -- the encoding is a representation detail, and a replica reached its own
// encoding by the same rule the master did.
func cmdPFDebug(s *Server, w *resp.Writer, args [][]byte) bool {
	key := string(args[2])
	switch strings.ToUpper(string(args[1])) {
	case "GETREG":
		regs, ok, err := s.store.PFRegisters(key)
		if err != nil {
			writeHLLErr(w, err)
			return false
		}
		if !ok {
			w.WriteError("ERR The specified key does not exist")
			return false
		}
		w.WriteArrayHeader(len(regs))
		for _, r := range regs {
			w.WriteInt(int64(r))
		}

	case "ENCODING":
		enc, ok, err := s.store.PFEncoding(key)
		if err != nil {
			writeHLLErr(w, err)
			return false
		}
		if !ok {
			w.WriteError("ERR The specified key does not exist")
			return false
		}
		w.WriteSimple(enc)

	case "TODENSE":
		changed, ok, err := s.store.PFToDense(key)
		if err != nil {
			writeHLLErr(w, err)
			return false
		}
		if !ok {
			w.WriteError("ERR The specified key does not exist")
			return false
		}
		w.WriteInt(int64(boolToInt(changed)))

	default:
		// PFDEBUG is the one container that does *not* use the shared wording: Redis spells
		// it "ERR Unknown PFDEBUG subcommand 'X'" with no arity clause and no "Try ... HELP"
		// (it has no HELP to try). Measured on redis:7.2; converting it to the shared helper
		// would have been a regression dressed as a cleanup.
		w.WriteError("ERR Unknown PFDEBUG subcommand '" + string(args[1]) + "'")
	}
	return false
}

// cmdPFSelfTest runs the implementation's own consistency check: that the 6-bit dense
// packing round-trips every register value at every position, that the sparse encoding
// round-trips, and that an empty sketch counts zero.
//
// The point of it being a command rather than a Go test is that it can be run against a
// server that is already running -- which is when a packing bug would matter, and when a
// test binary cannot help.
func cmdPFSelfTest(s *Server, w *resp.Writer, args [][]byte) bool {
	if err := store.HLLSelfTest(); err != nil {
		w.WriteError("ERR " + err.Error())
		return false
	}
	w.WriteSimple("OK")
	return false
}
