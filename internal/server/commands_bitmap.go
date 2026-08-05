package server

// The bit commands: SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP and BITFIELD.
//
// They all operate on the string type -- a bitmap here is a string addressed a bit at a
// time, exactly as in Redis -- so they interoperate with SETRANGE, APPEND, STRLEN and
// GETRANGE by construction rather than by agreement. See internal/store/bitmap.go for
// the bit numbering and the size cap.
//
// All of them propagate verbatim. Every one is a pure function of its arguments and the
// value it reads: there is no clock, no randomness and no map iteration order anywhere in
// the family, so a replica replaying the command reaches the same bytes. BITFIELD is the
// interesting case -- its OVERFLOW policies make it non-linear, but not
// non-deterministic -- and it is registered as an ordinary write for that reason.

import (
	"errors"
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("SETBIT", 4, true, cmdSetBit)
	register("GETBIT", 3, false, cmdGetBit)
	register("BITCOUNT", -2, false, cmdBitCount)
	register("BITPOS", -3, false, cmdBitPos)
	register("BITOP", -4, true, cmdBitOp)
	register("BITFIELD", -2, true, cmdBitField)
	// BITFIELD_RO is the read-only subset, which exists so a client can send it to a
	// replica. It rejects SET and INCRBY rather than silently accepting them.
	register("BITFIELD_RO", -2, false, cmdBitFieldRO)
}

// writeBitErr maps the bit sentinels onto Redis's messages.
func writeBitErr(w *resp.Writer, err error) {
	switch {
	case errors.Is(err, store.ErrBitOffset):
		w.WriteError("ERR bit offset is not an integer or out of range")
	case errors.Is(err, store.ErrBitValue):
		w.WriteError("ERR bit is not an integer or out of range")
	case errors.Is(err, store.ErrStringTooLong):
		w.WriteError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	default:
		writeStoreErr(w, err)
	}
}

func cmdSetBit(s *Server, w *resp.Writer, args [][]byte) bool {
	offset, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR bit offset is not an integer or out of range")
		return false
	}
	v, ok := parseInt64(args[3])
	if !ok || (v != 0 && v != 1) {
		w.WriteError("ERR bit is not an integer or out of range")
		return false
	}
	old, err := s.store.SetBit(string(args[1]), offset, v == 1)
	if err != nil {
		writeBitErr(w, err)
		return false
	}
	w.WriteInt(int64(old))
	// Always dirty, even when the bit already had that value: SETBIT also grows the
	// string, so "the bit did not change" does not mean "the value did not change".
	return true
}

func cmdGetBit(s *Server, w *resp.Writer, args [][]byte) bool {
	offset, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR bit offset is not an integer or out of range")
		return false
	}
	bit, err := s.store.GetBit(string(args[1]), offset)
	if err != nil {
		writeBitErr(w, err)
		return false
	}
	w.WriteInt(int64(bit))
	return false
}

// parseBitRange parses the "[start end [BYTE|BIT]]" tail BITCOUNT and BITPOS share.
// present reports whether a range was given at all, and hasEnd whether an explicit end
// was -- BITPOS treats "start only" differently from "start and end".
func parseBitRange(args [][]byte, from int) (r store.BitRange, hasEnd bool, errMsg string) {
	rest := args[from:]
	if len(rest) == 0 {
		return r, false, ""
	}
	start, ok := parseInt64(rest[0])
	if !ok {
		return r, false, "ERR value is not an integer or out of range"
	}
	r.Present = true
	r.Start = start
	if len(rest) == 1 {
		// BITPOS accepts a start on its own, meaning "to the end of the value". BITCOUNT
		// does not, and rejects it by passing a from index that makes this case impossible.
		r.End = -1
		return r, false, ""
	}
	end, ok := parseInt64(rest[1])
	if !ok {
		return r, false, "ERR value is not an integer or out of range"
	}
	r.End = end
	hasEnd = true
	switch len(rest) {
	case 2:
	case 3:
		switch strings.ToUpper(string(rest[2])) {
		case "BYTE":
		case "BIT":
			r.Bits = true
		default:
			return r, false, "ERR syntax error"
		}
	default:
		return r, false, "ERR syntax error"
	}
	return r, hasEnd, ""
}

// cmdBitCount implements BITCOUNT key [start end [BYTE|BIT]].
func cmdBitCount(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args) == 3 {
		// A start with no end is a syntax error here, unlike in BITPOS: BITCOUNT's range
		// has always been a pair, and accepting a lone start would silently count a
		// different thing from what a client that mistyped it meant.
		w.WriteError("ERR syntax error")
		return false
	}
	r, _, errMsg := parseBitRange(args, 2)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	n, err := s.store.BitCount(string(args[1]), r)
	if err != nil {
		writeBitErr(w, err)
		return false
	}
	w.WriteInt(n)
	return false
}

// cmdBitPos implements BITPOS key bit [start [end [BYTE|BIT]]].
func cmdBitPos(s *Server, w *resp.Writer, args [][]byte) bool {
	bit, ok := parseInt64(args[2])
	if !ok || (bit != 0 && bit != 1) {
		w.WriteError("ERR The bit argument must be 1 or 0.")
		return false
	}
	r, hasEnd, errMsg := parseBitRange(args, 3)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	pos, err := s.store.BitPos(string(args[1]), int(bit), r, hasEnd)
	if err != nil {
		writeBitErr(w, err)
		return false
	}
	w.WriteInt(pos)
	return false
}

// cmdBitOp implements BITOP AND|OR|XOR|NOT destkey srckey [srckey...].
func cmdBitOp(s *Server, w *resp.Writer, args [][]byte) bool {
	var op store.BitOpKind
	switch strings.ToUpper(string(args[1])) {
	case "AND":
		op = store.BitOpAnd
	case "OR":
		op = store.BitOpOr
	case "XOR":
		op = store.BitOpXor
	case "NOT":
		op = store.BitOpNot
		if len(args) != 4 {
			w.WriteError("ERR BITOP NOT must be called with a single source key.")
			return false
		}
	default:
		w.WriteError("ERR syntax error")
		return false
	}
	n, err := s.store.BitOp(op, string(args[2]), byteStrings(args[3:]))
	if err != nil {
		writeBitErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	// Always dirty: the destination is replaced even when the result is empty, which is
	// a deletion the replica has to perform too.
	return true
}

// parseBitFieldType parses a "u8"/"i53" type operand into its width and signedness.
//
// Unsigned is capped at 63 bits and signed at 64, which is Redis's rule: the reply is an
// integer, and a 64-bit unsigned value has no room in one.
func parseBitFieldType(b []byte) (signed bool, width int, ok bool) {
	if len(b) < 2 {
		return false, 0, false
	}
	switch b[0] {
	case 'u':
	case 'i':
		signed = true
	default:
		return false, 0, false
	}
	n, err := strconv.Atoi(string(b[1:]))
	if err != nil || n < 1 {
		return false, 0, false
	}
	if signed && n > 64 {
		return false, 0, false
	}
	if !signed && n > 63 {
		return false, 0, false
	}
	return signed, n, true
}

// parseBitFieldOffset parses an offset operand, honouring the "#" form: "#2" with a u8
// type means the third 8-bit field, i.e. bit 16. It is what lets a client address an
// array of counters without doing the multiplication itself.
func parseBitFieldOffset(b []byte, width int) (int64, bool) {
	s := string(b)
	if strings.HasPrefix(s, "#") {
		n, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil || n < 0 {
			return 0, false
		}
		return n * int64(width), true
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// parseBitField parses the whole operation list. readOnly rejects the mutating
// subcommands, for BITFIELD_RO.
func parseBitField(args [][]byte, readOnly bool) ([]store.BitFieldOp, string) {
	var ops []store.BitFieldOp
	overflow := store.OverflowWrap
	for i := 2; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "OVERFLOW":
			if readOnly {
				return nil, "ERR BITFIELD_RO only supports the GET subcommand"
			}
			if i+1 >= len(args) {
				return nil, "ERR syntax error"
			}
			switch strings.ToUpper(string(args[i+1])) {
			case "WRAP":
				overflow = store.OverflowWrap
			case "SAT":
				overflow = store.OverflowSat
			case "FAIL":
				overflow = store.OverflowFail
			default:
				return nil, "ERR Invalid OVERFLOW type specified"
			}
			// The policy applies to every operation that follows it, and a later OVERFLOW
			// replaces it. That is Redis's rule, and it is why the policy is threaded through
			// the loop rather than parsed once.
			i += 2

		case "GET":
			if i+2 >= len(args) {
				return nil, "ERR syntax error"
			}
			signed, width, ok := parseBitFieldType(args[i+1])
			if !ok {
				return nil, "ERR Invalid bitfield type. Use something like i16 u8. " +
					"Note that u64 is not supported but i64 is."
			}
			offset, ok := parseBitFieldOffset(args[i+2], width)
			if !ok {
				return nil, "ERR bit offset is not an integer or out of range"
			}
			ops = append(ops, store.BitFieldOp{
				Kind: store.BitFieldGet, Signed: signed, Bits: width, Offset: offset,
			})
			i += 3

		case "SET", "INCRBY":
			if readOnly {
				return nil, "ERR BITFIELD_RO only supports the GET subcommand"
			}
			if i+3 >= len(args) {
				return nil, "ERR syntax error"
			}
			kind := store.BitFieldSet
			if strings.EqualFold(string(args[i]), "INCRBY") {
				kind = store.BitFieldIncrBy
			}
			signed, width, ok := parseBitFieldType(args[i+1])
			if !ok {
				return nil, "ERR Invalid bitfield type. Use something like i16 u8. " +
					"Note that u64 is not supported but i64 is."
			}
			offset, ok := parseBitFieldOffset(args[i+2], width)
			if !ok {
				return nil, "ERR bit offset is not an integer or out of range"
			}
			value, ok := parseInt64(args[i+3])
			if !ok {
				return nil, "ERR value is not an integer or out of range"
			}
			if offset+int64(width) > maxBitFieldBit {
				return nil, "ERR bit offset is not an integer or out of range"
			}
			ops = append(ops, store.BitFieldOp{
				Kind: kind, Signed: signed, Bits: width, Offset: offset,
				Value: value, Overflow: overflow,
			})
			i += 4

		default:
			return nil, "ERR syntax error"
		}
	}
	if len(ops) == 0 {
		return nil, "ERR wrong number of arguments for 'bitfield' command"
	}
	return ops, ""
}

// maxBitFieldBit is one past the last addressable bit, so a field ending beyond it is
// refused before anything is allocated. It matches the cap on a string's length.
const maxBitFieldBit = int64(512<<20) * 8

// cmdBitField implements BITFIELD key
// [GET type offset] [SET type offset value] [INCRBY type offset increment]
// [OVERFLOW WRAP|SAT|FAIL] ...
func cmdBitField(s *Server, w *resp.Writer, args [][]byte) bool {
	return bitField(s, w, args, false)
}

func cmdBitFieldRO(s *Server, w *resp.Writer, args [][]byte) bool {
	bitField(s, w, args, true)
	return false
}

func bitField(s *Server, w *resp.Writer, args [][]byte, readOnly bool) bool {
	ops, errMsg := parseBitField(args, readOnly)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	results, changed, err := s.store.BitField(string(args[1]), ops)
	if err != nil {
		writeBitErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(results))
	for _, r := range results {
		if !r.Present {
			// OVERFLOW FAIL refused this operation. A null in its slot is how the reply says
			// so without disturbing the positional correspondence a client relies on.
			w.WriteNull()
			continue
		}
		w.WriteInt(r.Value)
	}
	return changed
}
