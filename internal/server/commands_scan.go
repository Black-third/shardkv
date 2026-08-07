package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("SCAN", -2, false, cmdScan)
	register("HSCAN", -3, false, cmdHScan)
	register("SSCAN", -3, false, cmdSScan)
	register("ZSCAN", -3, false, cmdZScan)
}

// scanOpts is the parsed [MATCH pattern] [COUNT n] tail shared by every cursor
// command. count is a hint at how much work one call should do; hasPattern says whether
// anything has to be filtered at all, so it distinguishes "no MATCH" -- and the lone "*"
// that means the same thing -- from `MATCH ""`, which keeps only an empty name.
type scanOpts struct {
	pattern    string
	hasPattern bool
	count      int
	// typeFilter keeps only keys of that data type, for SCAN's TYPE option. It is the
	// name TYPE reports (string/list/set/zset/hash/stream), matched case-insensitively as
	// Redis matches it.
	typeFilter string
	// noValues drops the value from each pair, for HSCAN's NOVALUES option: a caller
	// enumerating a hash's field names should not have to receive -- and pay to transfer
	// -- every value alongside them.
	noValues bool
}

// scanFlags says which of the options above the calling command accepts. TYPE is SCAN's
// alone (a collection has one type, so filtering its elements by one is meaningless) and
// NOVALUES is HSCAN's alone (only a hash has values to omit). Passing either to the wrong
// command is a syntax error, which is Redis's answer too.
type scanFlags struct {
	allowType     bool
	allowNoValues bool
}

// parseScanOpts parses the option tail of a cursor command, returning the RESP
// error message to reply with or "".
//
// The options are strictly name/value pairs: a trailing name with no value, or one
// stray extra argument, is a syntax error rather than something to ignore.
func parseScanOpts(tail [][]byte, f scanFlags) (scanOpts, string) {
	o := scanOpts{count: 10}
	for i := 0; i < len(tail); i++ {
		name := strings.ToUpper(string(tail[i]))
		// NOVALUES is the one option with no operand, so the "options are name/value
		// pairs" shortcut no longer holds and each option consumes what it needs.
		if name == "NOVALUES" && f.allowNoValues {
			o.noValues = true
			continue
		}
		if i+1 >= len(tail) {
			return o, "ERR syntax error"
		}
		switch name {
		case "MATCH":
			o.pattern = string(tail[i+1])
			// A lone "*" is recorded as no pattern at all, which is Redis's own `use_pattern`
			// test. It is not only a shortcut: the matcher refuses an empty subject (see
			// globMatch), so a hash field or a key whose name is the empty string -- which
			// HSET and SET both accept -- would otherwise be filtered out by the one pattern
			// that is supposed to keep everything. Measured against redis:7.2 with an empty
			// field name: `HSCAN h 0 MATCH *` reports it, `MATCH **` does not.
			o.hasPattern = o.pattern != "*"
		case "COUNT":
			n, ok := parseInt64(tail[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			if n < 1 { // Redis rejects COUNT 0 and negatives outright
				return o, "ERR syntax error"
			}
			o.count = int(min(n, math.MaxInt32))
		case "TYPE":
			if !f.allowType {
				return o, "ERR syntax error"
			}
			o.typeFilter = strings.ToLower(string(tail[i+1]))
		default:
			return o, "ERR syntax error"
		}
		i++
	}
	return o, ""
}

// cmdScan implements SCAN cursor [MATCH pattern] [COUNT n], returning a two-
// element reply: the next cursor and a batch of keys.
func cmdScan(s *Server, w *resp.Writer, args [][]byte) bool {
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		w.WriteError("ERR invalid cursor")
		return false
	}
	o, errMsg := parseScanOpts(args[2:], scanFlags{allowType: true})
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}

	keys, next := s.store.Scan(cursor, o.count)
	if o.hasPattern || o.typeFilter != "" {
		filtered := keys[:0]
		for _, k := range keys {
			if o.hasPattern && !globMatch(o.pattern, k) {
				continue
			}
			// The type is looked up per surviving key rather than returned by the walk, so
			// MATCH is applied first: a pattern that rejects a key saves the lookup. An
			// unknown type name is not an error -- it simply matches nothing, which is what
			// Redis does, since the set of type names is a property of the server and not
			// of the request.
			if o.typeFilter != "" {
				typ, ok := s.store.Type(k)
				if !ok || typ != o.typeFilter {
					continue
				}
			}
			filtered = append(filtered, k)
		}
		keys = filtered
	}
	writeScanReply(w, next, keys)
	return false
}

// cmdHScan implements HSCAN key cursor [MATCH pattern] [COUNT n] [NOVALUES], whose
// elements are flattened field/value pairs -- or the field names alone under NOVALUES.
// MATCH filters on the field name either way.
func cmdHScan(s *Server, w *resp.Writer, args [][]byte) bool {
	return collectionScan(s, w, args, scanFlags{allowNoValues: true}, func(key string) ([]string, error) {
		flat, err := s.store.HGetAll(key)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(flat))
		for _, v := range flat {
			out = append(out, string(v))
		}
		return out, nil
	}, 2)
}

// cmdSScan implements SSCAN key cursor [MATCH pattern] [COUNT n].
func cmdSScan(s *Server, w *resp.Writer, args [][]byte) bool {
	return collectionScan(s, w, args, scanFlags{}, func(key string) ([]string, error) {
		return s.store.SMembers(key)
	}, 1)
}

// cmdZScan implements ZSCAN key cursor [MATCH pattern] [COUNT n], whose elements are
// flattened member/score pairs. MATCH filters on the member.
func cmdZScan(s *Server, w *resp.Writer, args [][]byte) bool {
	return collectionScan(s, w, args, scanFlags{}, func(key string) ([]string, error) {
		members, err := s.store.ZRange(key, 0, -1)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(members)*2)
		for _, m := range members {
			out = append(out, m.Member, formatFloat(m.Score))
		}
		return out, nil
	}, 2)
}

// collectionScan implements the cursor commands over a single collection. stride is
// the width of one logical element (2 for the field/value of HSCAN and the
// member/score of ZSCAN, 1 for SSCAN), so MATCH filters on the first component of a
// pair and a pair is never split.
//
// A collection is returned in one batch with a next cursor of 0 -- the behaviour
// Redis exhibits for its compact encodings, and the only one available here: the
// elements live in a Go map, whose iteration order is deliberately randomized per
// walk, so a cursor into it could not offer the guarantee SCAN makes (an element
// present for the whole iteration is returned at least once). Returning everything
// at once satisfies that guarantee trivially. Any non-zero cursor therefore means
// the iteration is already complete and yields an empty batch.
func collectionScan(s *Server, w *resp.Writer, args [][]byte, f scanFlags,
	elems func(string) ([]string, error), stride int) bool {
	cursor, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		w.WriteError("ERR invalid cursor")
		return false
	}
	// The collection is read -- and so its type checked -- *before* the options are
	// parsed, which is the order Redis uses and an order that is visible: SSCAN on a list
	// with a bogus option answers WRONGTYPE, not "syntax error". Checked side by side
	// against redis:7.2.
	items, err := elems(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	o, errMsg := parseScanOpts(args[3:], f)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	if cursor != 0 {
		writeScanReply(w, 0, nil)
		return false
	}
	if o.noValues {
		// Drop the value of each pair and narrow the stride to one, so MATCH still filters on
		// the field name and the reply is field names alone.
		kept := items[:0]
		for i := 0; i+stride <= len(items); i += stride {
			kept = append(kept, items[i])
		}
		items = kept
		stride = 1
	}
	if o.hasPattern {
		matched := make([]string, 0, len(items))
		for i := 0; i+stride <= len(items); i += stride {
			if globMatch(o.pattern, items[i]) {
				matched = append(matched, items[i:i+stride]...)
			}
		}
		items = matched
	}
	writeScanReply(w, 0, items)
	return false
}

// writeScanReply writes the two-element [next-cursor, elements] reply every cursor
// command shares.
func writeScanReply(w *resp.Writer, next uint64, items []string) {
	w.WriteArrayHeader(2)
	w.WriteBulkString(strconv.FormatUint(next, 10))
	writeStrings(w, items)
}
