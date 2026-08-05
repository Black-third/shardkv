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
// command. count is a hint at how much work one call should do; hasPattern
// distinguishes "no MATCH" from "MATCH " with an empty pattern.
type scanOpts struct {
	pattern    string
	hasPattern bool
	count      int
}

// parseScanOpts parses the option tail of a cursor command, returning the RESP
// error message to reply with or "".
//
// The options are strictly name/value pairs: a trailing name with no value, or one
// stray extra argument, is a syntax error rather than something to ignore.
func parseScanOpts(tail [][]byte) (scanOpts, string) {
	o := scanOpts{count: 10}
	if len(tail)%2 != 0 {
		return o, "ERR syntax error"
	}
	for i := 0; i < len(tail); i += 2 {
		switch strings.ToUpper(string(tail[i])) {
		case "MATCH":
			o.pattern = string(tail[i+1])
			o.hasPattern = true
		case "COUNT":
			n, ok := parseInt64(tail[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			if n < 1 { // Redis rejects COUNT 0 and negatives outright
				return o, "ERR syntax error"
			}
			o.count = int(min(n, math.MaxInt32))
		default:
			return o, "ERR syntax error"
		}
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
	o, errMsg := parseScanOpts(args[2:])
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}

	keys, next := s.store.Scan(cursor, o.count)
	if o.hasPattern {
		filtered := keys[:0]
		for _, k := range keys {
			if globMatch(o.pattern, k) {
				filtered = append(filtered, k)
			}
		}
		keys = filtered
	}
	writeScanReply(w, next, keys)
	return false
}

// cmdHScan implements HSCAN key cursor [MATCH pattern] [COUNT n], whose elements
// are flattened field/value pairs. MATCH filters on the field name.
func cmdHScan(s *Server, w *resp.Writer, args [][]byte) bool {
	return collectionScan(s, w, args, func(key string) ([]string, error) {
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
	return collectionScan(s, w, args, func(key string) ([]string, error) {
		return s.store.SMembers(key)
	}, 1)
}

// cmdZScan implements ZSCAN key cursor [MATCH pattern] [COUNT n], whose elements are
// flattened member/score pairs. MATCH filters on the member.
func cmdZScan(s *Server, w *resp.Writer, args [][]byte) bool {
	return collectionScan(s, w, args, func(key string) ([]string, error) {
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
func collectionScan(s *Server, w *resp.Writer, args [][]byte, elems func(string) ([]string, error), stride int) bool {
	cursor, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		w.WriteError("ERR invalid cursor")
		return false
	}
	o, errMsg := parseScanOpts(args[3:])
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	if cursor != 0 {
		writeScanReply(w, 0, nil)
		return false
	}
	items, err := elems(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
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
	w.WriteBulk([]byte(strconv.FormatUint(next, 10)))
	writeStrings(w, items)
}
