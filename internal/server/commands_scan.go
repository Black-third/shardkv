package server

import (
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("SCAN", -2, false, cmdScan)
}

// cmdScan implements SCAN cursor [MATCH pattern] [COUNT n], returning a two-
// element reply: the next cursor and a batch of keys.
func cmdScan(s *Server, w *resp.Writer, args [][]byte) bool {
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		w.WriteError("ERR invalid cursor")
		return false
	}
	count := 10
	pattern := ""
	hasPattern := false
	for i := 2; i+1 < len(args); i += 2 {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			pattern = string(args[i+1])
			hasPattern = true
		case "COUNT":
			if n, ok := parseInt(args[i+1]); ok && n > 0 {
				count = n
			}
		default:
			w.WriteError("ERR syntax error")
			return false
		}
	}

	keys, next := s.store.Scan(cursor, count)
	if hasPattern {
		filtered := keys[:0]
		for _, k := range keys {
			if globMatch(pattern, k) {
				filtered = append(filtered, k)
			}
		}
		keys = filtered
	}

	w.WriteArrayHeader(2)
	w.WriteBulk([]byte(strconv.FormatUint(next, 10)))
	w.WriteArrayHeader(len(keys))
	for _, k := range keys {
		w.WriteBulk([]byte(k))
	}
	return false
}

// globMatch reports whether s matches a glob pattern supporting '*' (any run)
// and '?' (any single byte), as Redis's MATCH does for the common cases.
func globMatch(pattern, s string) bool {
	pi, si := 0, 0
	star, starS := -1, 0
	for si < len(s) {
		switch {
		case pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]):
			pi++
			si++
		case pi < len(pattern) && pattern[pi] == '*':
			star, starS = pi, si
			pi++
		case star != -1:
			pi = star + 1
			starS++
			si = starS
		default:
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}
