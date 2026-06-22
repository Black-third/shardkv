package server

import (
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("ZADD", -4, true, cmdZAdd)
	register("ZSCORE", 3, false, cmdZScore)
	register("ZREM", 3, true, cmdZRem)
	register("ZCARD", 2, false, cmdZCard)
	register("ZRANK", 3, false, cmdZRank)
	register("ZRANGE", -4, false, cmdZRange)
}

func cmdZAdd(s *Server, w *resp.Writer, args [][]byte) bool {
	// ZADD key score member [score member ...]
	if (len(args)-2)%2 != 0 {
		w.WriteError("ERR wrong number of arguments for 'zadd' command")
		return false
	}
	key := string(args[1])
	pairs := (len(args) - 2) / 2

	// Validate all scores before applying any, matching Redis semantics.
	scores := make([]float64, pairs)
	for j, i := 0, 2; i < len(args); j, i = j+1, i+2 {
		f, ok := parseFloat(args[i])
		if !ok {
			w.WriteError("ERR value is not a valid float")
			return false
		}
		scores[j] = f
	}

	added := 0
	for j, i := 0, 2; i < len(args); j, i = j+1, i+2 {
		n, err := s.store.ZAdd(key, string(args[i+1]), scores[j])
		if err != nil {
			writeStoreErr(w, err)
			return added > 0
		}
		added += n
	}
	w.WriteInt(int64(added))
	return true
}

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
	w.WriteBulk([]byte(formatFloat(score)))
	return false
}

func cmdZRem(s *Server, w *resp.Writer, args [][]byte) bool {
	removed, err := s.store.ZRem(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if removed {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return removed
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

func cmdZRange(s *Server, w *resp.Writer, args [][]byte) bool {
	withScores := false
	switch {
	case len(args) == 4:
	case len(args) == 5 && strings.ToUpper(string(args[4])) == "WITHSCORES":
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
	members, err := s.store.ZRange(string(args[1]), start, stop)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if withScores {
		w.WriteArrayHeader(len(members) * 2)
		for _, m := range members {
			w.WriteBulk([]byte(m.Member))
			w.WriteBulk([]byte(formatFloat(m.Score)))
		}
	} else {
		w.WriteArrayHeader(len(members))
		for _, m := range members {
			w.WriteBulk([]byte(m.Member))
		}
	}
	return false
}
