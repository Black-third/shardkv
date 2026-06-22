package server

import (
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("DEL", -2, true, cmdDel)
	register("EXISTS", -2, false, cmdExists)
	register("EXPIRE", 3, true, cmdExpire)
	register("TTL", 2, false, cmdTTL)
	register("TYPE", 2, false, cmdType)
	register("KEYS", 2, false, cmdKeys)
}

func cmdDel(s *Server, w *resp.Writer, args [][]byte) bool {
	count := 0
	for _, k := range args[1:] {
		if s.store.Del(string(k)) {
			count++
		}
	}
	w.WriteInt(int64(count))
	return count > 0
}

func cmdExists(s *Server, w *resp.Writer, args [][]byte) bool {
	count := 0
	for _, k := range args[1:] {
		if s.store.Exists(string(k)) {
			count++
		}
	}
	w.WriteInt(int64(count))
	return false
}

func cmdExpire(s *Server, w *resp.Writer, args [][]byte) bool {
	n, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if s.store.Expire(string(args[1]), time.Duration(n)*time.Second) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdTTL(s *Server, w *resp.Writer, args [][]byte) bool {
	d, hasTTL, ok := s.store.TTL(string(args[1]))
	switch {
	case !ok:
		w.WriteInt(-2) // no such key
	case !hasTTL:
		w.WriteInt(-1) // exists, no expiry
	default:
		// Round to the nearest second, as Redis does, rather than truncating.
		w.WriteInt(int64((d + 500*time.Millisecond) / time.Second))
	}
	return false
}

func cmdType(s *Server, w *resp.Writer, args [][]byte) bool {
	typ, _ := s.store.Type(string(args[1]))
	w.WriteSimple(typ)
	return false
}

func cmdKeys(s *Server, w *resp.Writer, args [][]byte) bool {
	// Only KEYS * is supported; any pattern returns all keys.
	keys := s.store.Keys()
	w.WriteArrayHeader(len(keys))
	for _, k := range keys {
		w.WriteBulk([]byte(k))
	}
	return false
}
