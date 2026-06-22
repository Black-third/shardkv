package server

import (
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("DEL", -2, true, cmdDel)
	register("EXISTS", -2, false, cmdExists)
	register("EXPIRE", 3, true, cmdExpire)
	register("PEXPIRE", 3, true, cmdPExpire)
	register("EXPIREAT", 3, true, cmdExpireAt)
	register("PEXPIREAT", 3, true, cmdPExpireAt)
	register("PERSIST", 2, true, cmdPersist)
	register("TTL", 2, false, cmdTTL)
	register("PTTL", 2, false, cmdPTTL)
	register("TYPE", 2, false, cmdType)
	register("KEYS", 2, false, cmdKeys)
	register("RENAME", 3, true, cmdRename)
}

func cmdPExpire(s *Server, w *resp.Writer, args [][]byte) bool {
	n, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if s.store.ExpireAt(string(args[1]), time.Now().Add(time.Duration(n)*time.Millisecond)) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdExpireAt(s *Server, w *resp.Writer, args [][]byte) bool {
	n, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if s.store.ExpireAt(string(args[1]), time.Unix(int64(n), 0)) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdPExpireAt(s *Server, w *resp.Writer, args [][]byte) bool {
	n, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if s.store.ExpireAt(string(args[1]), time.UnixMilli(int64(n))) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdPersist(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.store.Persist(string(args[1])) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdPTTL(s *Server, w *resp.Writer, args [][]byte) bool {
	d, hasTTL, ok := s.store.TTL(string(args[1]))
	switch {
	case !ok:
		w.WriteInt(-2)
	case !hasTTL:
		w.WriteInt(-1)
	default:
		w.WriteInt(int64(d / time.Millisecond))
	}
	return false
}

func cmdRename(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.store.Rename(string(args[1]), string(args[2])) {
		w.WriteSimple("OK")
		return true
	}
	w.WriteError("ERR no such key")
	return false
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
