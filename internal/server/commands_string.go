package server

import (
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("SET", -3, true, cmdSet)
	register("GET", 2, false, cmdGet)
	register("INCR", 2, true, cmdIncr)
	register("DECR", 2, true, cmdDecr)
	register("INCRBY", 3, true, cmdIncrBy)
	register("DECRBY", 3, true, cmdDecrBy)
	register("MSET", -3, true, cmdMSet)
	register("MGET", -2, false, cmdMGet)
	register("SETNX", 3, true, cmdSetNX)
	register("GETSET", 3, true, cmdGetSet)
	register("GETDEL", 2, true, cmdGetDel)
	register("APPEND", 3, true, cmdAppend)
	register("STRLEN", 2, false, cmdStrLen)
}

func cmdSet(s *Server, w *resp.Writer, args [][]byte) bool {
	switch len(args) {
	case 3:
		s.store.Set(string(args[1]), args[2], 0)
	case 5:
		n, ok := parseInt(args[4])
		if !ok {
			w.WriteError("ERR value is not an integer or out of range")
			return false
		}
		switch strings.ToUpper(string(args[3])) {
		case "EX", "PX":
			if n <= 0 { // Redis rejects a non-positive relative expire on SET
				w.WriteError("ERR invalid expire time in 'set' command")
				return false
			}
			unit := time.Second
			if strings.EqualFold(string(args[3]), "PX") {
				unit = time.Millisecond
			}
			s.store.Set(string(args[1]), args[2], time.Duration(n)*unit)
		case "PXAT": // absolute ms deadline (used by propagation/replay)
			s.store.SetDeadline(string(args[1]), args[2], time.UnixMilli(int64(n)))
		default:
			w.WriteError("ERR syntax error")
			return false
		}
	default:
		w.WriteError("ERR syntax error")
		return false
	}
	w.WriteSimple("OK")
	return true
}

func cmdIncrBy(s *Server, w *resp.Writer, args [][]byte) bool {
	delta, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	n, err := s.store.Incr(string(args[1]), int64(delta))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return true
}

func cmdDecrBy(s *Server, w *resp.Writer, args [][]byte) bool {
	delta, ok := parseInt(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	n, err := s.store.Incr(string(args[1]), -int64(delta))
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

func cmdGet(s *Server, w *resp.Writer, args [][]byte) bool {
	key := string(args[1])
	if typ, ok := s.store.Type(key); ok && typ != "string" {
		w.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return false
	}
	if v, ok := s.store.Get(key); ok {
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
