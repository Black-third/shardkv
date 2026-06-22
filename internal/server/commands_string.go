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
	register("MSET", -3, true, cmdMSet)
	register("MGET", -2, false, cmdMGet)
}

func cmdSet(s *Server, w *resp.Writer, args [][]byte) bool {
	var ttl time.Duration
	switch len(args) {
	case 3:
	case 5:
		n, ok := parseInt(args[4])
		if !ok || n <= 0 { // Redis rejects a non-positive expire on SET
			w.WriteError("ERR invalid expire time in 'set' command")
			return false
		}
		switch strings.ToUpper(string(args[3])) {
		case "EX":
			ttl = time.Duration(n) * time.Second
		case "PX":
			ttl = time.Duration(n) * time.Millisecond
		default:
			w.WriteError("ERR syntax error")
			return false
		}
	default:
		w.WriteError("ERR syntax error")
		return false
	}
	s.store.Set(string(args[1]), args[2], ttl)
	w.WriteSimple("OK")
	return true
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
