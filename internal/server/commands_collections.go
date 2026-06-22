package server

import "github.com/Black-third/shardkv/internal/resp"

func init() {
	// Lists
	register("LPUSH", -3, true, cmdLPush)
	register("RPUSH", -3, true, cmdRPush)
	register("LPOP", 2, true, cmdLPop)
	register("RPOP", 2, true, cmdRPop)
	register("LRANGE", 4, false, cmdLRange)
	register("LLEN", 2, false, cmdLLen)
	// Hashes
	register("HSET", -4, true, cmdHSet)
	register("HGET", 3, false, cmdHGet)
	register("HDEL", -3, true, cmdHDel)
	register("HGETALL", 2, false, cmdHGetAll)
	register("HLEN", 2, false, cmdHLen)
	// Sets
	register("SADD", -3, true, cmdSAdd)
	register("SREM", -3, true, cmdSRem)
	register("SMEMBERS", 2, false, cmdSMembers)
	register("SISMEMBER", 3, false, cmdSIsMember)
	register("SCARD", 2, false, cmdSCard)
}

// --- lists ---

func cmdLPush(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.LPush(string(args[1]), args[2:]...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return true
}

func cmdRPush(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.RPush(string(args[1]), args[2:]...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return true
}

func cmdLPop(s *Server, w *resp.Writer, args [][]byte) bool {
	v, ok, err := s.store.LPop(string(args[1]))
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

func cmdRPop(s *Server, w *resp.Writer, args [][]byte) bool {
	v, ok, err := s.store.RPop(string(args[1]))
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

func cmdLRange(s *Server, w *resp.Writer, args [][]byte) bool {
	start, ok1 := parseInt(args[2])
	stop, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	items, err := s.store.LRange(string(args[1]), start, stop)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(items))
	for _, v := range items {
		w.WriteBulk(v)
	}
	return false
}

func cmdLLen(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.LLen(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

// --- hashes ---

func cmdHSet(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args)%2 != 0 {
		w.WriteError("ERR wrong number of arguments for 'hset' command")
		return false
	}
	pairs := make([][2][]byte, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		pairs = append(pairs, [2][]byte{args[i], args[i+1]})
	}
	created, err := s.store.HSet(string(args[1]), pairs...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(created))
	return true
}

func cmdHGet(s *Server, w *resp.Writer, args [][]byte) bool {
	v, ok, err := s.store.HGet(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteBulk(v)
	return false
}

func cmdHDel(s *Server, w *resp.Writer, args [][]byte) bool {
	fields := make([]string, 0, len(args)-2)
	for _, f := range args[2:] {
		fields = append(fields, string(f))
	}
	n, err := s.store.HDel(string(args[1]), fields...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return n > 0
}

func cmdHGetAll(s *Server, w *resp.Writer, args [][]byte) bool {
	flat, err := s.store.HGetAll(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(flat))
	for _, v := range flat {
		w.WriteBulk(v)
	}
	return false
}

func cmdHLen(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.HLen(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}

// --- sets ---

func cmdSAdd(s *Server, w *resp.Writer, args [][]byte) bool {
	members := make([]string, 0, len(args)-2)
	for _, m := range args[2:] {
		members = append(members, string(m))
	}
	added, err := s.store.SAdd(string(args[1]), members...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(added))
	return added > 0
}

func cmdSRem(s *Server, w *resp.Writer, args [][]byte) bool {
	members := make([]string, 0, len(args)-2)
	for _, m := range args[2:] {
		members = append(members, string(m))
	}
	removed, err := s.store.SRem(string(args[1]), members...)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(removed))
	return removed > 0
}

func cmdSMembers(s *Server, w *resp.Writer, args [][]byte) bool {
	members, err := s.store.SMembers(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(members))
	for _, m := range members {
		w.WriteBulk([]byte(m))
	}
	return false
}

func cmdSIsMember(s *Server, w *resp.Writer, args [][]byte) bool {
	ok, err := s.store.SIsMember(string(args[1]), string(args[2]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if ok {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return false
}

func cmdSCard(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.SCard(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	return false
}
