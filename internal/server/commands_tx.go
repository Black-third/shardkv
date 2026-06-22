package server

import (
	"strings"
	"sync/atomic"

	"github.com/Black-third/shardkv/internal/resp"
)

// session holds the per-connection transaction state. All fields except dirty
// are touched only by the owning connection's goroutine; dirty is set by other
// goroutines when a watched key changes, so it is atomic.
type session struct {
	inMulti  bool
	queued   [][][]byte
	queueErr bool // a queued command failed to parse -> EXEC aborts with EXECABORT
	watched  map[string]struct{}
	dirty    atomic.Bool
}

// execute is the per-connection entry point. It handles transaction control
// commands itself, queues commands while in MULTI, and otherwise dispatches
// normally.
func (s *Server) execute(sess *session, w *resp.Writer, args [][]byte) {
	name := strings.ToUpper(string(args[0]))

	switch name {
	case "MULTI":
		if sess.inMulti {
			w.WriteError("ERR MULTI calls can not be nested")
			return
		}
		sess.inMulti = true
		sess.queued = nil
		sess.queueErr = false
		w.WriteSimple("OK")
		return

	case "EXEC":
		s.execExec(sess, w)
		return

	case "DISCARD":
		if !sess.inMulti {
			w.WriteError("ERR DISCARD without MULTI")
			return
		}
		sess.inMulti = false
		sess.queued = nil
		sess.queueErr = false
		s.unwatchAll(sess)
		w.WriteSimple("OK")
		return

	case "WATCH":
		if len(args) < 2 {
			w.WriteError("ERR wrong number of arguments for 'watch' command")
			return
		}
		if sess.inMulti {
			w.WriteError("ERR WATCH inside MULTI is not allowed")
			return
		}
		for _, k := range args[1:] {
			s.watchKey(sess, string(k))
		}
		w.WriteSimple("OK")
		return

	case "UNWATCH":
		s.unwatchAll(sess)
		w.WriteSimple("OK")
		return
	}

	if sess.inMulti {
		// Validate now so EXEC can abort cleanly; queue for later execution.
		cmd, ok := commandTable[name]
		if !ok {
			sess.queueErr = true
			w.WriteError("ERR unknown command '" + string(args[0]) + "'")
			return
		}
		if !arityOK(cmd.arity, len(args)) {
			sess.queueErr = true
			w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
			return
		}
		sess.queued = append(sess.queued, args)
		w.WriteSimple("QUEUED")
		return
	}

	s.dispatch(w, args)
}

func (s *Server) execExec(sess *session, w *resp.Writer) {
	if !sess.inMulti {
		w.WriteError("ERR EXEC without MULTI")
		return
	}
	queued := sess.queued
	queueErr := sess.queueErr
	aborted := sess.dirty.Load()

	// Reset transaction + watch state before running, so the batch's own writes
	// don't mark this session dirty.
	sess.inMulti = false
	sess.queued = nil
	sess.queueErr = false
	s.unwatchAll(sess)

	if queueErr {
		w.WriteError("EXECABORT Transaction discarded because of previous errors.")
		return
	}
	if aborted {
		w.WriteNullArray() // a watched key changed
		return
	}

	w.WriteArrayHeader(len(queued))
	for _, cmd := range queued {
		s.dispatch(w, cmd)
	}
}

// --- WATCH registry ----------------------------------------------------------

func (s *Server) watchKey(sess *session, key string) {
	if sess.watched == nil {
		sess.watched = make(map[string]struct{})
	}
	if _, ok := sess.watched[key]; ok {
		return
	}
	sess.watched[key] = struct{}{}

	s.watchMu.Lock()
	if s.watchers[key] == nil {
		s.watchers[key] = make(map[*session]struct{})
	}
	s.watchers[key][sess] = struct{}{}
	s.watchMu.Unlock()
}

// unwatchAll removes all of the session's watches and clears its dirty flag.
func (s *Server) unwatchAll(sess *session) {
	if len(sess.watched) > 0 {
		s.watchMu.Lock()
		for k := range sess.watched {
			if m := s.watchers[k]; m != nil {
				delete(m, sess)
				if len(m) == 0 {
					delete(s.watchers, k)
				}
			}
		}
		s.watchMu.Unlock()
		sess.watched = nil
	}
	sess.dirty.Store(false)
}

// touchWatchers marks every session watching an affected key as dirty, so a
// pending EXEC will abort. FLUSHALL invalidates all watchers.
func (s *Server) touchWatchers(args [][]byte) {
	name := strings.ToUpper(string(args[0]))

	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if len(s.watchers) == 0 {
		return
	}
	if name == "FLUSHALL" {
		for _, sessions := range s.watchers {
			for sess := range sessions {
				sess.dirty.Store(true)
			}
		}
		return
	}
	for _, k := range affectedKeys(name, args) {
		for sess := range s.watchers[k] {
			sess.dirty.Store(true)
		}
	}
}

// affectedKeys returns the keys a write command modifies, for WATCH invalidation.
func affectedKeys(name string, args [][]byte) []string {
	switch name {
	case "MSET":
		var ks []string
		for i := 1; i+1 < len(args); i += 2 {
			ks = append(ks, string(args[i]))
		}
		return ks
	case "DEL":
		ks := make([]string, 0, len(args)-1)
		for _, k := range args[1:] {
			ks = append(ks, string(k))
		}
		return ks
	case "RENAME":
		if len(args) == 3 {
			return []string{string(args[1]), string(args[2])}
		}
		return nil
	default:
		if len(args) >= 2 {
			return []string{string(args[1])}
		}
		return nil
	}
}
