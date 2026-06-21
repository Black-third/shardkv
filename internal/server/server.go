// Package server exposes a store.Store over TCP using the RESP protocol, so
// standard Redis clients can talk to it. Each connection is served by its own
// goroutine; the shared store is safe for concurrent access.
package server

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// Server serves a store over a TCP listener.
type Server struct {
	store *store.Store
	ln    net.Listener
	wg    sync.WaitGroup
}

// New returns a Server backed by st.
func New(st *store.Store) *Server { return &Server{store: st} }

// Listen binds the server to addr without yet accepting connections, so the
// caller (and tests) can read Addr before serving.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	return nil
}

// Addr reports the bound address, or nil if Listen has not been called.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve accepts connections until ctx is canceled, then waits for in-flight
// connections to finish before returning.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.ln.Close() // unblocks Accept
	}()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
				return err
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(ctx, conn)
		}()
	}
}

// ListenAndServe is the convenience combination of Listen and Serve.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	if err := s.Listen(addr); err != nil {
		return err
	}
	return s.Serve(ctx)
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	// Close the connection on shutdown so a blocked Read returns.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return // EOF or protocol error: drop the connection
		}
		if len(args) == 0 {
			continue
		}
		s.dispatch(w, args)
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(w *resp.Writer, args [][]byte) {
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "PING":
		if len(args) > 1 {
			w.WriteBulk(args[1])
		} else {
			w.WriteSimple("PONG")
		}

	case "SET":
		// SET key value [EX seconds | PX milliseconds]
		if len(args) != 3 && len(args) != 5 {
			w.WriteError("ERR wrong number of arguments for 'set' command")
			return
		}
		var ttl time.Duration
		if len(args) == 5 {
			n, err := strconv.Atoi(string(args[4]))
			if err != nil || n < 0 {
				w.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			switch strings.ToUpper(string(args[3])) {
			case "EX":
				ttl = time.Duration(n) * time.Second
			case "PX":
				ttl = time.Duration(n) * time.Millisecond
			default:
				w.WriteError("ERR syntax error")
				return
			}
		}
		s.store.Set(string(args[1]), args[2], ttl)
		w.WriteSimple("OK")

	case "GET":
		if len(args) != 2 {
			w.WriteError("ERR wrong number of arguments for 'get' command")
			return
		}
		if v, ok := s.store.Get(string(args[1])); ok {
			w.WriteBulk(v)
		} else {
			w.WriteNull()
		}

	case "DEL":
		if len(args) < 2 {
			w.WriteError("ERR wrong number of arguments for 'del' command")
			return
		}
		count := 0
		for _, k := range args[1:] {
			if s.store.Del(string(k)) {
				count++
			}
		}
		w.WriteInt(int64(count))

	case "EXISTS":
		if len(args) < 2 {
			w.WriteError("ERR wrong number of arguments for 'exists' command")
			return
		}
		count := 0
		for _, k := range args[1:] {
			if s.store.Exists(string(k)) {
				count++
			}
		}
		w.WriteInt(int64(count))

	case "INCR", "DECR":
		if len(args) != 2 {
			w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
			return
		}
		delta := int64(1)
		if cmd == "DECR" {
			delta = -1
		}
		n, err := s.store.Incr(string(args[1]), delta)
		if err != nil {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		w.WriteInt(n)

	case "EXPIRE":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'expire' command")
			return
		}
		n, err := strconv.Atoi(string(args[2]))
		if err != nil {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		if s.store.Expire(string(args[1]), time.Duration(n)*time.Second) {
			w.WriteInt(1)
		} else {
			w.WriteInt(0)
		}

	case "TTL":
		if len(args) != 2 {
			w.WriteError("ERR wrong number of arguments for 'ttl' command")
			return
		}
		d, hasTTL, ok := s.store.TTL(string(args[1]))
		switch {
		case !ok:
			w.WriteInt(-2) // no such key
		case !hasTTL:
			w.WriteInt(-1) // exists, no expiry
		default:
			w.WriteInt(int64(d / time.Second))
		}

	case "DBSIZE":
		w.WriteInt(int64(s.store.Len()))

	case "KEYS":
		// Only KEYS * is supported; any pattern returns all keys.
		keys := s.store.Keys()
		w.WriteArrayHeader(len(keys))
		for _, k := range keys {
			w.WriteBulk([]byte(k))
		}

	case "FLUSHALL":
		s.store.FlushAll()
		w.WriteSimple("OK")

	case "COMMAND":
		// redis-cli issues COMMAND DOCS on connect; an empty array is enough.
		w.WriteArrayHeader(0)

	default:
		w.WriteError("ERR unknown command '" + string(args[0]) + "'")
	}
}
