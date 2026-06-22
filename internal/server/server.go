// Package server exposes a store.Store over TCP using the RESP protocol, so
// standard Redis clients can talk to it. Each connection is served by its own
// goroutine; the shared store is safe for concurrent access.
//
// Commands are looked up in a self-registering table (see register). Each entry
// declares an arity and a write flag; the write flag is what drives append-only
// persistence and replica propagation, so adding a new mutating command wires it
// into durability and replication automatically.
package server

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// handlerFunc handles one command. It writes the reply to w and returns whether
// it actually modified the dataset (dirty); only dirty writes are persisted and
// replicated.
type handlerFunc func(s *Server, w *resp.Writer, args [][]byte) (dirty bool)

type command struct {
	arity int // >0: exact arg count (incl. name); <0: at least -arity
	write bool
	fn    handlerFunc
}

var commandTable = map[string]*command{}

// register adds a command to the table. Called from init() in the commands_*.go
// files so each group of commands wires itself in.
func register(name string, arity int, write bool, fn handlerFunc) {
	commandTable[name] = &command{arity: arity, write: write, fn: fn}
}

func arityOK(arity, n int) bool {
	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
}

// replicaConn is the master's view of a connected replica: a buffered channel of
// commands drained by the replica's serving goroutine.
type replicaConn struct {
	ch chan [][]byte
}

// Server serves a store over a TCP listener.
type Server struct {
	store *store.Store
	ln    net.Listener
	wg    sync.WaitGroup

	aof *aof.Log // nil if persistence is disabled

	// propagating becomes true once durability or replication is active (AOF
	// attached or a replica connected). While it is true, write commands are
	// serialized through propMu across the store mutation AND the propagation, so
	// the order applied to memory is the exact order written to the AOF and the
	// replica stream. A point-in-time snapshot is taken under propMu, so an
	// initial replica sync sees a consistent cut with no double-apply. When it is
	// false (pure single-node cache), writes stay sharded-concurrent.
	propagating atomic.Bool
	propMu      sync.Mutex

	mu         sync.Mutex
	role       string // "master" or "replica"
	masterAddr string
	replicas   map[*replicaConn]struct{}
	replCancel context.CancelFunc // cancels the active replication loop, if any

	watchMu  sync.Mutex
	watchers map[string]map[*session]struct{} // key -> sessions WATCHing it

	baseCtx    context.Context // lifetime ctx for replication started via REPLICAOF
	startTime  time.Time
	totalConns atomic.Int64
	totalCmds  atomic.Int64
}

// New returns a master Server backed by st. It must be called before the
// store's janitor goroutine starts, since it registers the store removal hook.
func New(st *store.Store) *Server {
	s := &Server{
		store:     st,
		role:      "master",
		replicas:  make(map[*replicaConn]struct{}),
		watchers:  make(map[string]map[*session]struct{}),
		baseCtx:   context.Background(),
		startTime: time.Now(),
	}
	st.SetRemovalHook(s.onKeyRemoved)
	return s
}

// onKeyRemoved propagates a synthetic DEL when the store expires or evicts a key
// on its own, so the AOF and replicas converge, and invalidates any WATCH on
// that key. Invoked by the store with no shard lock held.
func (s *Server) onKeyRemoved(key string) {
	args := [][]byte{[]byte("DEL"), []byte(key)}
	s.touchWatchers(args)
	if s.propagating.Load() {
		s.propMu.Lock()
		s.propagate(args)
		s.propMu.Unlock()
	}
}

// AttachAOF wires an append-only log so write commands are persisted. Enabling
// persistence also turns on write serialization (propagating).
func (s *Server) AttachAOF(log *aof.Log) {
	s.aof = log
	s.propagating.Store(true)
}

// ReplayCommands applies a recorded command stream (e.g. from the AOF) to the
// store without persisting or propagating it. Used once at startup.
func (s *Server) ReplayCommands(cmds [][][]byte) {
	dw := resp.NewWriter(io.Discard)
	for _, cmd := range cmds {
		s.applyCommand(dw, cmd)
	}
}

func (s *Server) isReplica() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.role == "replica"
}

// Listen binds the server to addr without yet accepting connections.
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
// connections to finish.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	s.baseCtx = ctx // so runtime REPLICAOF ties replication to the serve lifetime
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.ln.Close()
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
		s.totalConns.Add(1)
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
	sess := &session{}
	defer s.unwatchAll(sess) // release any WATCHes on disconnect
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		// PSYNC turns this connection into a replication feed and never returns
		// while the replica stays connected.
		if strings.ToUpper(string(args[0])) == "PSYNC" {
			s.handleSync(ctx, w)
			return
		}
		s.execute(sess, w, args)
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(w *resp.Writer, args [][]byte) {
	s.totalCmds.Add(1)
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commandTable[name]
	if !ok {
		w.WriteError("ERR unknown command '" + string(args[0]) + "'")
		return
	}
	if !arityOK(cmd.arity, len(args)) {
		w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
		return
	}
	if cmd.write && s.isReplica() {
		w.WriteError("READONLY You can't write against a read only replica.")
		return
	}
	if !cmd.write {
		cmd.fn(s, w, args)
		return
	}
	// Write command. When propagation is active, serialize the mutation and its
	// propagation under propMu so memory, AOF, and replica order all agree.
	if s.propagating.Load() {
		s.propMu.Lock()
		dirty := cmd.fn(s, w, args)
		if dirty {
			s.touchWatchers(args)
			s.propagate(args)
		}
		s.propMu.Unlock()
		return
	}
	dirty := cmd.fn(s, w, args)
	if dirty {
		s.touchWatchers(args) // WATCH still works without propagation
	}
}

// applyCommand runs a command directly (no arity reply, no propagation). Used to
// replay the AOF and to apply the stream received from a master.
func (s *Server) applyCommand(w *resp.Writer, args [][]byte) {
	if len(args) == 0 {
		return
	}
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commandTable[name]
	if !ok || !arityOK(cmd.arity, len(args)) {
		return
	}
	cmd.fn(s, w, args)
	w.Flush()
}

// propagate persists a write to the AOF and streams it to all connected
// replicas. Slow replicas whose buffers are full are skipped (they will fall
// behind and can resync).
func (s *Server) propagate(args [][]byte) {
	// Rewrite relative TTLs to absolute deadlines so the AOF and replicas
	// reconstruct the same expiry instant regardless of when they apply it.
	args = propagationForm(args, time.Now())
	if s.aof != nil {
		if err := s.aof.Append(args); err != nil {
			// The write was already acked to the client; surface the durability
			// failure rather than swallowing it.
			log.Printf("shardkv: AOF append failed (write not persisted): %v", err)
		}
	}
	s.mu.Lock()
	for rc := range s.replicas {
		select {
		case rc.ch <- args:
		default:
		}
	}
	s.mu.Unlock()
}
