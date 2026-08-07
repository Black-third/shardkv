package shardkv

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/server"
)

// Client runs commands against an embedded DB with no socket in between.
//
// It is a client in the server's own sense: it has a session, it appears in CLIENT LIST,
// it has its own SELECTed database, its own MULTI, its own AUTH, its own WATCHes. What it
// does not have is a connection, so a command costs a buffer write and a parse instead of
// a round trip.
//
// # Which semantics it gets
//
// The server has two command paths and this one takes the client path (executeCommand),
// not the replay path (dispatch). That is a decision rather than an accident. dispatch is
// what an AOF replay and a master's stream go through and it is deliberately never
// redirected -- invariant 13 -- because a replica must apply every write its master sends
// whatever its own slot map says. Routing an embedded caller through it would have made
// this package a hole through every gate the client path exists to enforce. So a Client
// is authenticated if a password is set, is refused a write on a replica with READONLY, is
// refused a write over the memory budget with OOM, is redirected with MOVED or ASK for a
// slot this node does not own in cluster mode, and queues rather than runs inside MULTI --
// all identically to a client on a socket, because it is the same code.
//
// # Concurrency
//
// One Client is safe to use from several goroutines but serializes them, exactly as one
// connection does, and for the same reason: a session's transaction state, database and
// authentication belong to whoever is using it, and interleaving two callers' commands
// through one would let one goroutine's EXEC run another's queue. Concurrency comes from
// having more clients -- DB.NewClient -- which is what a connection pool is.
type Client struct {
	// mu serializes commands on this client. It is held across the whole of a command,
	// including the decode, because the session's reply buffer is reused between calls.
	mu   sync.Mutex
	sess *server.EmbeddedSession
	rd   bytes.Reader
	br   *bufio.Reader
	rr   *resp.Reader

	closeOnce sync.Once
}

// NewClient returns an independent client: its own session, its own database, its own
// transaction. Use one per goroutine that issues commands concurrently.
//
// The caller must Close it, which releases its WATCHes and its entry in the client
// registry. A client that is never closed leaks that entry for the lifetime of the DB,
// which is what an unclosed connection would also do. The DB's own default client is
// closed by DB.Close.
func (db *DB) NewClient() *Client {
	c := &Client{sess: db.srv.NewEmbeddedSession(db.done)}
	// One reader over a bytes.Reader that is re-pointed at each reply, rather than a fresh
	// pair per command: a bufio.Reader is a 4 KB buffer, and allocating one per command
	// would cost more than the command. Reset is what makes them reusable.
	c.br = bufio.NewReader(&c.rd)
	c.rr = resp.NewReader(c.br)
	return c
}

// Close releases the client. Safe to call more than once, so a defer and an explicit
// close can both be present.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.sess.Close()
	})
	return nil
}

// Do runs one command and returns its reply as Go values.
//
// The arguments are the command and its operands, unparsed and unquoted -- Do("SET", "k",
// "a b") sets one key to the three-character value, with no quoting rules in the way. A Go
// string may hold arbitrary bytes, so a binary value needs no special form.
//
// The reply mapping:
//
//	simple string, bulk string, verbatim string  ->  string
//	integer                                      ->  int64
//	double                                       ->  float64  (RESP3 only; a string in RESP2)
//	boolean                                      ->  bool     (RESP3 only; an int64 in RESP2)
//	null, and a missing key's bulk reply          ->  nil
//	array, set, push                             ->  []any
//	map                                          ->  map[string]any  (RESP3 only; see Map)
//
// An error reply is returned as err, and the value is nil. An error *inside* an array is
// a value implementing error, which is the only way to report an EXEC: a transaction runs
// every queued command whether or not an earlier one failed, so its reply is an array in
// which any element may have failed and the ones that succeeded still have results.
//
// The reply shapes are RESP2's, because a session that has sent no HELLO is a RESP2
// connection and this one is no different. Send HELLO 3 to change that -- it is an
// ordinary command -- and HGETALL starts arriving as a map and ZSCORE as a float64. The
// accessors below read both, so most callers never need to choose.
func (c *Client) Do(args ...string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.do(args)
}

// do is Do without the lock, for the accessors, which hold it across the conversion.
func (c *Client) do(args []string) (any, error) {
	if len(args) == 0 {
		return nil, errors.New("shardkv: no command")
	}
	raw := c.sess.Do(toArgs(args))
	if len(raw) == 0 {
		// CLIENT REPLY OFF|SKIP withheld the reply, exactly as it would on a socket. There
		// is nothing to decode and nothing went wrong.
		return nil, nil
	}
	c.rd.Reset(raw)
	c.br.Reset(&c.rd)
	v, err := c.rr.ReadReply()
	if err != nil {
		// Unreachable from this server's own replies: the bytes were produced by the writer
		// this decoder inverts, and they are all in memory, so neither a short read nor an
		// unknown type can occur. It is reported rather than dropped because the day it
		// happens it is a bug in one of the two halves, and a silent nil would send the
		// caller looking anywhere else.
		return nil, fmt.Errorf("shardkv: decoding the reply to %s: %w", args[0], err)
	}
	if e, ok := v.(error); ok {
		return nil, e
	}
	return v, nil
}

// toArgs copies the arguments into the [][]byte the command table takes.
//
// The allocation is per call and is deliberately not pooled, which is the one
// optimization to refuse here. A write's argument slice does not stop being used when the
// command returns: it is handed to the AOF and, for each connected replica, put on that
// replica's feed channel and read by another goroutine afterwards (see shipReplicas).
// Reusing these buffers would therefore not corrupt the caller's next command -- it would
// corrupt the replication stream and the log, silently, and only on a server that had a
// replica or persistence attached. That is the failure shape every invariant in CLAUDE.md
// is about, bought for four allocations.
func toArgs(args []string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}

// The typed accessors.
//
// There is deliberately no method per command. What a caller actually repeats is not the
// command -- it is the type assertion on the reply, and the 224 commands answer in far
// fewer distinct shapes than there are commands. So the conversion is named once per
// shape and serves every command that has it, including ones that do not exist yet, which
// a hand-written mirror could not. Each also absorbs the RESP2/RESP3 difference where
// there is one, so a caller's code does not change when it sends HELLO 3.

// OK runs a command whose reply is a status and discards it, reporting only failure. It
// is for SET, SELECT, FLUSHDB, CONFIG SET and the rest of the commands whose answer
// carries no information beyond "it worked".
func (c *Client) OK(args ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.do(args)
	return err
}

// Int runs a command whose reply is an integer: INCR, DEL, EXISTS, LLEN, SADD, ZADD,
// PFCOUNT, TTL.
//
// A RESP3 boolean counts as one (true is 1), because a handful of commands answer with a
// boolean under RESP3 and an integer under RESP2 -- SISMEMBER, EXPIRE, SETNX -- and a
// caller asking for the count should not have to know which protocol it negotiated.
func (c *Client) Int(args ...string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.do(args)
	if err != nil {
		return 0, err
	}
	switch n := v.(type) {
	case int64:
		return n, nil
	case bool:
		if n {
			return 1, nil
		}
		return 0, nil
	case string:
		// A number the server sent as text, which is what RESP2 does for a double and what
		// several commands do for a count that may exceed an int64 reply's range.
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i, nil
		}
	}
	return 0, typeErr(args, "an integer", v)
}

// Float runs a command whose reply is a number that may not be whole: ZSCORE, ZINCRBY,
// INCRBYFLOAT, GEODIST.
//
// It reads both protocols' spellings -- RESP3's double and RESP2's bulk string of the
// same digits -- and both of the infinities, which a sorted set really can hold and which
// are spelled "inf" and "-inf" rather than in Go's syntax. A missing key's nil is
// reported as an error rather than as 0, because 0 is a score a member can have.
func (c *Client) Float(args ...string) (float64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.do(args)
	if err != nil {
		return 0, err
	}
	switch f := v.(type) {
	case float64:
		return f, nil
	case int64:
		return float64(f), nil
	case string:
		if d, ok := resp.ParseDouble(f); ok {
			return d, nil
		}
	}
	return 0, typeErr(args, "a number", v)
}

// Bool runs a command whose reply is a yes or no: SISMEMBER, EXPIRE, SETNX, HEXISTS,
// RENAMENX, MOVE. It reads RESP2's 1/0 and RESP3's boolean alike.
func (c *Client) Bool(args ...string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.do(args)
	if err != nil {
		return false, err
	}
	switch b := v.(type) {
	case bool:
		return b, nil
	case int64:
		return b != 0, nil
	case nil:
		// A null is how several commands spell "no": GET on a missing key, LPOP on an empty
		// list. Reporting it as false is the answer the caller asked for.
		return false, nil
	}
	return false, typeErr(args, "a boolean", v)
}

// Bytes runs a command whose reply is a single value that may be absent, and reports
// which: GET, HGET, LINDEX, GETRANGE, SPOP, LPOP, DUMP.
//
// ok is false for a missing key, and that is the whole reason this returns three values.
// An absent key and a key holding the empty string are different states that Redis
// answers differently and that the string "" cannot tell apart -- which is the single
// most common mistake made against a reply typed as any.
func (c *Client) Bytes(args ...string) (value []byte, ok bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.do(args)
	if err != nil {
		return nil, false, err
	}
	switch s := v.(type) {
	case nil:
		return nil, false, nil
	case string:
		return []byte(s), true, nil
	case int64:
		return []byte(strconv.FormatInt(s, 10)), true, nil
	}
	return nil, false, typeErr(args, "a value", v)
}

// Strings runs a command whose reply is a collection of values: LRANGE, SMEMBERS, KEYS,
// HVALS, ZRANGE, MGET, SRANDMEMBER.
//
// A RESP3 set is read as the array RESP2 sends for the same command, so the result does
// not depend on the protocol. A null element -- which MGET produces for a key that is not
// there, and only MGET and its relatives do -- becomes "", so a caller that needs to tell
// a missing key from an empty value uses Do for those.
func (c *Client) Strings(args ...string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.do(args)
	if err != nil {
		return nil, err
	}
	switch items := v.(type) {
	case nil:
		// The null array, which is how RESP2 spells a collection that is not there at all --
		// a timed-out BLPOP, an aborted EXEC. An empty slice is the honest reading: there
		// are no elements.
		return nil, nil
	case []any:
		out := make([]string, len(items))
		for i, it := range items {
			s, ok := asString(it)
			if !ok {
				return nil, typeErr(args, "a collection of values", v)
			}
			out[i] = s
		}
		return out, nil
	}
	return nil, typeErr(args, "a collection of values", v)
}

// Map runs a command whose reply is a set of name/value pairs: HGETALL, CONFIG GET, XINFO
// STREAM.
//
// It reads both shapes, and that is what it is for: the same HGETALL is a flat array of
// 2n elements under RESP2 and a map of n pairs under RESP3, which is one of the few
// places RESP3 changed a reply's shape rather than only its tag. Pairing the flat form up
// here means a caller's code is the same either way.
func (c *Client) Map(args ...string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.do(args)
	if err != nil {
		return nil, err
	}
	switch reply := v.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		out := make(map[string]string, len(reply))
		for k, raw := range reply {
			s, ok := asString(raw)
			if !ok {
				return nil, typeErr(args, "name/value pairs", v)
			}
			out[k] = s
		}
		return out, nil
	case []any:
		if len(reply)%2 != 0 {
			// An odd count cannot be pairs. Reported rather than truncated: dropping the tail
			// would answer a question the caller did not ask.
			return nil, typeErr(args, "name/value pairs", v)
		}
		out := make(map[string]string, len(reply)/2)
		for i := 0; i < len(reply); i += 2 {
			k, kok := asString(reply[i])
			val, vok := asString(reply[i+1])
			if !kok || !vok {
				return nil, typeErr(args, "name/value pairs", v)
			}
			out[k] = val
		}
		return out, nil
	}
	return nil, typeErr(args, "name/value pairs", v)
}

// asString renders a scalar reply element as text. It accepts the numeric and boolean
// forms because a collection is not always homogeneous -- XPENDING mixes ids with
// counts, and CONFIG GET's values are text for settings that are numbers -- so refusing
// anything but a string would fail on replies that are perfectly well formed. A nested
// collection is refused, since flattening it would invent a value.
func asString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case nil:
		return "", true
	case int64:
		return strconv.FormatInt(s, 10), true
	case float64:
		return resp.FormatDouble(s), true
	case bool:
		return strconv.FormatBool(s), true
	}
	return "", false
}

// typeErr reports a reply that was well formed but not the shape the accessor was asked
// for, naming the command and what actually arrived. Both halves are needed to act on it:
// the command says which call site is wrong, and the Go type says whether the mistake was
// the accessor or the protocol version.
func typeErr(args []string, want string, got any) error {
	return fmt.Errorf("shardkv: %s did not reply with %s but with %T", args[0], want, got)
}
