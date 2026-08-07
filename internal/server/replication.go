package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

const (
	// replBackoff is the delay between replica reconnection attempts.
	replBackoff = time.Second
	// replFeedBuffer is how many commands a replica may fall behind by before the
	// master drops its feed and makes it resync.
	replFeedBuffer = 1024
	// replPingInterval is how often a master sends a no-op PING down each replica
	// feed. Without it a replica cannot distinguish an idle master from one that
	// vanished behind a network partition, which leaves the TCP connection open but
	// permanently silent.
	replPingInterval = time.Second
	// replReadTimeout is how long a replica waits for any byte from its master
	// before assuming the link is dead and reconnecting. It is an order of
	// magnitude larger than replPingInterval so a scheduling hiccup or a slow
	// snapshot chunk cannot trip it.
	replReadTimeout = 10 * time.Second
	// replAckInterval is how often a replica volunteers its processed offset. The
	// report rides the wakeups the master's keepalive already causes, so it costs no
	// timer and no extra goroutine on the replica.
	replAckInterval = 500 * time.Millisecond
)

func init() {
	registerSession("REPLCONF", -1, cmdReplConf)
	register("WAIT", 3, false, cmdWait)
	register("ROLE", 1, false, cmdRole)
}

// cmdRole reports this server's replication role in the shape a client library parses,
// which is why it exists separately from INFO replication: INFO is a text blob a client
// has to scrape, while ROLE is a typed reply, and go-redis (among others) calls it to
// decide whether the connection it has is writable.
//
// The two shapes are Redis's:
//
//	master:  ["master", <offset>, [[ip, port, offset], ...]]
//	replica: ["slave", <master ip>, <master port>, <link state>, <offset>]
//
// The replica's port and each replica's offset are bulk strings rather than integers, and
// the role word is "slave" rather than "replica" -- both are Redis's encoding, and a
// client matching on either would break if this normalized them. (INFO says "role:master"
// or "role:replica"; ROLE says "master" or "slave". That inconsistency is Redis's too.)
func cmdRole(s *Server, w *resp.Writer, args [][]byte) bool {
	s.mu.Lock()
	role, master := s.role, s.masterAddr
	offset, slaveOffset, linkUp := s.replOffset, s.slaveOffset, s.masterLinkUp
	replicas := make([]*replicaConn, 0, len(s.replicas))
	for rc := range s.replicas {
		replicas = append(replicas, rc)
	}
	s.mu.Unlock()

	if role == "replica" {
		host, port := master, "0"
		if h, p, err := net.SplitHostPort(master); err == nil {
			host, port = h, p
		}
		w.WriteArrayHeader(5)
		w.WriteBulkString("slave")
		w.WriteBulkString(host)
		p, _ := strconv.Atoi(port)
		w.WriteInt(int64(p))
		// Redis reports one of "none", "connect", "connecting", "sync" or "connected". This
		// link is either up or it is being retried, and there is no partial state in between
		// worth reporting as though it were finer-grained than it is.
		state := "connect"
		if linkUp {
			state = "connected"
		}
		w.WriteBulkString(state)
		w.WriteInt(slaveOffset)
		return false
	}

	sort.Slice(replicas, func(i, j int) bool { return replicas[i].port < replicas[j].port })
	w.WriteArrayHeader(3)
	w.WriteBulkString("master")
	w.WriteInt(offset)
	w.WriteArrayHeader(len(replicas))
	for _, rc := range replicas {
		host := rc.addr
		if h, _, err := net.SplitHostPort(rc.addr); err == nil {
			host = h
		}
		w.WriteArrayHeader(3)
		w.WriteBulkString(host)
		w.WriteBulkString(rc.port)
		w.WriteBulkString(strconv.FormatInt(rc.ack.Load(), 10))
	}
	return false
}

// keepalive is the no-op command a master periodically writes to each replica
// feed purely to prove the connection still carries bytes.
var keepalive = [][]byte{[]byte("PING")}

// getack is what a master sends down each feed to ask its replicas where they are.
// It is written straight to the feeds rather than through shipReplicas, because it
// must not advance the replication offset: the offset is the very thing being asked
// about, and moving it would make the answer chase a target that keeps receding.
var getack = [][]byte{[]byte("REPLCONF"), []byte("GETACK"), []byte("*")}

// endSnapshot terminates the command stream a full resync sends. The snapshot is a
// run of ordinary write commands rather than one length-prefixed blob (a single blob
// would exceed the protocol's bulk-string limit for a large dataset, the same reason
// Dump chunks at all), so the replica needs an explicit marker to know where the
// dataset ends and the live stream -- the part its replication offset counts --
// begins.
var endSnapshot = [][]byte{[]byte("REPLCONF"), []byte("ENDSNAPSHOT")}

// isKeepalive reports whether args is a master's liveness probe. A replica drops
// it instead of applying or forwarding it: it carries no state, buffering it would
// splice a stray command into a replayed MULTI group, and each feed downstream
// already sends a probe of its own.
func isKeepalive(args [][]byte) bool {
	return len(args) == 1 && strings.EqualFold(string(args[0]), "PING")
}

// newReplID returns a fresh replication ID: a random 40-character hex string, the
// same shape Redis uses. It identifies the stream one server serves, so a replica
// asking to continue from an offset can be told whether that offset refers to this
// history at all.
func newReplID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A failed read from the OS entropy source is not a reason to refuse to serve;
		// fall back to something unique enough to distinguish this process's stream.
		return hex.EncodeToString([]byte(strconv.FormatInt(time.Now().UnixNano(), 16)))
	}
	return hex.EncodeToString(b[:])
}

// psyncRequest parses the replid and offset a PSYNC asks to continue from. A bare
// PSYNC, or the "? -1" a replica with no history sends, means "full resync": the
// returned offset is negative, which no backlog range can satisfy.
func psyncRequest(args [][]byte) (id string, offset int64) {
	if len(args) < 3 {
		return "", -1
	}
	off, ok := parseInt64(args[2])
	if !ok {
		return "", -1
	}
	return string(args[1]), off
}

// handleSync turns the calling connection into a replication feed. It answers the
// replica's PSYNC with either a continuation from the backlog or a full resync,
// registers the connection as a replica, and streams every subsequent write until the
// replica disconnects or the server shuts down.
//
// The decision and the registration happen under propMu, which write commands also
// hold across their mutation+propagation. That is what makes both answers exact:
//
//   - Full resync: the snapshot is a consistent point-in-time cut. No write can
//     interleave, and every write after this point is enqueued to the new replica
//     exactly once, so nothing is applied twice or missed.
//   - Partial resync: the backlog is read up to exactly the offset the feed will
//     continue from, so the bytes sent from the backlog and the commands queued to the
//     feed abut with no gap and no overlap.
//
// One narrow window remains: a master started without -aof only flips propagating to
// true here, so a write already in flight on the fast path at the instant of the very
// first PSYNC may be missed. Running a replicated master with -aof (propagating from
// startup) closes it.
func (s *Server) handleSync(ctx context.Context, sess *session, r *resp.Reader, w *resp.Writer, args [][]byte) {
	rc := newReplicaConn(replFeedBuffer)
	rc.addr, rc.port = splitHostPort(sess.addr), sess.listeningPort
	wantID, wantOffset := psyncRequest(args)

	// From here on this connection is a replication feed, not a client. CLIENT LIST
	// has to say so: operators and monitoring identify replica links by the S flag,
	// and CLIENT KILL TYPE would otherwise treat a feed as ordinary traffic.
	sess.isReplicaFeed.Store(true)
	defer sess.isReplicaFeed.Store(false)

	s.propagating.Store(true)
	s.propMu.Lock()
	s.mu.Lock()
	var (
		cont     []byte
		partial  bool
		snapshot [][][]byte
	)
	if wantID != "" && wantID == s.replID {
		cont, partial = s.backlog.read(wantOffset)
	}
	if !partial {
		// Every database, each framed by the SELECT that puts the replica into it, and
		// ending back at the database the shared stream is positioned in. See dumpAll.
		snapshot = s.dumpAll()
	}
	offset := s.replOffset
	s.replicas[rc] = struct{}{}
	s.mu.Unlock()
	s.propMu.Unlock()
	defer s.removeReplica(rc)

	if partial {
		s.partialOK.Add(1)
		if w.WriteSimple("CONTINUE") != nil || w.WriteRaw(cont) != nil || w.Flush() != nil {
			return
		}
	} else {
		if wantOffset >= 0 {
			s.partialErr.Add(1) // it asked to continue and we could not oblige
		}
		s.fullSyncs.Add(1)
		if w.WriteSimple("FULLRESYNC "+s.replID+" "+strconv.FormatInt(offset, 10)) != nil {
			return
		}
		for _, cmd := range snapshot {
			if w.WriteCommand(cmd) != nil {
				return
			}
		}
		if w.WriteCommand(endSnapshot) != nil || w.Flush() != nil {
			return
		}
	}

	// The replica reports its progress back up this same connection; read those
	// acknowledgements so WAIT can count them.
	go s.readReplicaAcks(rc, r)

	// The keepalive rides this feed only; it is never handed to shipRaw, so it
	// never reaches the AOF and never enters a propagated MULTI group.
	ping := time.NewTicker(s.pingEvery)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rc.dropped:
			return // fell too far behind: end the feed so the replica resyncs
		case <-ping.C:
			if w.WriteCommand(keepalive) != nil || w.Flush() != nil {
				return
			}
		case cmd := <-rc.ch:
			if w.WriteCommand(cmd) != nil {
				return
			}
			if w.Flush() != nil {
				return
			}
		}
	}
}

// SetReplBacklogSize sets how many bytes of the replication stream are retained for
// partial resync. It must be called before serving starts, and a size of zero
// disables continuations entirely (every PSYNC then gets a full resync).
//
// The bound is a direct trade: memory against how long a replica may be absent and
// still come back cheaply. Sizing it is a matter of write rate times the disconnect
// window worth tolerating -- 1 MB, the default, covers a few seconds of a busy master
// or minutes of a quiet one.
func (s *Server) SetReplBacklogSize(size int) {
	s.mu.Lock()
	s.backlog = newReplBacklog(size)
	s.backlog.start = s.replOffset
	s.backlog.end = s.replOffset
	s.mu.Unlock()
}

// ReplBacklogSize reports the configured backlog size in bytes, for CONFIG GET.
func (s *Server) ReplBacklogSize() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.backlog.buf))
}

// cmdReplConf implements the REPLCONF handshake exchange. A replica announces the
// port it serves on, which is all the master needs to report it in INFO the way an
// operator expects (the peer port of the replication socket is ephemeral and names
// nothing they can connect to).
//
// Unknown options are acknowledged rather than rejected: REPLCONF is Redis's
// extension point, and a newer replica sending an option this master has no use for
// must still be able to finish its handshake.
func cmdReplConf(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	for i := 1; i+1 < len(args); i += 2 {
		if strings.EqualFold(string(args[i]), "listening-port") {
			sess.listeningPort = string(args[i+1])
		}
	}
	if len(args) >= 2 && strings.EqualFold(string(args[1]), "ACK") {
		return // an acknowledgement is a report, not a request: it gets no reply
	}
	w.WriteSimple("OK")
}

// cmdWait implements WAIT numreplicas timeout: it reports how many replicas have
// acknowledged everything written so far, waiting up to timeout milliseconds for the
// count to reach numreplicas.
//
// It is a measurement, not a guarantee. The write it waits on was already applied and
// acknowledged to its client, so WAIT cannot make a write durable retroactively; what
// it offers is the number of copies that have it, which is what a caller needs to
// decide whether to proceed. A count below numreplicas is returned rather than
// treated as an error, exactly as in Redis.
func cmdWait(s *Server, w *resp.Writer, args [][]byte) bool {
	numreplicas, ok := parseInt(args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	timeoutMs, ok := parseInt64(args[2])
	if !ok || timeoutMs < 0 {
		w.WriteError("ERR timeout is not an integer or out of range")
		return false
	}
	if s.isReplica() {
		// A replica does not own the stream, so it has nothing to be acknowledged.
		w.WriteError("ERR WAIT cannot be used with replica instances. Please also note that if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.")
		return false
	}
	w.WriteInt(int64(s.waitForAcks(numreplicas, timeoutMs)))
	return false
}

// waitForAcks polls the replicas' reported offsets until enough of them have caught
// up with the current stream position, the timeout elapses, or the server shuts down.
//
// The target offset is read once, at entry: WAIT asks about the writes that preceded
// it, and re-reading a stream that other clients keep extending would make it wait
// for work that arrived after the question was asked.
func (s *Server) waitForAcks(numreplicas int, timeoutMs int64) int {
	s.mu.Lock()
	target := s.replOffset
	ctx := s.baseCtx
	s.mu.Unlock()

	if n := s.ackedReplicas(target); n >= numreplicas {
		return n
	}
	s.requestAcks() // replicas volunteer their offset only periodically; ask now

	var timeout <-chan time.Time
	if timeoutMs > 0 {
		t := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
		defer t.Stop()
		timeout = t.C
	}
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-timeout:
			return s.ackedReplicas(target)
		case <-ctx.Done():
			// A timeout of 0 means "wait indefinitely"; without this the connection's
			// goroutine would outlive the server and block a clean shutdown forever.
			return s.ackedReplicas(target)
		case <-tick.C:
			if n := s.ackedReplicas(target); n >= numreplicas {
				return n
			}
		}
	}
}

// ackedReplicas counts the replicas that have acknowledged at least offset.
func (s *Server) ackedReplicas(offset int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for rc := range s.replicas {
		if rc.ack.Load() >= offset {
			n++
		}
	}
	return n
}

// requestAcks asks every replica to report its offset now.
func (s *Server) requestAcks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for rc := range s.replicas {
		select {
		case rc.ch <- getack:
		default:
			// A feed with no room is already about to be dropped by a write; asking it
			// for an acknowledgement is not worth dropping it for.
		}
	}
}

// readReplicaAcks records the offsets a replica reports with REPLCONF ACK.
//
// A read error only stops the accounting, it does not end the feed: the write side
// already detects a dead connection (its keepalive fails), and tearing the feed down
// here would disconnect any replica that simply never acknowledges -- including one
// talking an older protocol that has nothing to say back.
func (s *Server) readReplicaAcks(rc *replicaConn, r *resp.Reader) {
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		if len(args) >= 3 && strings.EqualFold(string(args[0]), "REPLCONF") &&
			strings.EqualFold(string(args[1]), "ACK") {
			if off, ok := parseInt64(args[2]); ok {
				rc.ack.Store(off)
			}
		}
	}
}

func (s *Server) removeReplica(rc *replicaConn) {
	s.mu.Lock()
	delete(s.replicas, rc)
	s.mu.Unlock()
}

// splitHostPort returns just the host part of an addr:port pair, which is how INFO
// reports a replica's address.
func splitHostPort(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// ReplicaOf makes this server replicate from the master at addr. Passing an
// empty addr promotes the server back to master ("REPLICAOF NO ONE"). Any
// existing replication link is stopped first.
//
// Changing the master (or being promoted) discards the continuation point held for
// the old one. A promoted master accepts its own writes, so its history diverges from
// the one it was following: continuing that history later would silently skip
// everything that happened in between, and a full resync is the only honest answer.
func (s *Server) ReplicaOf(ctx context.Context, addr string) {
	s.mu.Lock()
	if s.replCancel != nil {
		s.replCancel()
		s.replCancel = nil
	}
	if addr != s.masterAddr {
		s.masterReplID = ""
		s.slaveOffset = 0
	}
	if addr == "" {
		s.role = "master"
		s.masterAddr = ""
		s.masterLinkUp = false
		s.mu.Unlock()
		return
	}
	cctx, cancel := context.WithCancel(ctx)
	s.replCancel = cancel
	s.role = "replica"
	s.masterAddr = addr
	s.mu.Unlock()

	// Clients blocked in BLPOP and its relatives are waiting for a write to become
	// possible, and writes are now refused on this node. Waiting for something that can
	// no longer happen is worse than an error, so they are told -- with the same
	// -UNBLOCKED error Redis uses, and outside mu because unblocking touches the block
	// registry.
	s.unblockAll(errUnblockedRoleChange)

	go s.replicationLoop(cctx, addr)
}

// replicationLoop maintains a connection to the master, applying the command
// stream it receives. It reconnects with a fixed backoff until ctx is canceled.
func (s *Server) replicationLoop(ctx context.Context, addr string) {
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := s.dialMaster(addr)
		if err != nil {
			if sleepCtx(ctx, replBackoff) {
				return
			}
			continue
		}

		// Close the connection if ctx is canceled so the read loop unblocks.
		connDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				conn.Close()
			case <-connDone:
			}
		}()

		s.runReplicationLink(conn)

		close(connDone)
		conn.Close()
		s.setMasterLinkUp(false)
		if sleepCtx(ctx, replBackoff) {
			return
		}
	}
}

// runReplicationLink performs the handshake on an established connection to the
// master and then applies its stream until the link fails.
//
// Every read carries a deadline. A master that dies without closing its socket -- a
// partition, a half-open connection, a hung process -- would otherwise leave this
// loop blocked in ReadCommand forever, serving stale data and never reconnecting. The
// master's periodic keepalive is what keeps the deadline from firing on a merely idle
// link.
func (s *Server) runReplicationLink(conn net.Conn) {
	r := resp.NewReader(conn)
	hw := resp.NewWriter(conn)
	discard := resp.NewWriter(io.Discard)
	applier := newTxApplier(s, discard)

	offset, ok := s.handshake(conn, r, hw, applier)
	if !ok {
		return
	}
	// Everything the master sends from here on is the live stream, and its byte count
	// is what the offset advances by. base is subtracted so the count starts at the
	// offset the master reported rather than at the bytes of the handshake.
	base := r.Consumed()
	s.setMasterLinkUp(true)
	lastAck := time.Now()

	for {
		if conn.SetReadDeadline(time.Now().Add(s.readTimeout)) != nil {
			return
		}
		before := r.Consumed()
		args, err := r.ReadCommand()
		if err != nil {
			return // EOF, a canceled ctx, or a master gone silent: resync
		}
		if isKeepalive(args) || isGetAck(args) {
			// Out of band: the keepalive and the acknowledgement request are written
			// straight to this one feed rather than through the shared stream, so their
			// bytes must not advance the offset. Discounting them here -- by moving the
			// base past them -- is what keeps this replica's idea of the offset equal to
			// the master's, and equal to its siblings'.
			base += r.Consumed() - before
			if s.sendAck(hw, offset+r.Consumed()-base) != nil {
				return
			}
			lastAck = time.Now()
			continue
		}
		// Apply with MULTI/EXEC awareness (atomic transactions), and forward
		// the raw stream — including the MULTI/EXEC markers — to our own
		// replicas/AOF so chained replicas and replica-side persistence keep
		// the same framing. The stream is already absolute/effect form.
		applier.feed(args)
		if s.propagating.Load() {
			s.propMu.Lock()
			s.forward(args)
			s.propMu.Unlock()
		}
		s.setSlaveOffset(offset + r.Consumed() - base)
		if time.Since(lastAck) >= replAckInterval {
			if s.sendAck(hw, offset+r.Consumed()-base) != nil {
				return
			}
			lastAck = time.Now()
		}
	}
}

// handshake authenticates, announces this replica, and issues the PSYNC. It returns
// the stream offset the live feed starts at.
//
// The order is Redis's: AUTH first, because a password-protected master refuses
// everything else; then REPLCONF listening-port so the master can report this replica
// in INFO; then PSYNC, whose answer decides whether the dataset arrives too.
func (s *Server) handshake(conn net.Conn, r *resp.Reader, w *resp.Writer, applier *txApplier) (offset int64, ok bool) {
	if conn.SetReadDeadline(time.Now().Add(s.readTimeout)) != nil {
		return 0, false
	}
	if pass := s.MasterAuth(); pass != "" {
		if _, err := request(r, w, [][]byte{[]byte("AUTH"), []byte(pass)}); err != nil {
			// Nothing is retried inline: a rejected password will still be rejected a
			// microsecond later, and the reconnect backoff keeps a misconfigured replica
			// from hammering the master. The error deliberately does not include the
			// password.
			log.Printf("shardkv: master rejected authentication: %v", err)
			return 0, false
		}
	}
	if port := s.listeningPort(); port != "" {
		// The reply is deliberately ignored: a master that does not know REPLCONF can
		// still serve the stream, and this exchange only affects what its INFO reports.
		_, _ = request(r, w, [][]byte{[]byte("REPLCONF"), []byte("listening-port"), []byte(port)})
	}

	id, from := s.replicationTarget()
	if w.WriteCommand([][]byte{[]byte("PSYNC"), []byte(id), []byte(strconv.FormatInt(from, 10))}) != nil ||
		w.Flush() != nil {
		return 0, false
	}
	status, err := r.ReadStatus()
	if err != nil {
		log.Printf("shardkv: PSYNC failed: %v", err)
		return 0, false
	}

	fields := strings.Fields(status)
	switch {
	case len(fields) > 0 && strings.EqualFold(fields[0], "CONTINUE"):
		// The master still had our history: keep the dataset and resume the stream.
		return from, true

	case len(fields) == 3 && strings.EqualFold(fields[0], "FULLRESYNC"):
		masterID := fields[1]
		start, ok := parseInt64([]byte(fields[2]))
		if !ok {
			return 0, false
		}
		// The snapshot describes the master's dataset, not a delta against ours. Keys we
		// hold that the master no longer has (deleted while we were disconnected) are
		// not mentioned by it, in any database, so without a flush they would survive a
		// resync forever. The flush is forwarded downstream for the same reason.
		s.flushDatabases()
		s.applyFromMaster(applier, [][]byte{[]byte("FLUSHALL")})
		for {
			if conn.SetReadDeadline(time.Now().Add(s.readTimeout)) != nil {
				return 0, false
			}
			args, err := r.ReadCommand()
			if err != nil {
				return 0, false
			}
			if isEndSnapshot(args) {
				break
			}
			if isKeepalive(args) {
				continue
			}
			s.applyFromMaster(applier, args)
		}
		s.adoptMaster(masterID, start)
		return start, true
	}
	log.Printf("shardkv: master answered PSYNC with %q", status)
	return 0, false
}

// applyFromMaster applies one command from the master's stream and forwards it to
// this server's own AOF and replicas, so a chained replica converges on the same
// dataset and a replica's own log stays replayable.
func (s *Server) applyFromMaster(applier *txApplier, args [][]byte) {
	applier.feed(args)
	if s.propagating.Load() {
		s.propMu.Lock()
		s.forward(args)
		s.propMu.Unlock()
	}
}

// request writes a command to the master and reads its status reply.
func request(r *resp.Reader, w *resp.Writer, args [][]byte) (string, error) {
	if err := w.WriteCommand(args); err != nil {
		return "", err
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return r.ReadStatus()
}

// sendAck reports the replica's processed offset to the master, which is what lets a
// client's WAIT on the master learn how far each replica has got.
func (s *Server) sendAck(w *resp.Writer, offset int64) error {
	if err := w.WriteCommand([][]byte{
		[]byte("REPLCONF"), []byte("ACK"), []byte(strconv.FormatInt(offset, 10)),
	}); err != nil {
		return err
	}
	return w.Flush()
}

func isGetAck(args [][]byte) bool {
	return len(args) >= 2 && strings.EqualFold(string(args[0]), "REPLCONF") &&
		strings.EqualFold(string(args[1]), "GETACK")
}

func isEndSnapshot(args [][]byte) bool {
	return len(args) == 2 && strings.EqualFold(string(args[0]), "REPLCONF") &&
		strings.EqualFold(string(args[1]), "ENDSNAPSHOT")
}

// replicationTarget reports the replication ID and offset to ask a master to continue
// from. ("?", -1) means "no usable history": a first connection, or one after a role
// change, where only a full resync is correct.
//
// The offset asked for is the next byte wanted, i.e. one past what was processed --
// the same convention Redis uses, and what makes "continue from exactly where I
// stopped" expressible.
func (s *Server) replicationTarget() (string, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.masterReplID == "" {
		return "?", -1
	}
	return s.masterReplID, s.slaveOffset
}

// adoptMaster records the history a full resync placed this replica on.
func (s *Server) adoptMaster(id string, offset int64) {
	s.mu.Lock()
	s.masterReplID = id
	s.slaveOffset = offset
	s.mu.Unlock()
}

func (s *Server) setSlaveOffset(offset int64) {
	s.mu.Lock()
	s.slaveOffset = offset
	s.mu.Unlock()
}

func (s *Server) setMasterLinkUp(up bool) {
	s.mu.Lock()
	s.masterLinkUp = up
	s.mu.Unlock()
}

// listeningPort reports the port this server accepts connections on, announced to the
// master with REPLCONF listening-port. It is empty until Listen has been called.
func (s *Server) listeningPort() string {
	if s.ln == nil {
		return ""
	}
	_, port, err := net.SplitHostPort(s.ln.Addr().String())
	if err != nil {
		return ""
	}
	return port
}

// sleepCtx waits for d or until ctx is canceled. It reports whether ctx was
// canceled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}
