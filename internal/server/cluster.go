package server

// Cluster mode: the slot map, the node table, and the configuration that survives a
// restart.
//
// # What this is, and what it is not
//
// Real Redis Cluster has two halves. The client-facing half is the slot map, the
// redirects (MOVED/ASK/CROSSSLOT), the CLUSTER administration commands and the
// migration primitives -- everything a client library or an operator interacts with.
// The other half is a binary gossip protocol on a second port (the "cluster bus", the
// listening port plus 10000) over which nodes exchange PING/PONG packets to detect
// failures, propagate configuration, elect replicas and settle conflicts by epoch.
//
// This implements the first half completely and the second half not at all, on
// purpose. The consequence is stated rather than hidden: **configuration does not
// propagate**. A slot assignment made on one node is known only to that node until
// somebody tells the others. There is exactly one place where information crosses
// nodes without an operator typing it -- CLUSTER MEET, which opens an ordinary RESP
// client connection to the peer, reads its CLUSTER NODES, and adopts the peer's id and
// the slots it claims that are still unassigned here. That is a pull at MEET time, not
// gossip: nothing is exchanged afterwards, no failure is detected, and no node ever
// changes another node's mind.
//
// The README's Cluster section states the boundary in full. The reason for drawing it
// here rather than approximating the bus is that a half-implemented gossip protocol
// fails by *disagreeing silently* -- two nodes each convinced they own a slot -- which
// is the same class of bug as every invariant in CLAUDE.md, and the one thing a
// deliberately-simple design can rule out entirely.
//
// # Why the slot map is copy-on-write
//
// A lookup sits on the path of every command in cluster mode, so it must be one atomic
// load and an index -- no lock, no map. Mutations (ADDSLOTS, SETSLOT) are
// administrative and rare, so they copy the whole 16384-entry table under a mutex and
// swap the pointer. That also buys consistency for free where it matters most: a
// multi-key command resolves all of its keys against one immutable snapshot, so a
// concurrent SETSLOT can never make two keys of the same command disagree about who
// owns their slot.

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// clusterNode is one node as this node knows it. It is immutable once constructed: the
// slot table holds these by pointer and is read without a lock, so an address that
// changed in place would be a data race. An updated node is a new value that replaces
// the old one in the table and in the node map together.
type clusterNode struct {
	id    string
	ip    string
	port  int
	cport int // the cluster-bus port Redis would use; reported for format compatibility only
	// replicaOf is the id of the master this node follows, or "" for a master.
	replicaOf   string
	configEpoch int64
	myself      bool
}

// addr is the "host:port" a MOVED or ASK redirect names, and the address a client will
// dial next. It is what CLUSTER MEET was told, which is why the announce address must
// be the one clients can actually reach.
func (n *clusterNode) addr() string {
	return net.JoinHostPort(n.ip, strconv.Itoa(n.port))
}

func (n *clusterNode) isReplica() bool { return n.replicaOf != "" }

// slotInfo is one slot's assignment plus its migration state. The two migration fields
// are what make ASK redirects possible: migrating names the node a key is moving to
// (set on the source), importing names the node it is coming from (set on the
// destination). At most one of them is ever set.
type slotInfo struct {
	owner     *clusterNode
	migrating *clusterNode
	importing *clusterNode
}

// slotTable is the whole slot map. It is treated as immutable: a mutation clones it,
// edits the clone, and swaps the pointer, so readers never need a lock and always see
// one consistent generation.
type slotTable struct {
	slots [numSlots]slotInfo
}

func (t *slotTable) clone() *slotTable {
	c := *t
	return &c
}

// count reports how many slots have an owner, which is what CLUSTER INFO's
// slots_assigned line and the cluster_state verdict are computed from.
func (t *slotTable) count() int {
	n := 0
	for i := range t.slots {
		if t.slots[i].owner != nil {
			n++
		}
	}
	return n
}

// clusterState is a server's whole cluster configuration.
type clusterState struct {
	// enabled is the gate. Every command pays exactly one atomic load for cluster mode
	// when it is off, which is the same discipline invariant 12 imposes on the
	// observability hooks: a feature nobody switched on must not cost anything.
	enabled atomic.Bool

	// table is the slot map, swapped wholesale on every change. Read with one atomic
	// load; never mutated in place.
	table atomic.Pointer[slotTable]

	// mu guards everything below, all of which changes only on administrative commands.
	mu           sync.Mutex
	myID         string
	nodes        map[string]*clusterNode
	configFile   string
	currentEpoch int64
	// announceIP/announcePort are what this node calls itself in CLUSTER NODES, SLOTS
	// and SHARDS. They have to be an address the *client* can reach, which is not
	// always the address the listener is bound to -- a node behind a port mapping, or
	// reached through host.docker.internal, is the ordinary case.
	announceIP   string
	announcePort int
}

func newClusterState() *clusterState {
	cs := &clusterState{nodes: map[string]*clusterNode{}}
	cs.table.Store(&slotTable{})
	return cs
}

// ClusterOptions configures cluster mode. The zero value is valid: an in-memory
// configuration announced on the listening address.
type ClusterOptions struct {
	// ConfigFile is where the node id and the slot map are persisted, so a restarted
	// node rejoins as itself rather than as a stranger. Empty keeps it in memory only.
	ConfigFile string
	// AnnounceIP and AnnouncePort are the address other nodes and redirected clients
	// should use. Empty/zero means the listening address.
	AnnounceIP   string
	AnnouncePort int
}

// ClusterEnabled reports whether this server is running in cluster mode. It is the
// single gate every cluster behaviour hangs off, and it is one atomic load.
func (s *Server) ClusterEnabled() bool { return s.cluster.enabled.Load() }

// ClusterMyID reports this node's id, which is what CLUSTER MYID answers and what the
// startup log announces so an operator can drive SETSLOT without a round trip.
func (s *Server) ClusterMyID() string {
	if n := s.cluster.myself(); n != nil {
		return n.id
	}
	return ""
}

// EnableCluster turns on cluster mode. It must be called after Listen (so the
// announced port is known) and before serving starts.
//
// Cluster mode has exactly one database, as in Redis: the slot map is a partition of a
// single keyspace, and a second database would be a keyspace with no slots and so no
// node responsible for it. The check is here rather than a silent clamp because
// -databases and -cluster-enabled together are a contradiction an operator should be
// told about.
func (s *Server) EnableCluster(opts ClusterOptions) error {
	if len(s.dbs) != 1 {
		return fmt.Errorf("cluster mode requires a single database, but %d are configured", len(s.dbs))
	}
	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.announceIP = opts.AnnounceIP
	if cs.announceIP == "" {
		cs.announceIP = "127.0.0.1"
	}
	cs.announcePort = opts.AnnouncePort
	if cs.announcePort == 0 {
		port, err := strconv.Atoi(s.listeningPort())
		if err != nil {
			return fmt.Errorf("cluster mode needs a listening port: %w", err)
		}
		cs.announcePort = port
	}
	cs.configFile = opts.ConfigFile

	// A configuration on disk is this node's identity: its id, the slots it owns, and
	// the peers it had met. Reloading it is what lets a restarted node rejoin as itself
	// instead of as a stranger holding data it no longer claims.
	if cs.configFile != "" {
		if err := cs.loadLocked(); err != nil {
			return fmt.Errorf("loading cluster config %s: %w", cs.configFile, err)
		}
	}
	if cs.myID == "" {
		cs.myID = newReplID()
	}
	// Rebuild myself from the announce address, which may have changed since the
	// configuration was written (a different port mapping, a new announce IP).
	old := cs.nodes[cs.myID]
	me := &clusterNode{
		id: cs.myID, ip: cs.announceIP, port: cs.announcePort,
		cport: cs.announcePort + 10000, myself: true,
	}
	if old != nil {
		me.replicaOf, me.configEpoch = old.replicaOf, old.configEpoch
	}
	cs.putNodeLocked(me)

	cs.enabled.Store(true)
	return cs.saveLocked()
}

// myself returns this node's entry. It is never nil once cluster mode is enabled.
func (cs *clusterState) myself() *clusterNode {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.nodes[cs.myID]
}

// putNodeLocked installs a node and repoints every slot that referred to the previous
// value of the same id at the new one, so the immutable-node discipline holds: nothing
// in the table ever points at a node that has been superseded.
func (cs *clusterState) putNodeLocked(n *clusterNode) {
	prev := cs.nodes[n.id]
	cs.nodes[n.id] = n
	if prev == nil {
		return
	}
	cs.mutateLocked(func(t *slotTable) {
		for i := range t.slots {
			if t.slots[i].owner == prev {
				t.slots[i].owner = n
			}
			if t.slots[i].migrating == prev {
				t.slots[i].migrating = n
			}
			if t.slots[i].importing == prev {
				t.slots[i].importing = n
			}
		}
	})
}

// mutateLocked applies fn to a copy of the slot map and publishes it. Callers hold mu;
// readers take no lock at all, which is the point (see the file comment).
func (cs *clusterState) mutateLocked(fn func(*slotTable)) {
	next := cs.table.Load().clone()
	fn(next)
	cs.table.Store(next)
}

// slot returns one slot's assignment. This is the hot-path read: one atomic load and
// an index, no lock and no allocation.
func (cs *clusterState) slot(i int) slotInfo { return cs.table.Load().slots[i] }

// nodeByID returns a known node, or nil.
func (cs *clusterState) nodeByID(id string) *clusterNode {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.nodes[id]
}

// sortedNodesLocked returns the node table in a stable order -- myself first, then by
// id -- so CLUSTER NODES, SHARDS and the config file do not reshuffle themselves
// between calls the way a map iteration would.
func (cs *clusterState) sortedNodesLocked() []*clusterNode {
	out := make([]*clusterNode, 0, len(cs.nodes))
	for _, n := range cs.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].myself != out[j].myself {
			return out[i].myself
		}
		return out[i].id < out[j].id
	})
	return out
}

// ownedSlots returns the slot ranges a node owns, as inclusive [start, end] pairs. It
// is what CLUSTER NODES, SLOTS and SHARDS all render, and collapsing runs into ranges
// there rather than in each of them is what keeps the three formats agreeing.
func (cs *clusterState) ownedSlots(n *clusterNode) [][2]int {
	t := cs.table.Load()
	var out [][2]int
	start := -1
	for i := 0; i < numSlots; i++ {
		if t.slots[i].owner == n {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			out = append(out, [2]int{start, i - 1})
			start = -1
		}
	}
	if start >= 0 {
		out = append(out, [2]int{start, numSlots - 1})
	}
	return out
}

// --- configuration file -------------------------------------------------------

// The on-disk format is the CLUSTER NODES text plus Redis's trailing vars line. Using
// the wire format as the file format means there is one parser and one renderer to keep
// correct instead of two, and it means the file is directly readable -- an operator
// diagnosing a cluster can cat it and see exactly what CLUSTER NODES would have said.

// loadLocked reads the configuration file if it exists. A missing file is not an error
// (this is a node's first start); a malformed one is, because continuing from a
// half-understood slot map would mean serving keys this node may no longer own.
func (cs *clusterState) loadLocked() error {
	f, err := os.Open(cs.configFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }() // read-only: nothing to fail on close

	type pending struct {
		node  *clusterNode
		slots []string
	}
	var parsed []pending
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] == "vars" {
			continue
		}
		if len(fields) < 8 {
			return fmt.Errorf("malformed node line %q", sc.Text())
		}
		ip, port, cport, err := parseNodeAddr(fields[1])
		if err != nil {
			return err
		}
		epoch, err := strconv.ParseInt(fields[6], 10, 64)
		if err != nil {
			return fmt.Errorf("malformed config epoch %q", fields[6])
		}
		n := &clusterNode{id: fields[0], ip: ip, port: port, cport: cport, configEpoch: epoch}
		if strings.Contains(fields[2], "myself") {
			cs.myID = n.id
			n.myself = true
		}
		if fields[3] != "-" {
			n.replicaOf = fields[3]
		}
		parsed = append(parsed, pending{n, fields[8:]})
	}
	if err := sc.Err(); err != nil {
		return err
	}

	// Nodes first, then slots: a slot line names an owner by position in the file, and
	// the owner has to exist before anything can point at it.
	for _, p := range parsed {
		cs.nodes[p.node.id] = p.node
	}
	table := &slotTable{}
	for _, p := range parsed {
		for _, spec := range p.slots {
			if strings.HasPrefix(spec, "[") {
				continue // a migration marker: transient state, deliberately not persisted
			}
			lo, hi, err := parseSlotRange(spec)
			if err != nil {
				return err
			}
			for i := lo; i <= hi; i++ {
				table.slots[i].owner = p.node
			}
		}
	}
	cs.table.Store(table)
	return nil
}

func parseNodeAddr(field string) (ip string, port, cport int, err error) {
	// <ip>:<port>@<cport>[,hostname]
	if i := strings.IndexByte(field, ','); i >= 0 {
		field = field[:i]
	}
	at := strings.LastIndexByte(field, '@')
	if at < 0 {
		return "", 0, 0, fmt.Errorf("malformed node address %q", field)
	}
	cport, err = strconv.Atoi(field[at+1:])
	if err != nil {
		return "", 0, 0, fmt.Errorf("malformed cluster-bus port in %q", field)
	}
	host, portStr, err := net.SplitHostPort(field[:at])
	if err != nil {
		return "", 0, 0, fmt.Errorf("malformed node address %q", field)
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", 0, 0, fmt.Errorf("malformed port in %q", field)
	}
	return host, port, cport, nil
}

func parseSlotRange(spec string) (lo, hi int, err error) {
	if i := strings.IndexByte(spec, '-'); i >= 0 {
		lo, err = strconv.Atoi(spec[:i])
		if err == nil {
			hi, err = strconv.Atoi(spec[i+1:])
		}
	} else {
		lo, err = strconv.Atoi(spec)
		hi = lo
	}
	if err != nil || lo < 0 || hi >= numSlots || lo > hi {
		return 0, 0, fmt.Errorf("malformed slot specification %q", spec)
	}
	return lo, hi, nil
}

// saveLocked writes the configuration atomically: a temporary file in the same
// directory, then a rename. A crash during the write must not leave a node with a
// truncated slot map, which it would then load on restart and act on.
func (cs *clusterState) saveLocked() error {
	if cs.configFile == "" {
		return nil
	}
	var b strings.Builder
	for _, n := range cs.sortedNodesLocked() {
		b.WriteString(cs.nodeLineLocked(n))
		b.WriteByte('\n')
	}
	fmt.Fprintf(&b, "vars currentEpoch %d lastVoteEpoch 0\n", cs.currentEpoch)

	tmp, err := os.CreateTemp(filepath.Dir(cs.configFile), ".shardkv-cluster-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	err = writeAndSync(tmp, b.String())
	// The close is reported only when the write itself succeeded: a failed write is the
	// more useful diagnosis, and a close error after one is a consequence of it.
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(name) // best effort: the write already failed, and this is a temp file
		return err
	}
	return os.Rename(name, cs.configFile)
}

func writeAndSync(f *os.File, s string) error {
	if _, err := f.WriteString(s); err != nil {
		return err
	}
	// Synced before the rename, so the file the rename publishes is the whole file: a
	// crash must not leave a node with a truncated slot map that it then loads and acts on.
	return f.Sync()
}

// saveErrLocked persists the configuration, logging rather than failing a command that
// has already taken effect in memory: refusing an ADDSLOTS the server has already
// applied would leave the client with an error and the server with the change.
func (cs *clusterState) saveErrLocked() {
	if err := cs.saveLocked(); err != nil {
		logClusterSaveFailure(err)
	}
}

// --- CLUSTER MEET -------------------------------------------------------------

// clusterMeetTimeout bounds the handshake. MEET is synchronous here (Redis's is
// asynchronous, completing over the gossip bus this server does not have), so a peer
// that is down has to become an error the operator sees rather than a command that
// silently did nothing.
const clusterMeetTimeout = 5 * time.Second

// meet introduces this node to a peer at addr.
//
// It is the one place configuration crosses a node boundary without an operator typing
// it, and it is a *pull*, once: an ordinary RESP client connection, one CLUSTER NODES,
// and then it is over. From the peer's reply this node takes two things -- the peer's
// id and address, and the slots the peer claims.
//
// A claimed slot is adopted only when it is unassigned here. That rule is what makes a
// gossip-free cluster safe to assemble in any order: MEET can never move a slot from
// one node to another, so two nodes that disagree about an owner stay disagreeing
// visibly (one of them will redirect to the other) instead of silently swapping
// ownership under a client's feet. Moving a slot is SETSLOT's job, and SETSLOT is
// explicit.
func (s *Server) meet(ip string, port int) error {
	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, clusterMeetTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(clusterMeetTimeout)); err != nil {
		return err
	}

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	if pass := s.RequirePass(); pass != "" {
		// The peer is configured like this node, so the password this node requires is the
		// one it presents. Without it a secured cluster could not be assembled at all.
		if err := writeNodeCommand(w, "AUTH", pass); err != nil {
			return err
		}
		if _, err := r.ReadStatus(); err != nil {
			return err
		}
	}
	if err := writeNodeCommand(w, "CLUSTER", "NODES"); err != nil {
		return err
	}
	table, err := r.ReadBulk()
	if err != nil {
		return err
	}

	peer, slots, err := parseMeetReply(string(table))
	if err != nil {
		return err
	}
	// The address recorded is the one the peer *announces*, not the one it was reached
	// at. Those are routinely different and the difference is the point: the address
	// used here has to work for a client following a MOVED, which may be on the other
	// side of a port mapping or a container boundary from the operator running MEET. The
	// peer is the only party that knows what clients should dial, which is what
	// -cluster-announce-ip is for; the dialled address is merely how the handshake got
	// through.
	if peer.ip == "" {
		peer.ip, peer.port, peer.cport = ip, port, port+10000
	}

	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if peer.id == cs.myID {
		return errMeetSelf
	}
	cs.putNodeLocked(peer)
	node := cs.nodes[peer.id]
	cs.mutateLocked(func(t *slotTable) {
		for _, r := range slots {
			for i := r[0]; i <= r[1]; i++ {
				if t.slots[i].owner == nil {
					t.slots[i].owner = node
				}
			}
		}
	})
	cs.saveErrLocked()
	return nil
}

const errMeetSelf = wireError("Can't meet myself")

// parseMeetReply extracts the peer's own line from its CLUSTER NODES output: the one
// flagged "myself". Only that line is used -- the peer's view of *third* nodes is
// deliberately not adopted, because that would be configuration propagation, which is
// exactly the half of Redis Cluster this server does not implement.
func parseMeetReply(table string) (*clusterNode, [][2]int, error) {
	for _, line := range strings.Split(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 || !strings.Contains(fields[2], "myself") {
			continue
		}
		ip, port, cport, err := parseNodeAddr(fields[1])
		if err != nil {
			return nil, nil, err
		}
		n := &clusterNode{id: fields[0], ip: ip, port: port, cport: cport}
		if fields[3] != "-" {
			n.replicaOf = fields[3]
		}
		var slots [][2]int
		for _, spec := range fields[8:] {
			if strings.HasPrefix(spec, "[") {
				continue
			}
			lo, hi, err := parseSlotRange(spec)
			if err != nil {
				return nil, nil, err
			}
			slots = append(slots, [2]int{lo, hi})
		}
		return n, slots, nil
	}
	return nil, nil, errNotACluster
}

const errNotACluster = wireError("the node did not identify itself; is it running with cluster mode enabled?")

// writeNodeCommand sends one command on a node-to-node connection and flushes it.
func writeNodeCommand(w *resp.Writer, parts ...string) error {
	if err := w.WriteCommand(cmdBytes(parts...)); err != nil {
		return err
	}
	return w.Flush()
}

func cmdBytes(parts ...string) [][]byte {
	out := make([][]byte, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}
