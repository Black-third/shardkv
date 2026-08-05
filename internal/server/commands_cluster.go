package server

// The CLUSTER command and the connection-level flags that go with it.
//
// The reply *formats* here are load-bearing in a way most replies are not. A client
// library builds its routing table by parsing CLUSTER SLOTS or CLUSTER SHARDS, and
// redis-cli --cluster and every cluster-aware driver parse CLUSTER NODES as text with
// positional fields. A field in the wrong position does not produce an error; it
// produces a client that routes to the wrong node, or that decides the cluster is
// down. So each renderer below states the exact shape it is producing, and the tests
// assert on the shape rather than only on the content.

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("CLUSTER", -2, false, cmdCluster)
	registerSession("ASKING", 1, cmdAsking)
	registerSession("READONLY", 1, cmdReadOnly)
	registerSession("READWRITE", 1, cmdReadWrite)
}

// errNotCluster is what every cluster command answers on a server started without
// -cluster-enabled. It is Redis's message, so a client that probes for cluster support
// by running CLUSTER INFO gets the answer it knows how to read.
const errNotCluster = "ERR This instance has cluster support disabled"

func logClusterSaveFailure(err error) {
	log.Printf("shardkv: could not persist the cluster configuration: %v", err)
}

func cmdCluster(s *Server, w *resp.Writer, args [][]byte) bool {
	sub := strings.ToUpper(string(args[1]))
	// KEYSLOT and HELP answer on any server: the first is a pure function of the key and
	// is genuinely useful for inspecting how a keyspace would shard, and the second must
	// work in order to say what the rest would need.
	switch sub {
	case "KEYSLOT":
		if len(args) != 3 {
			writeClusterArity(w, args[1])
			return false
		}
		w.WriteInt(int64(KeySlot(string(args[2]))))
		return false
	case "HELP":
		writeClusterHelp(w)
		return false
	}
	if !s.ClusterEnabled() {
		w.WriteError(errNotCluster)
		return false
	}
	return s.clusterSubcommand(w, sub, args)
}

// clusterSubcommand runs the subcommands that need cluster mode. It is split from
// cmdCluster so the two that do not need it stay obviously separate.
func (s *Server) clusterSubcommand(w *resp.Writer, sub string, args [][]byte) bool {
	cs := s.cluster
	switch sub {
	case "MYID":
		w.WriteBulk([]byte(cs.myself().id))

	case "INFO":
		w.WriteVerbatim("txt", []byte(s.clusterInfo()))

	case "NODES":
		w.WriteBulk([]byte(s.clusterNodesText()))

	case "SLOTS":
		s.writeClusterSlots(w)

	case "SHARDS":
		s.writeClusterShards(w)

	case "COUNTKEYSINSLOT":
		slot, ok := parseSlotArg(w, args, 2)
		if !ok {
			return false
		}
		w.WriteInt(int64(len(s.keysInSlot(slot, -1))))

	case "GETKEYSINSLOT":
		if len(args) != 4 {
			writeClusterArity(w, args[1])
			return false
		}
		slot, ok := parseSlotArg(w, args, 2)
		if !ok {
			return false
		}
		count, ok := parseInt(args[3])
		if !ok || count < 0 {
			w.WriteError("ERR Number of keys can't be negative")
			return false
		}
		writeStrings(w, s.keysInSlot(slot, count))

	case "ADDSLOTS", "DELSLOTS":
		return s.clusterAddDelSlots(w, sub == "ADDSLOTS", args)

	case "ADDSLOTSRANGE", "DELSLOTSRANGE":
		return s.clusterAddDelSlotsRange(w, sub == "ADDSLOTSRANGE", args)

	case "SETSLOT":
		return s.clusterSetSlot(w, args)

	case "MEET":
		return s.clusterMeet(w, args)

	case "FORGET":
		return s.clusterForget(w, args)

	case "REPLICATE":
		return s.clusterReplicate(w, args)

	case "RESET":
		return s.clusterReset(w, args)

	default:
		w.WriteError("ERR Unknown subcommand or wrong number of arguments for '" +
			string(args[1]) + "'. Try CLUSTER HELP.")
	}
	return false
}

func writeClusterArity(w *resp.Writer, sub []byte) {
	w.WriteError("ERR Unknown subcommand or wrong number of arguments for '" +
		string(sub) + "'. Try CLUSTER HELP.")
}

// parseSlotArg parses a slot operand, answering with Redis's message for a value that
// is not a number or is outside 0..16383.
func parseSlotArg(w *resp.Writer, args [][]byte, i int) (int, bool) {
	if i >= len(args) {
		writeClusterArity(w, args[1])
		return 0, false
	}
	n, ok := parseInt(args[i])
	if !ok || n < 0 || n >= numSlots {
		w.WriteError("ERR Invalid or out of range slot")
		return 0, false
	}
	return n, true
}

// --- slot assignment ----------------------------------------------------------

// clusterAddDelSlots implements CLUSTER ADDSLOTS/DELSLOTS.
//
// Every slot is validated before any is applied. Redis does the same, and the reason
// matters: these commands are how an operator lays out a cluster, and one that applied
// the first nine of ten slots and then reported an error would leave the node in a
// state neither the operator nor the node's own configuration file describes.
func (s *Server) clusterAddDelSlots(w *resp.Writer, add bool, args [][]byte) bool {
	if len(args) < 3 {
		writeClusterArity(w, args[1])
		return false
	}
	slots := make([]int, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		n, ok := parseSlotArg(w, args, i)
		if !ok {
			return false
		}
		slots = append(slots, n)
	}
	return s.applySlotChange(w, add, slots)
}

// clusterAddDelSlotsRange implements CLUSTER ADDSLOTSRANGE/DELSLOTSRANGE, which take
// start/end pairs. It is the form an operator actually uses: laying out three nodes
// means three commands rather than 16384 arguments.
func (s *Server) clusterAddDelSlotsRange(w *resp.Writer, add bool, args [][]byte) bool {
	if len(args) < 4 || (len(args)-2)%2 != 0 {
		writeClusterArity(w, args[1])
		return false
	}
	var slots []int
	for i := 2; i < len(args); i += 2 {
		start, ok := parseSlotArg(w, args, i)
		if !ok {
			return false
		}
		end, ok := parseSlotArg(w, args, i+1)
		if !ok {
			return false
		}
		if start > end {
			w.WriteError(fmt.Sprintf(
				"ERR start slot number %d is greater than end slot number %d", start, end))
			return false
		}
		for slot := start; slot <= end; slot++ {
			slots = append(slots, slot)
		}
	}
	return s.applySlotChange(w, add, slots)
}

// applySlotChange validates a whole batch of slots and then applies it, so the command
// is all-or-nothing.
func (s *Server) applySlotChange(w *resp.Writer, add bool, slots []int) bool {
	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()
	me := cs.nodes[cs.myID]

	seen := make(map[int]struct{}, len(slots))
	t := cs.table.Load()
	for _, slot := range slots {
		if _, dup := seen[slot]; dup {
			w.WriteError(fmt.Sprintf("ERR Slot %d specified multiple times", slot))
			return false
		}
		seen[slot] = struct{}{}
		switch {
		case add && t.slots[slot].owner != nil:
			w.WriteError(fmt.Sprintf("ERR Slot %d is already busy", slot))
			return false
		case !add && t.slots[slot].owner == nil:
			w.WriteError(fmt.Sprintf("ERR Slot %d is already unassigned", slot))
			return false
		}
	}
	cs.mutateLocked(func(t *slotTable) {
		for _, slot := range slots {
			if add {
				t.slots[slot].owner = me
			} else {
				t.slots[slot] = slotInfo{}
			}
		}
	})
	cs.currentEpoch++
	cs.saveErrLocked()
	w.WriteSimple("OK")
	return false
}

// clusterSetSlot implements
// CLUSTER SETSLOT <slot> IMPORTING <id> | MIGRATING <id> | STABLE | NODE <id>.
//
// The four forms are the whole of a slot migration, and they are issued in a fixed
// order that the ASK redirect depends on:
//
//	target: SETSLOT <slot> IMPORTING <source-id>   -- accept ASKING clients for it
//	source: SETSLOT <slot> MIGRATING <target-id>   -- ASK-redirect keys it no longer has
//	source: MIGRATE ... (one batch of keys at a time)
//	both:   SETSLOT <slot> NODE <target-id>        -- the assignment itself moves
//
// Between the second and the fourth step the slot is *open*: the source still owns it
// and serves the keys it still holds, while every key it has already handed over draws
// an ASK to the target. That is why MIGRATING and IMPORTING are slot state rather than
// a flag on the command -- the redirect has to be decided per key, from the slot, on
// every request.
func (s *Server) clusterSetSlot(w *resp.Writer, args [][]byte) bool {
	if len(args) < 4 {
		writeClusterArity(w, args[1])
		return false
	}
	slot, ok := parseSlotArg(w, args, 2)
	if !ok {
		return false
	}
	action := strings.ToUpper(string(args[3]))
	if action == "STABLE" {
		if len(args) != 4 {
			writeClusterArity(w, args[1])
			return false
		}
		s.cluster.mu.Lock()
		s.cluster.mutateLocked(func(t *slotTable) {
			t.slots[slot].migrating, t.slots[slot].importing = nil, nil
		})
		s.cluster.saveErrLocked()
		s.cluster.mu.Unlock()
		w.WriteSimple("OK")
		return false
	}
	if len(args) != 5 {
		writeClusterArity(w, args[1])
		return false
	}
	id := string(args[4])

	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()
	target := cs.nodes[id]
	if target == nil {
		w.WriteError("ERR Unknown node " + id)
		return false
	}
	me := cs.nodes[cs.myID]
	info := cs.table.Load().slots[slot]

	switch action {
	case "MIGRATING":
		if info.owner != me {
			w.WriteError(fmt.Sprintf("ERR I'm not the owner of hash slot %d", slot))
			return false
		}
		if target == me {
			w.WriteError("ERR Target node is myself")
			return false
		}
		cs.mutateLocked(func(t *slotTable) { t.slots[slot].migrating = target })

	case "IMPORTING":
		if info.owner == me {
			w.WriteError(fmt.Sprintf("ERR I'm already the owner of hash slot %d", slot))
			return false
		}
		if target == me {
			w.WriteError("ERR Target node is myself")
			return false
		}
		cs.mutateLocked(func(t *slotTable) { t.slots[slot].importing = target })

	case "NODE":
		// Handing a slot away while still holding keys for it would strand them: this node
		// would redirect every request for those keys elsewhere, and nothing would ever
		// read them again. Redis refuses for the same reason, and the refusal is what makes
		// a resharding script's "migrate until COUNTKEYSINSLOT is zero" loop meaningful.
		if info.owner == me && target != me && len(s.keysInSlot(slot, 1)) > 0 {
			w.WriteError(fmt.Sprintf("ERR Can't assign hashslot %d to a different node "+
				"while I still hold keys for this hash slot.", slot))
			return false
		}
		cs.mutateLocked(func(t *slotTable) {
			// The assignment closes the migration: whichever side this node was on, the slot
			// is now settled and further requests are answered or MOVED, never ASKed.
			t.slots[slot] = slotInfo{owner: target}
		})
		cs.currentEpoch++

	default:
		w.WriteError("ERR Invalid CLUSTER SETSLOT action or number of arguments. " +
			"Try CLUSTER HELP")
		return false
	}
	cs.saveErrLocked()
	w.WriteSimple("OK")
	return false
}

// --- node membership ----------------------------------------------------------

func (s *Server) clusterMeet(w *resp.Writer, args [][]byte) bool {
	if len(args) != 4 && len(args) != 5 {
		writeClusterArity(w, args[1])
		return false
	}
	port, ok := parseInt(args[3])
	if !ok || port <= 0 || port > 65535 {
		w.WriteError("ERR Invalid base port specified")
		return false
	}
	if err := s.meet(string(args[2]), port); err != nil {
		w.WriteError("ERR " + err.Error())
		return false
	}
	w.WriteSimple("OK")
	return false
}

func (s *Server) clusterForget(w *resp.Writer, args [][]byte) bool {
	if len(args) != 3 {
		writeClusterArity(w, args[1])
		return false
	}
	id := string(args[2])
	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if id == cs.myID {
		w.WriteError("ERR I tried hard but I can't forget myself...")
		return false
	}
	node := cs.nodes[id]
	if node == nil {
		w.WriteError("ERR Unknown node " + id)
		return false
	}
	// Forgetting a node that still owns slots would leave those slots pointing at a node
	// this server can no longer name, so they are released. The cluster then reports
	// itself down for them, which is the truth: nobody here knows who serves them.
	cs.mutateLocked(func(t *slotTable) {
		for i := range t.slots {
			if t.slots[i].owner == node {
				t.slots[i].owner = nil
			}
			if t.slots[i].migrating == node {
				t.slots[i].migrating = nil
			}
			if t.slots[i].importing == node {
				t.slots[i].importing = nil
			}
		}
	})
	delete(cs.nodes, id)
	cs.saveErrLocked()
	w.WriteSimple("OK")
	return false
}

// clusterReplicate implements CLUSTER REPLICATE <node-id>: this node becomes a replica
// of that one.
//
// It is wired to the server's real replication rather than being a bookkeeping change,
// because this server has real replication: the node starts a PSYNC against the
// master's client port exactly as REPLICAOF would, so the replica actually carries the
// master's data and can serve reads for its slots (see READONLY).
func (s *Server) clusterReplicate(w *resp.Writer, args [][]byte) bool {
	if len(args) != 3 {
		writeClusterArity(w, args[1])
		return false
	}
	id := string(args[2])
	cs := s.cluster
	cs.mu.Lock()
	if id == cs.myID {
		cs.mu.Unlock()
		w.WriteError("ERR Can't replicate myself")
		return false
	}
	master := cs.nodes[id]
	if master == nil {
		cs.mu.Unlock()
		w.WriteError("ERR Unknown node " + id)
		return false
	}
	if master.isReplica() {
		cs.mu.Unlock()
		w.WriteError("ERR I can only replicate a master, not a replica.")
		return false
	}
	me := cs.nodes[cs.myID]
	if len(cs.ownedSlots(me)) > 0 || s.store.Len() > 0 {
		cs.mu.Unlock()
		w.WriteError("ERR To set a master the node must be empty and " +
			"without assigned slots.")
		return false
	}
	updated := *me
	updated.replicaOf = id
	cs.putNodeLocked(&updated)
	cs.saveErrLocked()
	addr := master.addr()
	cs.mu.Unlock()

	s.ReplicaOf(s.baseCtx, addr)
	w.WriteSimple("OK")
	return false
}

// clusterReset implements CLUSTER RESET [HARD|SOFT].
//
// Both forms release every slot and forget every other node. HARD also mints a new
// node id and zeroes the epoch, which is how a node that is being repurposed stops
// being recognized as its former self by anything that remembers it.
//
// It refuses on a node that still holds data, as Redis does: resetting a node with keys
// would leave those keys in a keyspace no slot maps to, unreachable and unmentioned.
func (s *Server) clusterReset(w *resp.Writer, args [][]byte) bool {
	hard := false
	switch {
	case len(args) == 2:
	case len(args) == 3 && strings.EqualFold(string(args[2]), "HARD"):
		hard = true
	case len(args) == 3 && strings.EqualFold(string(args[2]), "SOFT"):
	default:
		writeClusterArity(w, args[1])
		return false
	}
	if s.store.Len() > 0 {
		w.WriteError("ERR CLUSTER RESET can't be called with master nodes containing keys")
		return false
	}
	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()
	me := cs.nodes[cs.myID]
	reset := &clusterNode{
		id: me.id, ip: me.ip, port: me.port, cport: me.cport,
		myself: true, configEpoch: me.configEpoch,
	}
	if hard {
		reset.id = newReplID()
		reset.configEpoch = 0
		cs.currentEpoch = 0
	}
	cs.nodes = map[string]*clusterNode{reset.id: reset}
	cs.myID = reset.id
	cs.table.Store(&slotTable{})
	cs.saveErrLocked()
	w.WriteSimple("OK")
	return false
}

// --- keys in a slot -----------------------------------------------------------

// keysInSlot returns up to limit local keys belonging to a slot (limit < 0 means all),
// sorted so repeated calls during a migration are stable.
//
// It scans the keyspace rather than consulting an index. Redis keeps a slot-to-keys
// index maintained on every insert and delete; this server deliberately does not,
// because that index would put slot arithmetic on the write path of every command in
// order to speed up two administrative ones. The cost is stated in the README rather
// than hidden: COUNTKEYSINSLOT and GETKEYSINSLOT are O(keys in this node's keyspace).
func (s *Server) keysInSlot(slot, limit int) []string {
	if limit == 0 {
		return nil
	}
	var out []string
	for _, k := range s.store.Keys() {
		if KeySlot(k) == slot {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// --- reply formats ------------------------------------------------------------

// clusterInfo renders CLUSTER INFO's field:value report.
//
// cluster_state is the field a client acts on: "ok" means every slot has an owner, and
// "fail" means at least one does not, which is when a client should expect CLUSTERDOWN
// for some keys. The message-count fields are reported as zero and stay zero -- there
// is no cluster bus to count messages on, and inventing plausible numbers for a
// protocol that is not running would be worse than a zero an operator can interpret.
func (s *Server) clusterInfo() string {
	cs := s.cluster
	t := cs.table.Load()
	assigned := t.count()

	cs.mu.Lock()
	known := len(cs.nodes)
	epoch := cs.currentEpoch
	masters := 0
	for _, n := range cs.nodes {
		if !n.isReplica() && len(cs.ownedSlots(n)) > 0 {
			masters++
		}
	}
	myEpoch := cs.nodes[cs.myID].configEpoch
	cs.mu.Unlock()

	state := "ok"
	if assigned < numSlots {
		state = "fail"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "cluster_enabled:1\r\n")
	fmt.Fprintf(&b, "cluster_state:%s\r\n", state)
	fmt.Fprintf(&b, "cluster_slots_assigned:%d\r\n", assigned)
	fmt.Fprintf(&b, "cluster_slots_ok:%d\r\n", assigned)
	fmt.Fprintf(&b, "cluster_slots_pfail:0\r\n")
	fmt.Fprintf(&b, "cluster_slots_fail:0\r\n")
	fmt.Fprintf(&b, "cluster_known_nodes:%d\r\n", known)
	fmt.Fprintf(&b, "cluster_size:%d\r\n", masters)
	fmt.Fprintf(&b, "cluster_current_epoch:%d\r\n", epoch)
	fmt.Fprintf(&b, "cluster_my_epoch:%d\r\n", myEpoch)
	// No cluster bus, so no messages. See the doc comment.
	fmt.Fprintf(&b, "cluster_stats_messages_sent:0\r\n")
	fmt.Fprintf(&b, "cluster_stats_messages_received:0\r\n")
	fmt.Fprintf(&b, "total_cluster_links_buffer_limit_exceeded:0\r\n")
	return b.String()
}

// nodeLineLocked renders one CLUSTER NODES line. The format is positional and parsed
// by every cluster-aware client and by redis-cli --cluster:
//
//	<id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot>...
//
// A node's own line additionally carries its open slots as migration markers --
// [slot->-<target>] while migrating away and [slot-<-<source>] while importing -- which
// is how redis-cli --cluster check discovers a resharding that was interrupted.
//
// The ping/pong timestamps are zero: they are the gossip bus's bookkeeping, and this
// server has no bus. Reporting a fabricated round-trip time would be inventing evidence
// of liveness checks that never happened.
func (cs *clusterState) nodeLineLocked(n *clusterNode) string {
	flags := make([]string, 0, 2)
	if n.myself {
		flags = append(flags, "myself")
	}
	if n.isReplica() {
		flags = append(flags, "slave")
	} else {
		flags = append(flags, "master")
	}
	master := "-"
	if n.replicaOf != "" {
		master = n.replicaOf
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s:%d@%d %s %s 0 0 %d connected",
		n.id, n.ip, n.port, n.cport, strings.Join(flags, ","), master, n.configEpoch)
	for _, r := range cs.ownedSlots(n) {
		if r[0] == r[1] {
			fmt.Fprintf(&b, " %d", r[0])
		} else {
			fmt.Fprintf(&b, " %d-%d", r[0], r[1])
		}
	}
	if n.myself {
		t := cs.table.Load()
		for i := 0; i < numSlots; i++ {
			if to := t.slots[i].migrating; to != nil {
				fmt.Fprintf(&b, " [%d->-%s]", i, to.id)
			}
			if from := t.slots[i].importing; from != nil {
				fmt.Fprintf(&b, " [%d-<-%s]", i, from.id)
			}
		}
	}
	return b.String()
}

func (s *Server) clusterNodesText() string {
	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()
	var b strings.Builder
	for _, n := range cs.sortedNodesLocked() {
		b.WriteString(cs.nodeLineLocked(n))
		b.WriteByte('\n')
	}
	return b.String()
}

// writeClusterSlots renders CLUSTER SLOTS: one entry per contiguous run of slots with
// the same owner, each carrying the master and then its replicas.
//
//  1. 1) (integer) start
//  2. (integer) end
//  3. 1) "ip"          <- master
//  2. (integer) port
//  3. "node id"
//  4. (empty array) <- additional networking metadata
//  4. ...              <- one more of the same shape per replica
//
// The fourth element of a node entry is Redis 7's metadata array. It is emitted empty
// rather than omitted: a client that indexes into it would panic on a three-element
// entry, and a client that does not look at it is unaffected either way.
func (s *Server) writeClusterSlots(w *resp.Writer) {
	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()

	replicas := cs.replicasByMasterLocked()
	runs := cs.slotRunsLocked()
	w.WriteArrayHeader(len(runs))
	for _, run := range runs {
		owner := run.owner
		w.WriteArrayHeader(3 + len(replicas[owner.id]))
		w.WriteInt(int64(run.start))
		w.WriteInt(int64(run.end))
		writeClusterSlotNode(w, owner)
		for _, r := range replicas[owner.id] {
			writeClusterSlotNode(w, r)
		}
	}
}

func writeClusterSlotNode(w *resp.Writer, n *clusterNode) {
	w.WriteArrayHeader(4)
	w.WriteBulk([]byte(n.ip))
	w.WriteInt(int64(n.port))
	w.WriteBulk([]byte(n.id))
	w.WriteArrayHeader(0)
}

// slotRun is one contiguous run of slots with a single owner.
type slotRun struct {
	start, end int
	owner      *clusterNode
}

// slotRunsLocked collapses the slot map into runs, which is the shape both CLUSTER
// SLOTS and CLUSTER SHARDS report. Unassigned slots are gaps: a client must not be told
// a node serves a slot nobody claimed.
func (cs *clusterState) slotRunsLocked() []slotRun {
	t := cs.table.Load()
	var runs []slotRun
	var cur *slotRun
	for i := 0; i < numSlots; i++ {
		owner := t.slots[i].owner
		if cur != nil && cur.owner == owner {
			cur.end = i
			continue
		}
		if cur != nil {
			runs = append(runs, *cur)
			cur = nil
		}
		if owner != nil {
			cur = &slotRun{start: i, end: i, owner: owner}
		}
	}
	if cur != nil {
		runs = append(runs, *cur)
	}
	return runs
}

// replicasByMasterLocked indexes the known replicas by the id of the master they
// follow, sorted for a stable reply.
func (cs *clusterState) replicasByMasterLocked() map[string][]*clusterNode {
	out := map[string][]*clusterNode{}
	for _, n := range cs.sortedNodesLocked() {
		if n.isReplica() {
			out[n.replicaOf] = append(out[n.replicaOf], n)
		}
	}
	return out
}

// writeClusterShards renders CLUSTER SHARDS, Redis 7's replacement for CLUSTER SLOTS:
// one entry per shard (a master and its replicas), carrying the slot ranges the shard
// owns and a map of attributes per node.
//
//  1. "slots"
//  2. 1) (integer) start   <- flat start/end pairs, not nested
//  2. (integer) end
//  3. "nodes"
//  4. 1) 1) "id" ... "endpoint" ... "role" ... "health" ...
//
// health is reported as "online" for every known node, and honestly so: with no gossip
// bus there is no failure detection, so this node genuinely does not know of any node
// being down. Saying "online" is the same claim CLUSTER INFO's zeroed message counters
// make -- that liveness is not tracked here -- rather than a claim that a check passed.
func (s *Server) writeClusterShards(w *resp.Writer) {
	cs := s.cluster
	cs.mu.Lock()
	defer cs.mu.Unlock()

	replicas := cs.replicasByMasterLocked()
	var masters []*clusterNode
	for _, n := range cs.sortedNodesLocked() {
		if !n.isReplica() && len(cs.ownedSlots(n)) > 0 {
			masters = append(masters, n)
		}
	}
	w.WriteArrayHeader(len(masters))
	for _, m := range masters {
		ranges := cs.ownedSlots(m)
		w.WriteMapHeader(2)
		w.WriteBulk([]byte("slots"))
		w.WriteArrayHeader(len(ranges) * 2)
		for _, r := range ranges {
			w.WriteInt(int64(r[0]))
			w.WriteInt(int64(r[1]))
		}
		w.WriteBulk([]byte("nodes"))
		w.WriteArrayHeader(1 + len(replicas[m.id]))
		writeShardNode(w, m, "master")
		for _, r := range replicas[m.id] {
			writeShardNode(w, r, "replica")
		}
	}
}

func writeShardNode(w *resp.Writer, n *clusterNode, role string) {
	w.WriteMapHeader(7)
	w.WriteBulk([]byte("id"))
	w.WriteBulk([]byte(n.id))
	w.WriteBulk([]byte("port"))
	w.WriteInt(int64(n.port))
	w.WriteBulk([]byte("ip"))
	w.WriteBulk([]byte(n.ip))
	w.WriteBulk([]byte("endpoint"))
	w.WriteBulk([]byte(n.ip))
	w.WriteBulk([]byte("role"))
	w.WriteBulk([]byte(role))
	w.WriteBulk([]byte("replication-offset"))
	w.WriteInt(0)
	w.WriteBulk([]byte("health"))
	w.WriteBulk([]byte("online"))
}

func writeClusterHelp(w *resp.Writer) {
	writeHelp(w, "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", []string{
		"ADDSLOTS <slot> [<slot> ...]",
		"    Assign slots to the current node.",
		"ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
		"    Assign slot ranges to the current node.",
		"COUNTKEYSINSLOT <slot>",
		"    Return the number of keys in <slot>.",
		"DELSLOTS <slot> [<slot> ...]",
		"    Delete slots from the current node.",
		"DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
		"    Delete slot ranges from the current node.",
		"FORGET <node-id>",
		"    Remove a node from the cluster.",
		"GETKEYSINSLOT <slot> <count>",
		"    Return <count> keys in <slot>.",
		"INFO",
		"    Return information about the cluster.",
		"KEYSLOT <key>",
		"    Return the hash slot for <key>.",
		"MEET <ip> <port> [<bus-port>]",
		"    Connect nodes into a working cluster.",
		"MYID",
		"    Return the node id.",
		"NODES",
		"    Return cluster configuration seen by node.",
		"REPLICATE <node-id>",
		"    Configure current node as replica to <node-id>.",
		"RESET [HARD|SOFT]",
		"    Reset current node (default: soft).",
		"SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)",
		"    Set slot state.",
		"SHARDS",
		"    Return information about slot range mappings and the nodes in them.",
		"SLOTS",
		"    Return information about slots range mappings.",
	})
}

// --- connection flags ---------------------------------------------------------

// cmdAsking implements ASKING, the one-shot flag that makes the *next* command
// acceptable on a node that is importing the key's slot.
//
// It exists because an importing node does not own the slot yet: without the flag it
// would answer MOVED and send the client straight back to the source, which had just
// sent it here. The flag is one-shot because ownership has not actually changed --
// letting it persist would make the importing node serve the slot indefinitely, which
// is precisely the split-brain the migration protocol is designed to avoid.
//
// See clearAsking for exactly when it is consumed.
func cmdAsking(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if !s.ClusterEnabled() {
		w.WriteError(errNotCluster)
		return
	}
	sess.asking = true
	w.WriteSimple("OK")
}

// cmdReadOnly and cmdReadWrite implement the flag that lets a client read from a
// replica of the node that owns a slot, instead of being redirected to the master.
//
// A replica is refused writes anyway (errReadOnly), so the flag only ever opens up
// reads, and only for slots this node's master owns.
func cmdReadOnly(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if !s.ClusterEnabled() {
		w.WriteError(errNotCluster)
		return
	}
	sess.readReplica = true
	w.WriteSimple("OK")
}

func cmdReadWrite(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if !s.ClusterEnabled() {
		w.WriteError(errNotCluster)
		return
	}
	sess.readReplica = false
	w.WriteSimple("OK")
}

// clusterInfoLine is what INFO's Cluster section reports. Redis publishes
// cluster_enabled there on every server, cluster or not, and a monitoring agent uses it
// to decide whether the cluster fields are worth polling.
func (s *Server) infoCluster(b *strings.Builder) {
	b.WriteString("cluster_enabled:" + strconv.Itoa(boolToInt(s.ClusterEnabled())) + "\r\n")
}
