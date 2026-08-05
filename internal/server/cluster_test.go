package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

// newClusterServer returns a cluster-enabled server that is not listening. The
// announce address is given explicitly, which is what a node that is reached through a
// port mapping needs anyway, and here it keeps the tests independent of a real socket.
func newClusterServer(t *testing.T, port int) (*Server, *directClient) {
	t.Helper()
	s := New(store.New(8))
	opts := ClusterOptions{AnnounceIP: "127.0.0.1", AnnouncePort: port}
	if err := s.EnableCluster(opts); err != nil {
		t.Fatalf("EnableCluster: %v", err)
	}
	return s, &directClient{t: t, s: s}
}

// startClusterNode starts a listening cluster-enabled node and returns it with its
// address. It is what the MEET tests need: MEET is a real client connection to a real
// peer, not a bookkeeping change.
func startClusterNode(t *testing.T, configFile string) (*Server, string) {
	t.Helper()
	s := New(store.New(8))
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if err := s.EnableCluster(ClusterOptions{ConfigFile: configFile}); err != nil {
		t.Fatalf("EnableCluster: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("cluster node did not shut down in time")
		}
	})
	return s, s.Addr().String()
}

// TestClusterDisabledByDefault is the compatibility guarantee for every existing
// deployment: a server started without -cluster-enabled behaves exactly as it did, and
// says so in the two places a client looks.
func TestClusterDisabledByDefault(t *testing.T) {
	s := New(store.New(8))
	c := &directClient{t: t, s: s}
	if s.ClusterEnabled() {
		t.Fatal("cluster mode is on by default")
	}
	for _, sub := range []string{
		"INFO", "MYID", "NODES", "SLOTS", "SHARDS", "ADDSLOTS 0", "DELSLOTS 0",
		"ADDSLOTSRANGE 0 1", "DELSLOTSRANGE 0 1", "SETSLOT 0 STABLE", "FORGET x",
		"MEET 127.0.0.1 1", "REPLICATE x", "RESET", "COUNTKEYSINSLOT 0",
		"GETKEYSINSLOT 0 1",
	} {
		if got := c.cmd("CLUSTER " + sub); got != "-"+errNotCluster {
			t.Errorf("CLUSTER %s on a standalone server = %q; want %q", sub, got, "-"+errNotCluster)
		}
	}
	// KEYSLOT is a pure function of the key, so it answers anywhere -- it is how an
	// operator inspects how a keyspace *would* shard before committing to a cluster.
	if got := c.cmd("CLUSTER KEYSLOT foo"); got != ":12182" {
		t.Errorf("CLUSTER KEYSLOT on a standalone server = %q", got)
	}
	if got := c.cmd("CLUSTER HELP"); !strings.Contains(got, "ADDSLOTS") {
		t.Errorf("CLUSTER HELP = %q", got)
	}
}

// TestClusterSlotAssignment covers ADDSLOTS/DELSLOTS and their range forms, including
// the all-or-nothing property: a batch with one bad slot in it changes nothing.
func TestClusterSlotAssignment(t *testing.T) {
	s, c := newClusterServer(t, 7001)

	if got := c.cmd("CLUSTER ADDSLOTS 0 1 2"); got != "+OK" {
		t.Fatalf("ADDSLOTS = %q", got)
	}
	if got := c.cmd("CLUSTER ADDSLOTS 2"); got != "-ERR Slot 2 is already busy" {
		t.Errorf("re-adding an owned slot = %q", got)
	}
	if got := c.cmd("CLUSTER ADDSLOTS 5 5"); got != "-ERR Slot 5 specified multiple times" {
		t.Errorf("duplicate slot = %q", got)
	}
	// The batch that failed must have changed nothing.
	if got := s.cluster.slot(5).owner; got != nil {
		t.Error("a rejected ADDSLOTS assigned a slot anyway")
	}
	for _, tc := range []struct{ cmd, want string }{
		{"CLUSTER ADDSLOTS 16384", "-ERR Invalid or out of range slot"},
		{"CLUSTER ADDSLOTS -1", "-ERR Invalid or out of range slot"},
		{"CLUSTER ADDSLOTS notanumber", "-ERR Invalid or out of range slot"},
		{"CLUSTER DELSLOTS 100", "-ERR Slot 100 is already unassigned"},
		{"CLUSTER ADDSLOTSRANGE 10 5", "-ERR start slot number 10 is greater than end slot number 5"},
	} {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%s -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	if got := c.cmd("CLUSTER ADDSLOTSRANGE 100 200 300 400"); got != "+OK" {
		t.Fatalf("ADDSLOTSRANGE = %q", got)
	}
	if s.cluster.table.Load().count() != 3+101+101 {
		t.Errorf("assigned %d slots; want 205", s.cluster.table.Load().count())
	}
	if got := c.cmd("CLUSTER DELSLOTSRANGE 100 200"); got != "+OK" {
		t.Fatalf("DELSLOTSRANGE = %q", got)
	}
	if s.cluster.table.Load().count() != 3+101 {
		t.Errorf("after DELSLOTSRANGE, %d slots assigned; want 104",
			s.cluster.table.Load().count())
	}
	if got := c.cmd("CLUSTER DELSLOTS 0 1 2"); got != "+OK" {
		t.Errorf("DELSLOTS = %q", got)
	}
}

// TestClusterInfoAndState pins the two fields a client acts on: cluster_state, which
// says whether every slot has an owner, and cluster_known_nodes.
func TestClusterInfoAndState(t *testing.T) {
	_, c := newClusterServer(t, 7001)

	info := c.cmd("CLUSTER INFO")
	if !strings.Contains(info, "cluster_enabled:1") {
		t.Errorf("CLUSTER INFO = %q", info)
	}
	if !strings.Contains(info, "cluster_state:fail") {
		t.Errorf("a cluster with no slots assigned reports %q; want state fail", info)
	}
	if !strings.Contains(info, "cluster_slots_assigned:0") {
		t.Errorf("CLUSTER INFO = %q", info)
	}
	// Owning every slot is the single-node cluster, and it is "ok".
	c.cmd("CLUSTER ADDSLOTSRANGE 0 16383")
	info = c.cmd("CLUSTER INFO")
	if !strings.Contains(info, "cluster_state:ok") {
		t.Errorf("a fully covered cluster reports %q; want state ok", info)
	}
	if !strings.Contains(info, "cluster_slots_assigned:16384") {
		t.Errorf("CLUSTER INFO = %q", info)
	}
	if !strings.Contains(info, "cluster_size:1") {
		t.Errorf("CLUSTER INFO = %q", info)
	}
	if !strings.Contains(c.cmd("INFO cluster"), "cluster_enabled:1") {
		t.Error("INFO cluster does not report cluster_enabled on a cluster node")
	}
}

// TestClusterNodesFormat pins the CLUSTER NODES line, which is positional text that
// redis-cli --cluster and every cluster-aware driver parse by field index. A field in
// the wrong place is not an error, it is a client that routes wrongly.
func TestClusterNodesFormat(t *testing.T) {
	s, c := newClusterServer(t, 7001)
	c.cmd("CLUSTER ADDSLOTSRANGE 0 100")
	c.cmd("CLUSTER ADDSLOTS 500")

	line := strings.TrimSpace(c.cmd("CLUSTER NODES"))
	fields := strings.Fields(line)
	if len(fields) < 10 {
		t.Fatalf("CLUSTER NODES line has %d fields: %q", len(fields), line)
	}
	myID := s.cluster.myself().id
	if fields[0] != myID {
		t.Errorf("field 0 (id) = %q; want %q", fields[0], myID)
	}
	if len(myID) != 40 {
		t.Errorf("node id %q is %d characters; Redis's are 40 hex characters", myID, len(myID))
	}
	if fields[1] != "127.0.0.1:7001@17001" {
		t.Errorf("field 1 (addr) = %q; want ip:port@cport", fields[1])
	}
	if fields[2] != "myself,master" {
		t.Errorf("field 2 (flags) = %q", fields[2])
	}
	if fields[3] != "-" {
		t.Errorf("field 3 (master) = %q; want - for a master", fields[3])
	}
	if fields[7] != "connected" {
		t.Errorf("field 7 (link-state) = %q", fields[7])
	}
	// Slots follow, with contiguous runs collapsed into ranges and singletons bare.
	if fields[8] != "0-100" || fields[9] != "500" {
		t.Errorf("slots = %v; want [0-100 500]", fields[8:])
	}

	// An open slot appears as a migration marker on the owner's own line, which is how a
	// resharding that was interrupted is discoverable.
	other := &clusterNode{id: strings.Repeat("b", 40), ip: "127.0.0.1", port: 7002, cport: 17002}
	s.cluster.mu.Lock()
	s.cluster.putNodeLocked(other)
	s.cluster.mu.Unlock()
	if got := c.cmd("CLUSTER SETSLOT 50 MIGRATING " + other.id); got != "+OK" {
		t.Fatalf("SETSLOT MIGRATING = %q", got)
	}
	if got := c.cmd("CLUSTER NODES"); !strings.Contains(got, "[50->-"+other.id+"]") {
		t.Errorf("CLUSTER NODES during a migration = %q; want a [50->-<id>] marker", got)
	}
	if got := c.cmd("CLUSTER SETSLOT 200 IMPORTING " + other.id); got != "+OK" {
		t.Fatalf("SETSLOT IMPORTING = %q", got)
	}
	if got := c.cmd("CLUSTER NODES"); !strings.Contains(got, "[200-<-"+other.id+"]") {
		t.Errorf("CLUSTER NODES while importing = %q; want a [200-<-<id>] marker", got)
	}
}

// TestClusterSlotsAndShardsFormat pins the two replies a client library builds its
// routing table from.
func TestClusterSlotsAndShardsFormat(t *testing.T) {
	s, c := newClusterServer(t, 7001)
	c.cmd("CLUSTER ADDSLOTSRANGE 0 10 100 110")
	myID := s.cluster.myself().id

	// CLUSTER SLOTS: one entry per contiguous run, each [start, end, [ip port id []]].
	want := "[[:0 :10 [127.0.0.1 :7001 " + myID + " []]] [:100 :110 [127.0.0.1 :7001 " + myID + " []]]]"
	if got := c.cmd("CLUSTER SLOTS"); got != want {
		t.Errorf("CLUSTER SLOTS =\n %s\nwant\n %s", got, want)
	}

	// CLUSTER SHARDS: one entry per shard, slots as flat start/end pairs.
	shards := c.cmd("CLUSTER SHARDS")
	for _, needle := range []string{
		"slots [:0 :10 :100 :110]", "role master", "health online",
		"id " + myID, "port :7001", "endpoint 127.0.0.1",
	} {
		if !strings.Contains(shards, needle) {
			t.Errorf("CLUSTER SHARDS = %s\nmissing %q", shards, needle)
		}
	}

	// A node with no slots is not a shard: reporting it would tell a client to route
	// nothing to it, which is at best noise and at worst a division by zero.
	if got := c.cmd("CLUSTER DELSLOTSRANGE 0 10 100 110"); got != "+OK" {
		t.Fatalf("DELSLOTSRANGE = %q", got)
	}
	if got := c.cmd("CLUSTER SHARDS"); got != "[]" {
		t.Errorf("CLUSTER SHARDS with no slots owned = %q; want an empty array", got)
	}
	if got := c.cmd("CLUSTER SLOTS"); got != "[]" {
		t.Errorf("CLUSTER SLOTS with no slots owned = %q; want an empty array", got)
	}
}

// TestClusterKeysInSlot covers COUNTKEYSINSLOT and GETKEYSINSLOT, which are what a
// resharding loop drives MIGRATE from.
func TestClusterKeysInSlot(t *testing.T) {
	_, c := newClusterServer(t, 7001)
	c.cmd("CLUSTER ADDSLOTSRANGE 0 16383")

	// Three keys sharing a tag share a slot, which is the whole point of tags.
	slot := KeySlot("{shard}")
	for _, k := range []string{"{shard}:a", "{shard}:b", "{shard}:c"} {
		c.cmd("SET " + k + " v")
	}
	c.cmd("SET elsewhere v")

	if got := c.cmd("CLUSTER COUNTKEYSINSLOT " + itoa(slot)); got != ":3" {
		t.Errorf("COUNTKEYSINSLOT %d = %q; want 3", slot, got)
	}
	if got := c.cmd("CLUSTER GETKEYSINSLOT " + itoa(slot) + " 10"); got != "[{shard}:a {shard}:b {shard}:c]" {
		t.Errorf("GETKEYSINSLOT = %q", got)
	}
	// The count operand bounds the reply, which is how a migration moves keys in batches.
	if got := c.cmd("CLUSTER GETKEYSINSLOT " + itoa(slot) + " 2"); got != "[{shard}:a {shard}:b]" {
		t.Errorf("GETKEYSINSLOT with a count of 2 = %q", got)
	}
	if got := c.cmd("CLUSTER COUNTKEYSINSLOT 16384"); got != "-ERR Invalid or out of range slot" {
		t.Errorf("COUNTKEYSINSLOT out of range = %q", got)
	}
	if got := c.cmd("CLUSTER GETKEYSINSLOT " + itoa(slot) + " -1"); got != "-ERR Number of keys can't be negative" {
		t.Errorf("GETKEYSINSLOT with a negative count = %q", got)
	}
}

// TestClusterSetSlotGuards covers the refusals that keep a migration from stranding
// data: a slot cannot be migrated away by a node that does not own it, cannot be
// imported by the node that already owns it, and cannot be reassigned while this node
// still holds keys for it.
func TestClusterSetSlotGuards(t *testing.T) {
	s, c := newClusterServer(t, 7001)
	other := &clusterNode{id: strings.Repeat("c", 40), ip: "127.0.0.1", port: 7002, cport: 17002}
	s.cluster.mu.Lock()
	s.cluster.putNodeLocked(other)
	s.cluster.mu.Unlock()
	c.cmd("CLUSTER ADDSLOTSRANGE 0 16383")

	myID := s.cluster.myself().id
	for _, tc := range []struct{ cmd, want string }{
		{"CLUSTER SETSLOT 0 MIGRATING " + strings.Repeat("z", 40),
			"-ERR Unknown node " + strings.Repeat("z", 40)},
		{"CLUSTER SETSLOT 0 IMPORTING " + other.id,
			"-ERR I'm already the owner of hash slot 0"},
		{"CLUSTER SETSLOT 0 MIGRATING " + myID, "-ERR Target node is myself"},
		{"CLUSTER SETSLOT 0 BOGUS " + other.id,
			"-ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP"},
	} {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%s -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A slot this node still holds keys for cannot be handed away: the keys would become
	// unreachable, since every request for them would be redirected elsewhere.
	slot := KeySlot("stranded")
	c.cmd("SET stranded v")
	want := "-ERR Can't assign hashslot " + itoa(slot) +
		" to a different node while I still hold keys for this hash slot."
	if got := c.cmd("CLUSTER SETSLOT " + itoa(slot) + " NODE " + other.id); got != want {
		t.Errorf("SETSLOT NODE with keys still held = %q; want %q", got, want)
	}
	c.cmd("DEL stranded")
	if got := c.cmd("CLUSTER SETSLOT " + itoa(slot) + " NODE " + other.id); got != "+OK" {
		t.Errorf("SETSLOT NODE after the keys were moved = %q", got)
	}
	if owner := s.cluster.slot(slot).owner; owner != other {
		t.Error("SETSLOT NODE did not move the slot's ownership")
	}

	// A node that is not the owner may still take a slot it is importing, and the
	// assignment clears the migration state on both sides.
	if got := c.cmd("CLUSTER SETSLOT 1 MIGRATING " + other.id); got != "+OK" {
		t.Fatalf("SETSLOT MIGRATING = %q", got)
	}
	if s.cluster.slot(1).migrating == nil {
		t.Fatal("SETSLOT MIGRATING did not record the migration")
	}
	c.cmd("CLUSTER SETSLOT 1 NODE " + other.id)
	if info := s.cluster.slot(1); info.migrating != nil || info.importing != nil {
		t.Error("SETSLOT NODE left the slot open")
	}
	// STABLE clears an open slot without moving it.
	c.cmd("CLUSTER SETSLOT 2 MIGRATING " + other.id)
	if got := c.cmd("CLUSTER SETSLOT 2 STABLE"); got != "+OK" {
		t.Errorf("SETSLOT STABLE = %q", got)
	}
	if s.cluster.slot(2).migrating != nil {
		t.Error("SETSLOT STABLE did not clear the migration state")
	}
}

// TestClusterForgetAndReset covers removing a node and wiping this one.
func TestClusterForgetAndReset(t *testing.T) {
	s, c := newClusterServer(t, 7001)
	other := &clusterNode{id: strings.Repeat("d", 40), ip: "127.0.0.1", port: 7002, cport: 17002}
	s.cluster.mu.Lock()
	s.cluster.putNodeLocked(other)
	s.cluster.mu.Unlock()

	myID := s.cluster.myself().id
	if got := c.cmd("CLUSTER FORGET " + myID); got != "-ERR I tried hard but I can't forget myself..." {
		t.Errorf("FORGET myself = %q", got)
	}
	if got := c.cmd("CLUSTER FORGET " + strings.Repeat("z", 40)); !strings.HasPrefix(got, "-ERR Unknown node") {
		t.Errorf("FORGET an unknown node = %q", got)
	}
	// Forgetting a node that owned slots releases them rather than leaving them pointing
	// at a node this server can no longer name.
	c.cmd("CLUSTER ADDSLOTS 7")
	c.cmd("CLUSTER SETSLOT 7 NODE " + other.id)
	if got := c.cmd("CLUSTER FORGET " + other.id); got != "+OK" {
		t.Fatalf("FORGET = %q", got)
	}
	if s.cluster.slot(7).owner != nil {
		t.Error("a forgotten node still owns slot 7")
	}
	if s.cluster.nodeByID(other.id) != nil {
		t.Error("FORGET left the node in the table")
	}

	// RESET refuses while the node holds data.
	c.cmd("CLUSTER ADDSLOTSRANGE 0 16383")
	c.cmd("SET k v")
	want := "-ERR CLUSTER RESET can't be called with master nodes containing keys"
	if got := c.cmd("CLUSTER RESET"); got != want {
		t.Errorf("RESET with keys = %q; want %q", got, want)
	}
	c.cmd("DEL k")
	if got := c.cmd("CLUSTER RESET SOFT"); got != "+OK" {
		t.Fatalf("RESET SOFT = %q", got)
	}
	if n := s.cluster.table.Load().count(); n != 0 {
		t.Errorf("after RESET, %d slots are still assigned", n)
	}
	if s.cluster.myself().id != myID {
		t.Error("a soft reset changed the node id")
	}
	// A hard reset mints a new identity.
	if got := c.cmd("CLUSTER RESET HARD"); got != "+OK" {
		t.Fatalf("RESET HARD = %q", got)
	}
	if s.cluster.myself().id == myID {
		t.Error("a hard reset kept the old node id")
	}
	if got := c.cmd("CLUSTER RESET SIDEWAYS"); !strings.HasPrefix(got, "-ERR Unknown subcommand") {
		t.Errorf("RESET with a bad argument = %q", got)
	}
}

// TestClusterConfigFileRoundTrip is what makes a restart survivable: a node has to come
// back as itself, owning the same slots and knowing the same peers. A node that
// restarted with a new id and an empty slot map would be holding data it no longer
// claims, and every client would be redirected away from keys that are right there.
func TestClusterConfigFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.conf")

	s1 := New(store.New(8))
	if err := s1.EnableCluster(ClusterOptions{
		ConfigFile: path, AnnounceIP: "10.0.0.1", AnnouncePort: 7001,
	}); err != nil {
		t.Fatalf("EnableCluster: %v", err)
	}
	c1 := &directClient{t: t, s: s1}
	c1.cmd("CLUSTER ADDSLOTSRANGE 0 5460")
	c1.cmd("CLUSTER ADDSLOTS 9000")
	// A peer, so the node table has to survive too.
	peer := &clusterNode{id: strings.Repeat("e", 40), ip: "10.0.0.2", port: 7002, cport: 17002}
	s1.cluster.mu.Lock()
	s1.cluster.putNodeLocked(peer)
	s1.cluster.saveErrLocked()
	s1.cluster.mu.Unlock()

	before := c1.cmd("CLUSTER NODES")
	myID := s1.cluster.myself().id

	// A second server on the same file is the restart.
	s2 := New(store.New(8))
	if err := s2.EnableCluster(ClusterOptions{
		ConfigFile: path, AnnounceIP: "10.0.0.1", AnnouncePort: 7001,
	}); err != nil {
		t.Fatalf("reloading: %v", err)
	}
	c2 := &directClient{t: t, s: s2}
	if got := s2.cluster.myself().id; got != myID {
		t.Errorf("after a restart the node id is %q; want %q", got, myID)
	}
	if got := c2.cmd("CLUSTER NODES"); got != before {
		t.Errorf("CLUSTER NODES after a restart:\n%s\nbefore:\n%s", got, before)
	}
	if s2.cluster.table.Load().count() != 5462 {
		t.Errorf("after a restart %d slots are owned; want 5462",
			s2.cluster.table.Load().count())
	}
	if s2.cluster.nodeByID(peer.id) == nil {
		t.Error("the peer did not survive the restart")
	}

	// A file that is not a cluster configuration is refused rather than half-understood:
	// continuing from a slot map nobody could parse would mean serving keys this node may
	// no longer own.
	bad := filepath.Join(t.TempDir(), "bad.conf")
	if err := os.WriteFile(bad, []byte("this is not a node line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s3 := New(store.New(8))
	if err := s3.EnableCluster(ClusterOptions{ConfigFile: bad, AnnouncePort: 7003}); err == nil {
		t.Error("a malformed cluster configuration was accepted")
	}
}

// TestClusterMeet drives the one place configuration crosses a node boundary without an
// operator typing it. MEET opens an ordinary client connection to the peer, reads its
// CLUSTER NODES, and adopts the peer's identity and the slots it claims -- once.
func TestClusterMeet(t *testing.T) {
	a, addrA := startClusterNode(t, "")
	b, addrB := startClusterNode(t, "")
	ca := &directClient{t: t, s: a}
	cb := &directClient{t: t, s: b}

	// B owns the upper half and announces the address it is actually reachable at.
	cb.cmd("CLUSTER ADDSLOTSRANGE 8192 16383")
	ca.cmd("CLUSTER ADDSLOTSRANGE 0 8191")

	host, port := splitHostPortTest(t, addrB)
	if got := ca.cmd("CLUSTER MEET " + host + " " + port); got != "+OK" {
		t.Fatalf("CLUSTER MEET = %q", got)
	}
	bID := b.cluster.myself().id
	if a.cluster.nodeByID(bID) == nil {
		t.Fatal("MEET did not add the peer to the node table")
	}
	// A's view now covers every slot, because it adopted B's claim for the half it had
	// not assigned itself.
	if n := a.cluster.table.Load().count(); n != numSlots {
		t.Errorf("after MEET, A knows an owner for %d slots; want %d", n, numSlots)
	}
	if owner := a.cluster.slot(10000).owner; owner == nil || owner.id != bID {
		t.Error("A did not adopt B's claim on slot 10000")
	}
	// And it did not adopt B's claim on anything A had already assigned. MEET can never
	// move a slot: that is SETSLOT's job, and it is explicit.
	if owner := a.cluster.slot(100).owner; owner == nil || !owner.myself {
		t.Error("MEET moved a slot A already owned")
	}
	if !strings.Contains(ca.cmd("CLUSTER INFO"), "cluster_state:ok") {
		t.Error("A does not consider the cluster covered after MEET")
	}

	// MEET is one-directional by design: B has heard nothing. Saying so in a test is the
	// point -- it is the boundary the README states, not an oversight.
	if b.cluster.nodeByID(a.cluster.myself().id) != nil {
		t.Error("MEET propagated to the peer; it is documented as one-directional")
	}

	// Meeting yourself is refused, and a peer that is not there is an error the operator
	// sees rather than a command that silently did nothing.
	hostA, portA := splitHostPortTest(t, addrA)
	if got := ca.cmd("CLUSTER MEET " + hostA + " " + portA); got != "-ERR Can't meet myself" {
		t.Errorf("MEET myself = %q", got)
	}
	if got := ca.cmd("CLUSTER MEET 127.0.0.1 1"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("MEET an address with nothing on it = %q; want an error", got)
	}
	if got := ca.cmd("CLUSTER MEET 127.0.0.1 notaport"); got != "-ERR Invalid base port specified" {
		t.Errorf("MEET with a bad port = %q", got)
	}
}

func splitHostPortTest(t *testing.T, addr string) (host, port string) {
	t.Helper()
	i := strings.LastIndexByte(addr, ':')
	if i < 0 {
		t.Fatalf("malformed address %q", addr)
	}
	return addr[:i], addr[i+1:]
}

// TestClusterRequiresSingleDatabase states the contradiction rather than clamping it
// silently: a cluster is a partition of one keyspace across nodes, and a second
// database would be a keyspace with no slots and so no node responsible for it.
func TestClusterRequiresSingleDatabase(t *testing.T) {
	s := New(store.New(8))
	if err := s.SetDatabases(16); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableCluster(ClusterOptions{AnnouncePort: 7001}); err == nil {
		t.Error("cluster mode was enabled on a server with 16 databases")
	}
}
