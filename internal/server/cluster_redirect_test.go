package server

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// sessionClient runs commands through the *client* entry point, which is where the
// cluster redirect lives.
//
// directClient goes through dispatch, and deliberately keeps doing so: dispatch is the
// path an AOF replay and a master's stream take, and neither may ever be redirected --
// a replica applying its master's writes must apply all of them, whatever the slot map
// says about who owns the key. That distinction is the reason for a second client here
// rather than a flag on the first.
type sessionClient struct {
	t    *testing.T
	s    *Server
	sess *session
}

func newSessionClient(t *testing.T, s *Server) *sessionClient {
	t.Helper()
	return &sessionClient{t: t, s: s, sess: s.newSession(nil)}
}

func (c *sessionClient) cmd(cmd string) string {
	c.t.Helper()
	return c.args(strings.Fields(cmd)...)
}

func (c *sessionClient) args(parts ...string) string {
	c.t.Helper()
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	c.s.execute(c.sess, w, cmdArgs(parts...))
	if err := w.Flush(); err != nil {
		c.t.Fatalf("flush: %v", err)
	}
	return readReply(c.t, bufio.NewReader(&buf))
}

// twoNodeCluster returns a cluster-enabled server that owns the slots of everything in
// `mine` and believes a peer at 127.0.0.1:7002 owns the slots of everything in
// `theirs`. Every other slot is left unassigned, which is what CLUSTERDOWN is for.
//
// It is built by hand rather than by standing up a second process because what is under
// test is the *decision*, and the decision is a pure function of the slot map.
func twoNodeCluster(t *testing.T, mine, theirs []string) (*Server, *clusterNode) {
	t.Helper()
	s := New(store.New(8))
	if err := s.EnableCluster(ClusterOptions{AnnounceIP: "127.0.0.1", AnnouncePort: 7001}); err != nil {
		t.Fatalf("EnableCluster: %v", err)
	}
	peer := &clusterNode{id: strings.Repeat("a", 40), ip: "127.0.0.1", port: 7002, cport: 17002}
	cs := s.cluster
	cs.mu.Lock()
	cs.putNodeLocked(peer)
	me := cs.nodes[cs.myID]
	cs.mutateLocked(func(tb *slotTable) {
		for _, k := range mine {
			tb.slots[KeySlot(k)].owner = me
		}
		for _, k := range theirs {
			tb.slots[KeySlot(k)].owner = peer
		}
	})
	cs.mu.Unlock()
	return s, peer
}

// TestClusterMoved is the ordinary redirect: the key's slot belongs to another node, so
// the client is told which one and expected to update its routing table.
func TestClusterMoved(t *testing.T) {
	s, peer := twoNodeCluster(t, []string{"mine"}, []string{"theirs"})
	c := newSessionClient(t, s)

	if got := c.cmd("SET mine value"); got != "+OK" {
		t.Errorf("a key in a slot this node owns = %q", got)
	}
	want := "-MOVED " + itoa(KeySlot("theirs")) + " 127.0.0.1:7002"
	for _, cmd := range []string{"GET theirs", "SET theirs v", "DEL theirs", "TYPE theirs",
		"EXPIRE theirs 10", "LPUSH theirs v", "DUMP theirs"} {
		if got := c.cmd(cmd); got != want {
			t.Errorf("%s -> %q; want %q", cmd, got, want)
		}
	}
	// The redirect names the address the peer was introduced with, because that is the
	// address the client will dial next.
	if !strings.Contains(want, peer.addr()) {
		t.Errorf("the redirect does not name the peer's address %q", peer.addr())
	}

	// A command that names no key belongs to whichever node received it. Redirecting
	// PING or INFO would leave a client unable to talk to a node at all.
	for _, cmd := range []string{"PING", "INFO server", "DBSIZE", "COMMAND COUNT",
		"CLUSTER MYID", "SCAN 0", "RANDOMKEY", "CLUSTER KEYSLOT theirs"} {
		if got := c.cmd(cmd); strings.HasPrefix(got, "-MOVED") {
			t.Errorf("%s was redirected: %q", cmd, got)
		}
	}
	// Nor is Pub/Sub routed: channels are not keys and have no slot. See the README's
	// Cluster section for what that means across nodes.
	if got := c.cmd("PUBLISH news hello"); strings.HasPrefix(got, "-") {
		t.Errorf("PUBLISH was redirected: %q", got)
	}
}

// TestClusterDownUnassignedSlot covers a slot nobody claims. It is a distinct answer
// from MOVED on purpose: there is no node to name, and a client that retried elsewhere
// would only be redirected again.
func TestClusterDownUnassignedSlot(t *testing.T) {
	s, _ := twoNodeCluster(t, []string{"mine"}, []string{"theirs"})
	c := newSessionClient(t, s)

	if got := c.cmd("GET orphan"); got != "-"+errSlotUnbound {
		t.Errorf("a key in an unassigned slot = %q; want %q", got, "-"+errSlotUnbound)
	}
	if got := c.cmd("SET orphan v"); got != "-"+errSlotUnbound {
		t.Errorf("writing an unassigned slot = %q", got)
	}
	// And CLUSTER INFO says why.
	if !strings.Contains(c.cmd("CLUSTER INFO"), "cluster_state:fail") {
		t.Error("a partially covered cluster does not report state fail")
	}
}

// TestClusterCrossSlot is the check that makes multi-key commands safe: two keys in
// different slots may live on different nodes, so no node can serve the command.
//
// The cases below are every multi-key shape the server has, and they are driven by the
// same key extraction COMMAND GETKEYS answers with and WATCH depends on (invariant 7).
// A second list maintained for routing would drift from that one, and the drift would
// be silent in both directions -- a command missing from the routing list served by the
// wrong node, a command missing from the WATCH list committing over a concurrent change.
func TestClusterCrossSlot(t *testing.T) {
	// One node owning everything, so nothing can be answered with MOVED and a
	// cross-slot command has no excuse other than the real one.
	s := New(store.New(8))
	if err := s.EnableCluster(ClusterOptions{AnnounceIP: "127.0.0.1", AnnouncePort: 7001}); err != nil {
		t.Fatalf("EnableCluster: %v", err)
	}
	dc := &directClient{t: t, s: s}
	dc.cmd("CLUSTER ADDSLOTSRANGE 0 16383")
	c := newSessionClient(t, s)

	// "a" and "b" are in different slots; "{t}:1" and "{t}:2" share one.
	if KeySlot("a") == KeySlot("b") {
		t.Fatal("the test's two keys collide; pick different ones")
	}
	crossSlot := []string{
		"MGET a b",
		"MSET a 1 b 2",
		"DEL a b",
		"UNLINK a b",
		"EXISTS a b",
		"TOUCH a b",
		"RENAME a b",
		"RENAMENX a b",
		"COPY a b",
		"SMOVE a b m",
		"LMOVE a b LEFT RIGHT",
		"RPOPLPUSH a b",
		"SINTERSTORE a b c",
		"SUNIONSTORE a b c",
		"SDIFFSTORE a b c",
		"SINTER a b",
		"SUNION a b",
		"SDIFF a b",
		"SINTERCARD 2 a b",
		"LMPOP 2 a b LEFT",
		"ZMPOP 2 a b MIN",
		"PFCOUNT a b",
		"PFMERGE a b",
		"BITOP AND dest a b",
		"GEOSEARCHSTORE a b FROMMEMBER m BYRADIUS 1 km ASC",
		"XREAD COUNT 1 STREAMS a b 0 0",
		"WATCH a b",
	}
	for _, cmd := range crossSlot {
		if got := c.cmd(cmd); got != "-"+errCrossSlot {
			t.Errorf("%s -> %q; want %q", cmd, got, "-"+errCrossSlot)
		}
	}

	// The same commands with a hash tag are legal, because the tag puts every key in one
	// slot. This half is what makes the feature usable rather than merely restrictive.
	for _, cmd := range []string{
		"MGET {t}:a {t}:b",
		"MSET {t}:a 1 {t}:b 2",
		"DEL {t}:a {t}:b",
		"EXISTS {t}:a {t}:b",
		"SINTER {t}:a {t}:b",
		"PFCOUNT {t}:a {t}:b",
		"BITOP AND {t}:dest {t}:a {t}:b",
		"XREAD COUNT 1 STREAMS {t}:a {t}:b 0 0",
		"WATCH {t}:a {t}:b",
	} {
		if got := c.cmd(cmd); got == "-"+errCrossSlot {
			t.Errorf("%s was refused as cross-slot, but its keys share a tag", cmd)
		}
	}
	c.cmd("UNWATCH")

	// A single-key command is never cross-slot, whatever its arity.
	if got := c.cmd("MGET onlyone"); strings.HasPrefix(got, "-CROSSSLOT") {
		t.Errorf("a one-key MGET was refused: %q", got)
	}
}

// TestClusterCrossSlotSourceIsCommandGetKeys is the anti-drift check the comment above
// argues for: whatever COMMAND GETKEYS reports as a command's keys is exactly what the
// redirect decision routes on. If someone adds a multi-key command and updates only one
// of the two, this fails.
func TestClusterCrossSlotSourceIsCommandGetKeys(t *testing.T) {
	s := New(store.New(8))
	if err := s.EnableCluster(ClusterOptions{AnnounceIP: "127.0.0.1", AnnouncePort: 7001}); err != nil {
		t.Fatalf("EnableCluster: %v", err)
	}
	dc := &directClient{t: t, s: s}
	dc.cmd("CLUSTER ADDSLOTSRANGE 0 16383")

	for _, cmd := range []string{
		"MSET a 1 b 2", "BITOP AND dest src1 src2", "GEOSEARCHSTORE dst src FROMMEMBER m BYRADIUS 1 km ASC",
		"XREAD COUNT 2 STREAMS s1 s2 0 0", "LMPOP 2 k1 k2 LEFT", "SINTERCARD 2 a b",
		"GET single", "XGROUP CREATE stream grp 0", "MEMORY USAGE somekey",
		"DEL a b c", "PFCOUNT h1 h2", "SMOVE src dst m", "COPY src dst",
	} {
		parts := strings.Fields(cmd)
		name := strings.ToUpper(parts[0])
		fromRouting := s.redirectKeys(nil, name, cmdArgs(parts...))
		fromGetKeys, errMsg := commandGetKeys(cmdArgs(parts...))
		if errMsg != "" {
			t.Errorf("COMMAND GETKEYS %s: %s", cmd, errMsg)
			continue
		}
		if strings.Join(fromRouting, ",") != strings.Join(fromGetKeys, ",") {
			t.Errorf("%s: routing sees %v, COMMAND GETKEYS reports %v",
				cmd, fromRouting, fromGetKeys)
		}
	}
}

// TestClusterAskRedirect drives a slot migration from the client's point of view: the
// three states a key can be in while its slot is open, and the one-shot flag that lets
// the destination serve it.
func TestClusterAskRedirect(t *testing.T) {
	s, peer := twoNodeCluster(t, []string{"{mig}"}, nil)
	c := newSessionClient(t, s)
	dc := &directClient{t: t, s: s}
	slot := KeySlot("{mig}")

	// Two keys in the migrating slot: one still here, one already handed over.
	c.cmd("SET {mig}:here value")
	if got := dc.cmd("CLUSTER SETSLOT " + itoa(slot) + " MIGRATING " + peer.id); got != "+OK" {
		t.Fatalf("SETSLOT MIGRATING = %q", got)
	}

	// A key this node still holds is served: it has not moved yet, and redirecting it
	// would send the client to a node that does not have it either.
	if got := c.cmd("GET {mig}:here"); got != "value" {
		t.Errorf("a key still present in a migrating slot = %q; want it served", got)
	}
	// A key this node no longer has draws an ASK, not a MOVED: ownership has not changed,
	// so the client must follow the redirect for this request only and must *not* update
	// its routing table.
	wantASK := "-ASK " + itoa(slot) + " 127.0.0.1:7002"
	if got := c.cmd("GET {mig}:gone"); got != wantASK {
		t.Errorf("a key missing from a migrating slot = %q; want %q", got, wantASK)
	}
	if got := c.cmd("SET {mig}:gone v"); got != wantASK {
		t.Errorf("writing a key missing from a migrating slot = %q", got)
	}
	// A multi-key command straddling the two is neither: no node can serve it, and both
	// redirects would be lies, so the client is asked to retry once the slot settles.
	if got := c.cmd("MGET {mig}:here {mig}:gone"); got != "-"+errTryAgain {
		t.Errorf("a half-migrated multi-key command = %q; want %q", got, "-"+errTryAgain)
	}
	// Both keys gone is an ordinary ASK: there is one node that has them.
	if got := c.cmd("MGET {mig}:gone {mig}:alsogone"); got != wantASK {
		t.Errorf("a fully-migrated multi-key command = %q; want %q", got, wantASK)
	}
}

// TestClusterAskingFlag covers the importing side: the node the ASK points at does not
// own the slot, so it answers MOVED to an ordinary client and serves only a client that
// says ASKING first -- for exactly one command.
func TestClusterAskingFlag(t *testing.T) {
	// This node owns nothing; the peer owns the slot and is migrating it here.
	s, peer := twoNodeCluster(t, nil, []string{"{imp}"})
	c := newSessionClient(t, s)
	dc := &directClient{t: t, s: s}
	slot := KeySlot("{imp}")
	if got := dc.cmd("CLUSTER SETSLOT " + itoa(slot) + " IMPORTING " + peer.id); got != "+OK" {
		t.Fatalf("SETSLOT IMPORTING = %q", got)
	}

	moved := "-MOVED " + itoa(slot) + " 127.0.0.1:7002"
	if got := c.cmd("GET {imp}:k"); got != moved {
		t.Errorf("an ordinary client on an importing node = %q; want %q", got, moved)
	}
	// ASKING makes exactly the next command acceptable.
	if got := c.cmd("ASKING"); got != "+OK" {
		t.Fatalf("ASKING = %q", got)
	}
	if got := c.cmd("SET {imp}:k value"); got != "+OK" {
		t.Errorf("the command after ASKING = %q; want it served", got)
	}
	// And the flag is gone: ownership has not changed, so a flag that persisted would let
	// this node serve a slot it does not own.
	if got := c.cmd("GET {imp}:k"); got != moved {
		t.Errorf("the second command after one ASKING = %q; want %q", got, moved)
	}
	if got := c.cmd("ASKING"); got != "+OK" {
		t.Fatalf("ASKING = %q", got)
	}
	if got := c.cmd("GET {imp}:k"); got != "value" {
		t.Errorf("after a fresh ASKING = %q; want the value that arrived", got)
	}

	// ASKING does not make an unrelated slot acceptable: the flag is about this slot's
	// migration, not a licence to serve anything.
	other := KeySlot("{other}")
	s.cluster.mu.Lock()
	s.cluster.mutateLocked(func(tb *slotTable) { tb.slots[other].owner = peer })
	s.cluster.mu.Unlock()
	c.cmd("ASKING")
	if got := c.cmd("GET {other}:k"); got != "-MOVED "+itoa(other)+" 127.0.0.1:7002" {
		t.Errorf("ASKING served a slot that is not being imported: %q", got)
	}
}

// TestClusterAskingRejectedOnStandalone keeps the new connection commands honest on a
// server that is not clustered.
func TestClusterAskingRejectedOnStandalone(t *testing.T) {
	s := New(store.New(8))
	c := newSessionClient(t, s)
	for _, cmd := range []string{"ASKING", "READONLY", "READWRITE"} {
		if got := c.cmd(cmd); got != "-"+errNotCluster {
			t.Errorf("%s on a standalone server = %q", cmd, got)
		}
	}
}

// TestClusterTransactionRedirect covers the batch. A transaction runs on one node or
// not at all, so EXEC is checked against every key its queued commands name -- and a
// queued command that was itself redirected has to poison the batch, or EXEC would
// apply a fragment of what the client asked for.
func TestClusterTransactionRedirect(t *testing.T) {
	s, _ := twoNodeCluster(t, []string{"mine", "mine2", "{tx}"}, []string{"theirs"})
	c := newSessionClient(t, s)
	moved := "-MOVED " + itoa(KeySlot("theirs")) + " 127.0.0.1:7002"

	// A command for another node is redirected at queue time, as Redis does: the client
	// finds out immediately rather than at EXEC.
	c.cmd("MULTI")
	if got := c.cmd("SET mine a"); got != "+QUEUED" {
		t.Errorf("queueing a local key = %q", got)
	}
	if got := c.cmd("SET theirs b"); got != moved {
		t.Errorf("queueing a remote key = %q; want %q", got, moved)
	}
	// And the batch is poisoned, so EXEC cannot apply the half that was accepted.
	if got := c.cmd("EXEC"); got != "-EXECABORT Transaction discarded because of previous errors." {
		t.Errorf("EXEC after a redirected queued command = %q", got)
	}
	if got := c.cmd("GET mine"); got != "(nil)" {
		t.Error("a poisoned transaction applied one of its commands")
	}

	// A batch whose keys are all in one slot this node owns runs.
	c.cmd("MULTI")
	c.cmd("SET {tx}:a 1")
	c.cmd("GET {tx}:a")
	if got := c.cmd("EXEC"); got != "[+OK 1]" {
		t.Errorf("EXEC of a local batch = %q", got)
	}
	c.cmd("DEL {tx}:a")

	// A batch that straddles two slots is refused at EXEC, where the whole set of keys is
	// visible for the first time. Both keys are owned by this node, so this is the
	// cross-slot check applied to the batch rather than to any one command.
	c.cmd("MULTI")
	c.cmd("SET mine a")
	c.cmd("SET mine2 b")
	if got := c.cmd("EXEC"); got != "-"+errCrossSlot {
		t.Errorf("EXEC of a batch spanning two slots = %q; want %q", got, "-"+errCrossSlot)
	}
	// The transaction is discarded outright, so the connection is not left inside a MULTI
	// it can never commit.
	if got := c.cmd("EXEC"); got != "-ERR EXEC without MULTI" {
		t.Errorf("after a redirected EXEC the connection is still in a transaction: %q", got)
	}

	// A batch whose keys share a tag is fine however many commands it has.
	c.cmd("MULTI")
	c.cmd("SET {tx}:a 1")
	c.cmd("INCR {tx}:b")
	c.cmd("LPUSH {tx}:c v")
	if got := c.cmd("EXEC"); got != "[+OK :1 :1]" {
		t.Errorf("EXEC of a tagged batch = %q", got)
	}
}

// TestClusterReplicaStreamIsNeverRedirected is the boundary that keeps replication
// working inside a cluster. A replica applies every write its master sends, whatever
// this node's slot map says: the master already decided the write belongs to that slot,
// and a replica that redirected it would silently drop data it is supposed to hold.
func TestClusterReplicaStreamIsNeverRedirected(t *testing.T) {
	s, _ := twoNodeCluster(t, []string{"mine"}, []string{"theirs"})
	c := newSessionClient(t, s)

	// A client is redirected...
	if got := c.cmd("SET theirs v"); !strings.HasPrefix(got, "-MOVED") {
		t.Fatalf("a client write to a foreign slot = %q", got)
	}
	// ...but the same command arriving from a master, or out of an AOF, is applied.
	s.applyCommand(resp.NewWriter(bytes.NewBuffer(nil)), cmdArgs("SET", "theirs", "v"))
	if got, ok := s.store.Get("theirs"); !ok || string(got) != "v" {
		t.Error("a write from the replication stream was not applied")
	}
	// dispatch is that path, and it is deliberately not gated either.
	dc := &directClient{t: t, s: s}
	if got := dc.cmd("SET theirs v2"); got != "+OK" {
		t.Errorf("dispatch (the AOF/replica path) was redirected: %q", got)
	}
}

// TestClusterSingleDatabase pins the commands that name a second database. In cluster
// mode there is only database 0, so each of them has nowhere to go and says so.
func TestClusterSingleDatabase(t *testing.T) {
	s, _ := twoNodeCluster(t, []string{"k", "{c}"}, nil)
	c := newSessionClient(t, s)

	if got := c.cmd("SELECT 0"); got != "+OK" {
		t.Errorf("SELECT 0 in cluster mode = %q; want it accepted", got)
	}
	for _, tc := range []struct{ cmd, want string }{
		{"SELECT 1", "-" + errClusterNoDB("SELECT")},
		{"SWAPDB 0 1", "-" + errClusterNoDB("SWAPDB")},
		{"MOVE k 1", "-" + errClusterNoDB("MOVE")},
		// Tagged, so the pair is in one slot and the cross-slot check does not answer
		// first: what is under test here is the database, not the routing.
		{"COPY {c}:1 {c}:2 DB 1", "-ERR Copying to another database is not allowed in cluster mode"},
	} {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%s -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestClusterRedirectCostsNothingWhenDisabled is invariant 12's discipline applied to
// the new gate: the redirect sits on the path of every command, so a standalone server
// must not pay for a feature it never switched on. One atomic load, no slot computed
// and no key slice built.
func TestClusterRedirectCostsNothingWhenDisabled(t *testing.T) {
	s := New(store.New(8))
	c := newSessionClient(t, s)
	c.cmd("MSET a 1 b 2")

	before := testing.AllocsPerRun(200, func() {
		if s.ClusterEnabled() {
			s.clusterRedirect(c.sess, "MGET", cmdArgs("MGET", "a", "b"))
		}
	})
	if before != 0 {
		t.Errorf("the disabled redirect gate allocates %v times per command; want 0", before)
	}
	// And it is a live gate, not a broken one: with cluster mode on, the same call does
	// decide something.
	if err := s.EnableCluster(ClusterOptions{AnnounceIP: "127.0.0.1", AnnouncePort: 7001}); err != nil {
		t.Fatalf("EnableCluster: %v", err)
	}
	if got := s.clusterRedirect(c.sess, "MGET", cmdArgs("MGET", "a", "b")); got != errCrossSlot {
		t.Errorf("with cluster mode on, the gate answered %q; want a cross-slot refusal", got)
	}
}
