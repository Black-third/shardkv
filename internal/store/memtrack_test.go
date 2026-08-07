package store

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// TestMemoryAccountingDoesNotDrift is the proof the maintained byte total is worth
// reporting: a long randomised mixture of every mutation the store offers, checked
// against a full recomputation.
//
// It is checked two ways, and the per-entry half is the one that matters. A total that
// agrees by luck -- one key over by 40 bytes, another under by 40 -- is not an accounting
// anyone can reason about, and it would drift the moment the workload changed. So every
// entry's maintained payload is compared against a walk of that entry, and only then is
// the sum compared against the store's counter.
//
// Streams are deliberately absent from the mutation set. Their write paths live in
// stream.go, which this accounting does not instrument, so their size is refreshed by the
// maintenance pass instead -- see TestMemoryAccountingConvergesForStreams, which measures
// exactly that lag and its convergence.
func TestMemoryAccountingDoesNotDrift(t *testing.T) {
	s := New(16)
	// The accounting is off until something asks for it, which is the state every existing
	// deployment is in. A budget (or a reader of used_memory) turns it on; this test is about
	// what happens once it is on.
	s.TrackMemory()
	rng := rand.New(rand.NewSource(20240806))

	key := func() string { return "k" + strconv.Itoa(rng.Intn(24)) }
	val := func() []byte {
		n := rng.Intn(40)
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('a' + rng.Intn(26))
		}
		return b
	}
	field := func() string { return "f" + strconv.Itoa(rng.Intn(8)) }

	// Every mutating store method reachable without a stream, one closure each, so the
	// mixture below covers create, overwrite, in-place growth, in-place shrink, element
	// removal, whole-collection replacement, cross-key moves and deletion.
	ops := []func(){
		func() { s.Set(key(), val(), 0) },
		func() { s.Set(key(), val(), time.Duration(rng.Intn(4))*time.Millisecond) },
		func() { s.SetNX(key(), val()) },
		func() { s.SetDeadline(key(), val(), s.Now().Add(time.Hour)) },
		func() { s.SetWithOptions(key(), val(), SetOptions{}) },                    //nolint:errcheck
		func() { s.SetWithOptions(key(), val(), SetOptions{XX: true, Get: true}) }, //nolint:errcheck
		func() { s.GetSet(key(), val()) },                                          //nolint:errcheck
		func() { s.GetDel(key()) },                                                 //nolint:errcheck
		func() { s.Append(key(), val()) },                                          //nolint:errcheck
		func() { s.SetRange(key(), rng.Intn(64), val()) },                          //nolint:errcheck
		func() { s.Incr(key(), int64(rng.Intn(9))) },                               //nolint:errcheck
		func() { s.IncrByFloat(key(), mustLD(t, "1.5")) },                          //nolint:errcheck
		func() { s.SetBit(key(), int64(rng.Intn(512)), rng.Intn(2) == 0) },         //nolint:errcheck
		func() { s.PFAdd(key(), [][]byte{val(), val()}) },                          //nolint:errcheck
		func() { s.PFMerge(key(), []string{key(), key()}) },                        //nolint:errcheck
		func() { s.PFToDense(key()) },                                              //nolint:errcheck
		func() { s.BitOp(BitOpXor, key(), []string{key(), key()}) },                //nolint:errcheck
		func() { s.LPush(key(), val(), val()) },                                    //nolint:errcheck
		func() { s.RPush(key(), val()) },                                           //nolint:errcheck
		func() { s.LPushX(key(), val()) },                                          //nolint:errcheck
		func() { s.LPop(key()) },                                                   //nolint:errcheck
		func() { s.RPop(key()) },                                                   //nolint:errcheck
		func() { s.LPopCount(key(), rng.Intn(3), rng.Intn(2) == 0) },               //nolint:errcheck
		func() { s.LSet(key(), rng.Intn(4), val()) },                               //nolint:errcheck
		func() { s.LInsert(key(), rng.Intn(2) == 0, val(), val()) },                //nolint:errcheck
		func() { s.LRem(key(), rng.Intn(3)-1, val()) },                             //nolint:errcheck
		func() { s.LTrim(key(), rng.Intn(3), rng.Intn(6)) },                        //nolint:errcheck
		func() { s.LMove(key(), key(), rng.Intn(2) == 0, rng.Intn(2) == 0) },       //nolint:errcheck
		func() { s.ReplaceList(key(), [][]byte{val(), val(), val()}) },             //nolint:errcheck
		func() { s.HSet(key(), [2][]byte{[]byte(field()), val()}) },                //nolint:errcheck
		func() { s.HSetNX(key(), field(), val()) },                                 //nolint:errcheck
		func() { s.HIncrBy(key(), field(), 3) },                                    //nolint:errcheck
		func() { s.HIncrByFloat(key(), field(), mustLD(t, "0.25")) },               //nolint:errcheck
		func() { s.HDel(key(), field(), field()) },                                 //nolint:errcheck
		func() { s.SAdd(key(), string(val()), string(val())) },                     //nolint:errcheck
		func() { s.SRem(key(), string(val())) },                                    //nolint:errcheck
		func() { s.SPop(key(), rng.Intn(3)) },                                      //nolint:errcheck
		func() { s.SMove(key(), key(), string(val())) },                            //nolint:errcheck
		func() { s.SCombineStore(SetOp(rng.Intn(3)), key(), key(), key()) },        //nolint:errcheck
		func() { s.ZAdd(key(), string(val()), float64(rng.Intn(50))) },             //nolint:errcheck
		func() {
			s.ZAddMulti(key(), ZAddOptions{}, []ZMember{ //nolint:errcheck
				{Member: string(val()), Score: 1}, {Member: string(val()), Score: 2},
			})
		},
		func() { s.ZIncrBy(key(), string(val()), 2, ZAddOptions{}) },      //nolint:errcheck
		func() { s.ZRem(key(), string(val())) },                           //nolint:errcheck
		func() { s.ZRemMulti(key(), string(val()), string(val())) },       //nolint:errcheck
		func() { s.ZPop(key(), rng.Intn(3), rng.Intn(2) == 0) },           //nolint:errcheck
		func() { s.ZRemRangeByRank(key(), 0, rng.Intn(2)) },               //nolint:errcheck
		func() { s.ZRemRangeByScore(key(), ScoreRange{Min: 0, Max: 10}) }, //nolint:errcheck
		func() {
			s.ZCombineStore(key(), ZCombineUnion, //nolint:errcheck
				[]ZSetOp{{Key: key(), Weight: 1}, {Key: key(), Weight: 1}}, ZAggSum)
		},
		func() { s.ZRangeStore(key(), key(), ZRangeSelector{By: ZRangeByRank, Stop: -1, Count: -1}) }, //nolint:errcheck
		func() { s.Copy(key(), key(), true) }, //nolint:errcheck
		func() { s.Rename(key(), key()) },
		func() { s.RenameNX(key(), key()) },
		func() { s.Del(key()) },
		func() { s.Expire(key(), time.Millisecond) },
		func() { s.Persist(key()) },
		func() { s.MSetNX([][2][]byte{{[]byte(key()), val()}, {[]byte(key()), val()}}) },
	}

	const rounds = 20000
	for i := 0; i < rounds; i++ {
		ops[rng.Intn(len(ops))]()
		// Every so often, reclaim what has expired: expiry and eviction are mutation
		// paths too, and the point of the test is that the counter survives them.
		if i%500 == 0 {
			s.sweep()
		}
		if i%1500 == 0 {
			s.SetMaxKeys(12)
			s.EvictToLimit()
			s.SetMaxKeys(0)
		}
	}

	assertNoDrift(t, s, "after the randomised mixture")

	// And through the operations that move whole keyspaces around.
	other := New(16)
	other.TrackMemory()
	for i := 0; i < 40; i++ {
		TransferKey(s, other, key(), i%2 == 0, true)
	}
	assertNoDrift(t, s, "after TransferKey out")
	assertNoDrift(t, other, "after TransferKey in")

	SwapData(s, other)
	assertNoDrift(t, s, "after SwapData")
	assertNoDrift(t, other, "after SwapData (other)")

	s.FlushAll()
	if got := s.UsedMemory(); got != 0 {
		t.Errorf("UsedMemory after FlushAll = %d; want 0", got)
	}
}

// assertNoDrift compares the maintained accounting against a full recomputation, per
// entry first and then in total.
func assertNoDrift(t *testing.T, s *Store, when string) {
	t.Helper()
	if !s.MemoryTracked() {
		t.Fatalf("%s: the accounting is not running, so this would compare two zeroes", when)
	}
	var want int64
	entries := 0
	for i, sh := range s.shards {
		sh.mu.RLock()
		var shardTotal int64
		for k, e := range sh.data {
			entries++
			maintained, exact := entryPayload(e), entrySize(e)
			if maintained != exact {
				t.Errorf("%s: key %q (%s) maintained payload %d, measured %d (drift %+d)",
					when, k, e.kind, maintained, exact, maintained-exact)
			}
			shardTotal += memEntryOverhead + int64(len(k)) + exact
		}
		if shardTotal != sh.mem {
			t.Errorf("%s: shard %d total %d, measured %d", when, i, sh.mem, shardTotal)
		}
		want += shardTotal
		sh.mu.RUnlock()
	}
	if got := s.UsedMemory(); got != want {
		t.Errorf("%s: UsedMemory = %d, full recomputation = %d (drift %+d over %d entries)",
			when, got, want, got-want, entries)
	}
	if entries == 0 {
		t.Errorf("%s: the keyspace is empty, so nothing was actually checked", when)
	}
}

func mustLD(t *testing.T, s string) LongDouble {
	t.Helper()
	v, ok := ParseLongDouble(s)
	if !ok {
		t.Fatalf("ParseLongDouble(%q) failed", s)
	}
	return v
}

// TestMemoryAccountingConvergesForStreams measures the one documented gap: a stream's
// payload is not tracked at the moment it changes, because its mutations live in a file
// this accounting does not instrument. The number therefore lags a stream write and is
// corrected by the maintenance pass.
//
// The test pins both halves of that statement -- that the lag exists and is in the
// under-reporting direction, and that one pass closes it exactly -- so that the gap is a
// measured property rather than a hope, and so that wiring stream.go into the accounting
// would fail here loudly rather than pass silently.
func TestMemoryAccountingConvergesForStreams(t *testing.T) {
	s := New(4)
	s.TrackMemory()
	for i := 0; i < 200; i++ {
		if _, _, _, err := s.XAdd("orders", XAddOptions{Auto: true}, [][]byte{
			[]byte("item"), []byte("widget-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("XAdd: %v", err)
		}
	}
	tracked, exact := s.UsedMemory(), s.exactMemory()
	if tracked >= exact {
		t.Fatalf("UsedMemory = %d and the measured total is %d; the stream gap should"+
			" under-report until the maintenance pass runs", tracked, exact)
	}
	t.Logf("stream lag before the maintenance pass: %d bytes over 200 XADDs (%d vs %d)",
		exact-tracked, tracked, exact)

	s.maintain(false)
	if got, want := s.UsedMemory(), s.exactMemory(); got != want {
		t.Errorf("after the maintenance pass UsedMemory = %d; want %d", got, want)
	}

	// And a stream key that goes away stops being counted, which needs the pass to
	// re-derive the shard total rather than apply a delta.
	s.Del("orders")
	s.maintain(false)
	if got := s.UsedMemory(); got != 0 {
		t.Errorf("UsedMemory after deleting the only key = %d; want 0", got)
	}
}

// TestRecomputeMemoryHeals covers the self-healing path: a total corrupted by hand is put
// right, and so are the per-container counts it was derived from. It is what keeps a
// mutation site that ever escapes the accounting from being a permanent lie.
func TestRecomputeMemoryHeals(t *testing.T) {
	s := New(4)
	s.TrackMemory()
	s.Set("a", []byte("hello"), 0)
	if _, err := s.HSet("h", [2][]byte{[]byte("f"), []byte("v")}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if _, err := s.RPush("l", []byte("x"), []byte("y")); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	want := s.UsedMemory()

	// Corrupt what can realistically drift: a container's own byte count, and the shard
	// total that was derived from it. (The store's grand total cannot drift from the sum of
	// the shard totals independently -- every write to it happens under a shard lock, in
	// the same statement that moves the shard's own figure -- so the shard is where a
	// forgotten mutation site would show up.)
	sh := s.getShard("l")
	sh.mu.Lock()
	sh.data["l"].list.bytes += 4242
	sh.mem += 4242
	s.mem.Add(4242)
	sh.mu.Unlock()
	if s.UsedMemory() == want {
		t.Fatal("the corruption did not take")
	}

	s.RecomputeMemory()
	if got := s.UsedMemory(); got != want {
		t.Errorf("after RecomputeMemory UsedMemory = %d; want %d", got, want)
	}
	assertNoDrift(t, s, "after RecomputeMemory")
}

// TestUsedMemoryCountsWhatItSays pins the two facts the doc comment on memtrack.go
// claims, because both are the kind of thing that quietly stops being true: the number
// includes a key whose deadline has passed but which nothing has reclaimed (it is still
// occupying memory), and it stops including it the moment something does.
func TestUsedMemoryCountsUnreclaimedExpiredKeys(t *testing.T) {
	s := New(4)
	s.TrackMemory()
	cur := time.Unix(1000, 0)
	s.SetClock(func() time.Time { return cur })
	s.SetActiveExpire(false)

	s.Set("vol", make([]byte, 4096), 5*time.Second)
	withKey := s.UsedMemory()
	if withKey < 4096 {
		t.Fatalf("UsedMemory = %d; want at least the 4096 payload bytes", withKey)
	}

	cur = cur.Add(time.Minute) // the key is now logically gone but still stored
	if got := s.UsedMemory(); got != withKey {
		t.Errorf("UsedMemory after the deadline passed = %d; want %d (the bytes are"+
			" still resident until something reclaims them)", got, withKey)
	}
	if s.Exists("vol") {
		t.Fatal("the key should read as absent once its deadline has passed")
	}

	s.sweep()
	if got := s.UsedMemory(); got != 0 {
		t.Errorf("UsedMemory after the sweep = %d; want 0", got)
	}
}

// BenchmarkSetTracked and BenchmarkSetUntracked are the before/after pair for the claim
// that the accounting is affordable on the write path. Untracked is not a different code
// path -- there is only one -- but a store with no eviction configured, which is what
// every existing deployment is: it isolates the accounting's own cost from the eviction
// machinery and the access-time write that a configured cap adds.
func BenchmarkSetUntracked(b *testing.B) {
	s := New(256)
	val := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key:"+strconv.Itoa(i&1023), val, 0)
	}
}

func BenchmarkSetWithEvictionTracking(b *testing.B) {
	s := New(256)
	s.SetEvictionPolicy(PolicyAllKeysLRU)
	s.SetMaxMemory(1 << 30) // large enough that nothing is evicted
	val := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key:"+strconv.Itoa(i&1023), val, 0)
	}
}

func BenchmarkUsedMemory(b *testing.B) {
	s := New(256)
	for i := 0; i < 10000; i++ {
		s.Set("key:"+strconv.Itoa(i), []byte("value"), 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if s.UsedMemory() == 0 {
			b.Fatal("empty")
		}
	}
}

// TestUntrackedAccountingCostsNothing pins the free path: until something asks for a byte
// total, a write pays one atomic load on each side and nothing else -- no map lookup, no
// arithmetic, no counter. This is invariant 12's discipline applied to the accounting, and
// the reason the default configuration is exactly as fast as it was before any of it existed.
//
// It finishes by switching tracking on and checking the total becomes right, so what was
// measured is a disabled path and not a broken one.
func TestUntrackedAccountingCostsNothing(t *testing.T) {
	s := New(16)
	if s.MemoryTracked() {
		t.Fatal("a fresh store should not be accounting for bytes")
	}
	val := make([]byte, 64)
	s.Set("seed", val, 0)
	if got := s.mem.Load(); got != 0 {
		t.Errorf("the raw counter moved to %d while untracked; nothing should be counting", got)
	}
	// Every mutation family, to be sure none of them accounts behind the gate.
	if _, err := s.HSet("h", [2][]byte{[]byte("f"), val}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if _, err := s.RPush("l", val); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	if _, err := s.SAdd("st", "m"); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if _, _, err := s.ZAdd("z", "m", 1); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	if _, err := s.Append("seed", val); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := s.mem.Load(); got != 0 {
		t.Errorf("the raw counter moved to %d over five mutation families while untracked", got)
	}

	if n := testing.AllocsPerRun(500, func() {
		s.Set("bench", val, 0)
	}); n > 3 {
		// Three is what Store.Set allocated before the accounting existed (the entry, the
		// value copy, and the map's key); the point is that the accounting adds none.
		t.Errorf("Store.Set allocated %v times while untracked; want no more than the 3 the"+
			" store allocated before the accounting existed", n)
	}

	// Asking for the number is what starts the accounting, and what it answers is derived
	// from the dataset rather than from the counter that was not being kept.
	used := s.UsedMemory()
	if !s.MemoryTracked() {
		t.Fatal("reading UsedMemory should have started the accounting")
	}
	if want := s.exactMemory(); used != want {
		t.Errorf("the first UsedMemory = %d; want %d, recomputed from the values", used, want)
	}
	assertNoDrift(t, s, "after the first read switched accounting on")
}
