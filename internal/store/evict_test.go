package store

import (
	"strconv"
	"testing"
	"time"
)

// TestEvictionPolicyNames pins the eight names CONFIG GET reports and CONFIG SET accepts,
// with the measured redis 7.2 behaviour beside each: the names are matched
// case-insensitively but not otherwise loosely.
func TestEvictionPolicyNames(t *testing.T) {
	cases := []struct {
		in   string
		want EvictionPolicy
		ok   bool
	}{
		// Measured on redis 7.2: all eight are accepted and read back verbatim.
		{"noeviction", PolicyNoEviction, true},
		{"allkeys-lru", PolicyAllKeysLRU, true},
		{"allkeys-lfu", PolicyAllKeysLFU, true},
		{"allkeys-random", PolicyAllKeysRandom, true},
		{"volatile-lru", PolicyVolatileLRU, true},
		{"volatile-lfu", PolicyVolatileLFU, true},
		{"volatile-random", PolicyVolatileRandom, true},
		{"volatile-ttl", PolicyVolatileTTL, true},
		// Measured: `CONFIG SET maxmemory-policy ALLKEYS-LRU` answers +OK on redis 7.2 and
		// reads back as allkeys-lru, so the match is case-insensitive.
		{"ALLKEYS-LRU", PolicyAllKeysLRU, true},
		{"Volatile-TTL", PolicyVolatileTTL, true},
		// Measured: these are refused with "argument(s) must be one of the following: ...".
		{"allkeys_lru", PolicyNoEviction, false},
		{"lru", PolicyNoEviction, false},
		{"", PolicyNoEviction, false},
		{"allkeys-lru ", PolicyNoEviction, false},
	}
	for _, tc := range cases {
		got, ok := ParseEvictionPolicy(tc.in)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("ParseEvictionPolicy(%q) = (%v, %v); want (%v, %v)",
				tc.in, got, ok, tc.want, tc.ok)
		}
	}
	// And every policy renders back to the name it was parsed from, since CONFIG GET and
	// INFO both report through String.
	for i, name := range policyNames {
		if got := EvictionPolicy(i).String(); got != name {
			t.Errorf("EvictionPolicy(%d).String() = %q; want %q", i, got, name)
		}
	}
}

// TestEvictOneRefusesWithoutCandidates covers the two states that must end in a refusal
// rather than a search: noeviction, and a volatile-* policy over a keyspace holding no
// volatile keys.
//
// The second is the one that matters, and it is why EvictOne reports a bool rather than
// looping: a volatile policy over 200 persistent keys has nothing it may ever evict, so a
// caller that retried would spin forever with a client waiting on the answer.
func TestEvictOneRefusesWithoutCandidates(t *testing.T) {
	s := New(16)
	for i := 0; i < 200; i++ {
		s.Set("persistent:"+strconv.Itoa(i), []byte("v"), 0)
	}

	if s.EvictOne(PolicyNoEviction) {
		t.Error("noeviction evicted a key; it must never evict")
	}
	for _, p := range []EvictionPolicy{
		PolicyVolatileLRU, PolicyVolatileLFU, PolicyVolatileRandom, PolicyVolatileTTL,
	} {
		if s.EvictOne(p) {
			t.Errorf("%s evicted a key from a keyspace with no volatile keys", p)
		}
	}
	if got := s.Len(); got != 200 {
		t.Fatalf("Len = %d; want 200 (nothing should have been evicted)", got)
	}
	if got := s.Evicted(); got != 0 {
		t.Errorf("Evicted = %d; want 0", got)
	}

	// Give one key a deadline and every volatile policy finds it.
	s.Set("volatile", []byte("v"), time.Hour)
	for _, p := range []EvictionPolicy{
		PolicyVolatileLRU, PolicyVolatileLFU, PolicyVolatileRandom, PolicyVolatileTTL,
	} {
		s.Set("volatile", []byte("v"), time.Hour)
		if !s.EvictOne(p) {
			t.Errorf("%s did not evict the one volatile key", p)
		}
		if s.Exists("volatile") {
			t.Errorf("%s evicted something, but not the only volatile key", p)
		}
	}
	// An allkeys policy is not restricted that way.
	if !s.EvictOne(PolicyAllKeysRandom) {
		t.Error("allkeys-random found nothing to evict in a keyspace of 200 keys")
	}
}

// TestEvictionPolicyPicksTheRightVictim drives each ranking policy against a keyspace
// arranged so that exactly one key is the correct answer. One shard, so sampling sees
// everything and the choice is deterministic rather than approximate -- the approximation
// is in *which* keys are sampled, not in how the sampled ones are compared.
func TestEvictionPolicyPicksTheRightVictim(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	setup := func(policy EvictionPolicy) *Store {
		s := New(1)
		s.SetClock(func() time.Time { return now })
		s.SetEvictionPolicy(policy)
		s.SetMaxMemory(1 << 30) // turns access tracking on without evicting anything
		return s
	}

	t.Run("allkeys-lru evicts the oldest access", func(t *testing.T) {
		s := setup(PolicyAllKeysLRU)
		for _, k := range []string{"old", "mid", "new"} {
			s.Set(k, []byte("v"), 0)
		}
		sh := s.shards[0]
		sh.data["old"].atime.Store(10)
		sh.data["mid"].atime.Store(20)
		sh.data["new"].atime.Store(30)
		if !s.EvictOne(PolicyAllKeysLRU) {
			t.Fatal("nothing evicted")
		}
		if s.Exists("old") {
			t.Error("old was least recently used and should have gone")
		}
		if !s.Exists("mid") || !s.Exists("new") {
			t.Error("the more recently used keys should have survived")
		}
	})

	t.Run("volatile-ttl evicts the soonest deadline", func(t *testing.T) {
		s := setup(PolicyVolatileTTL)
		s.Set("persistent", []byte("v"), 0)
		s.Set("late", []byte("v"), time.Hour)
		s.Set("soon", []byte("v"), time.Minute)
		if !s.EvictOne(PolicyVolatileTTL) {
			t.Fatal("nothing evicted")
		}
		if s.Exists("soon") {
			t.Error("soon had the nearest deadline and should have gone")
		}
		if !s.Exists("late") || !s.Exists("persistent") {
			t.Error("volatile-ttl took the wrong key")
		}
	})

	t.Run("allkeys-lfu evicts the least frequently used", func(t *testing.T) {
		s := setup(PolicyAllKeysLFU)
		// lfu-log-factor 0 makes the counter linear in the access count, which is a real
		// configuration (Redis accepts it and documents it as exactly that) and the only way
		// to test the *ranking* without also testing the sampling of a probability. At the
		// default factor of 10 the counter is deliberately almost flat -- measured on redis
		// 7.2, 100 reads of a key moves OBJECT FREQ from 5 to 6 and 10000 reads take it to
		// 19 -- so two keys read 100 and 25 times are indistinguishable, by design. An
		// earlier version of this test asserted they could be told apart, which was a claim
		// about this implementation that Redis's own numbers refute.
		s.SetLFUParams(0, 0)
		for _, k := range []string{"cold", "warm", "hot"} {
			s.Set(k, []byte("v"), 0)
		}
		for i := 0; i < 100; i++ {
			s.Get("hot")
			if i%4 == 0 {
				s.Get("warm")
			}
		}
		coldFreq, _ := s.Freq("cold")
		warmFreq, _ := s.Freq("warm")
		hotFreq, _ := s.Freq("hot")
		if coldFreq >= warmFreq || warmFreq >= hotFreq {
			t.Fatalf("LFU counters did not order by use: cold=%d warm=%d hot=%d",
				coldFreq, warmFreq, hotFreq)
		}
		if !s.EvictOne(PolicyAllKeysLFU) {
			t.Fatal("nothing evicted")
		}
		if s.Exists("cold") {
			t.Error("cold was never read and should have gone first")
		}
		if !s.Exists("hot") {
			t.Error("hot was read a hundred times and should have survived")
		}
	})

	t.Run("volatile-ttl takes the nearest of several deadlines", func(t *testing.T) {
		// The distinguishing test for volatile-ttl: with a single volatile key any policy
		// looks right, and a volatile-ttl that returned an arbitrary candidate would be
		// volatile-random under another name -- which would make the config value a lie.
		s := setup(PolicyVolatileTTL)
		for i := 0; i < 20; i++ {
			s.Set("p"+strconv.Itoa(i), []byte("v"), 0) // persistent decoys
		}
		for i := 1; i <= 8; i++ {
			s.Set("v"+strconv.Itoa(i), []byte("v"), time.Duration(i)*time.Hour)
		}
		for want := 1; want <= 8; want++ {
			if !s.EvictOne(PolicyVolatileTTL) {
				t.Fatalf("nothing evicted with %d volatile keys left", 9-want)
			}
			if s.Exists("v" + strconv.Itoa(want)) {
				t.Fatalf("v%d had the nearest deadline and should have gone", want)
			}
		}
		// And the persistent decoys are all still there: a volatile-* policy must never
		// take a key an operator marked permanent.
		for i := 0; i < 20; i++ {
			if !s.Exists("p" + strconv.Itoa(i)) {
				t.Errorf("volatile-ttl evicted the persistent key p%d", i)
			}
		}
	})

	t.Run("an expired entry is always the best victim", func(t *testing.T) {
		s := setup(PolicyAllKeysLRU)
		s.SetActiveExpire(false)
		s.Set("fresh", []byte("v"), 0)
		s.Set("stale", []byte("v"), time.Second)
		s.shards[0].data["fresh"].atime.Store(1) // fresh looks the least recently used
		now = now.Add(time.Minute)               // but stale is dead weight
		defer func() { now = now.Add(-time.Minute) }()
		if !s.EvictOne(PolicyAllKeysLRU) {
			t.Fatal("nothing evicted")
		}
		if !s.Exists("fresh") {
			t.Error("an entry past its deadline should be taken ahead of a live one")
		}
	})
}

// TestEvictionSamplesIsUsed is the test that keeps maxmemory-samples from being a lie. A
// reported knob that the sampler ignores is worse than no knob, because an operator tunes
// it and measures no change.
//
// It is measured behaviourally rather than by reading the field back: with a sample of 1 the
// sampler takes the first key its (randomised) map walk reaches, so over many runs it picks
// the true least-recently-used key only about as often as chance allows; with a sample
// covering the whole shard it picks it every time.
func TestEvictionSamplesIsUsed(t *testing.T) {
	const keys = 50
	build := func(samples int) *Store {
		s := New(1)
		s.SetEvictionPolicy(PolicyAllKeysLRU)
		s.SetMaxMemory(1 << 30)
		s.SetEvictionSamples(samples)
		for i := 0; i < keys; i++ {
			k := "k" + strconv.Itoa(i)
			s.Set(k, []byte("v"), 0)
			s.shards[0].data[k].atime.Store(int64(1000 + i))
		}
		return s
	}
	if got := build(7).EvictionSampleCount(); got != 7 {
		t.Fatalf("EvictionSampleCount = %d; want 7", got)
	}

	hits := func(samples, rounds int) int {
		n := 0
		for r := 0; r < rounds; r++ {
			s := build(samples)
			if !s.EvictOne(PolicyAllKeysLRU) {
				t.Fatal("nothing evicted")
			}
			if !s.Exists("k0") { // k0 has the oldest access time
				n++
			}
		}
		return n
	}
	const rounds = 60
	wide, narrow := hits(keys, rounds), hits(1, rounds)
	if wide != rounds {
		t.Errorf("with a sample of %d the true LRU key was chosen %d/%d times; want every"+
			" time -- the sample covers the whole shard", keys, wide, rounds)
	}
	if narrow >= rounds {
		t.Errorf("with a sample of 1 the true LRU key was chosen %d/%d times; the sample"+
			" size is not reaching the sampler", narrow, rounds)
	}
	t.Logf("true-LRU victim chosen %d/%d times with samples=%d, %d/%d with samples=1",
		wide, rounds, keys, narrow, rounds)
}

// TestLFUCounterDecays covers the half of LFU that makes it a *frequency* estimate rather
// than a lifetime total: a key that was hot an hour ago must not outrank one that is hot
// now. Without the decay, LFU never evicts anything that was ever popular.
func TestLFUCounterDecays(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := New(1)
	s.SetClock(func() time.Time { return now })
	s.SetEvictionPolicy(PolicyAllKeysLFU)
	s.SetMaxMemory(1 << 30)

	s.Set("k", []byte("v"), 0)
	// A fresh key reads 5, not 0: measured on redis 7.2, OBJECT FREQ on a key just SET
	// under allkeys-lfu answers 5. A counter starting at zero would make every
	// newly-written key the most attractive victim in the keyspace, so an LFU policy would
	// evict exactly what the workload had just started using.
	if got, _ := s.Freq("k"); got != lfuInitVal {
		t.Fatalf("Freq on a fresh key = %d; want the initial %d", got, lfuInitVal)
	}
	// Linear counter, so the climb is deterministic and the decay below is measured against
	// a known value rather than against a probability.
	s.SetLFUParams(0, 1)
	for i := 0; i < 200; i++ {
		s.Get("k")
	}
	hot, ok := s.Freq("k")
	if !ok {
		t.Fatal("Freq reported the key missing")
	}
	if hot <= lfuInitVal {
		t.Fatalf("Freq after 200 reads = %d; want more than the initial %d", hot, lfuInitVal)
	}

	// lfu-decay-time defaults to one minute per point, so ten idle minutes cost ten.
	now = now.Add(10 * time.Minute)
	cooled, _ := s.Freq("k")
	if cooled != hot-10 {
		t.Errorf("Freq after 10 idle minutes = %d; want %d (one point per minute at the"+
			" default lfu-decay-time)", cooled, hot-10)
	}

	// Long enough idle and it bottoms out at zero rather than wrapping.
	now = now.Add(10 * time.Hour)
	if got, _ := s.Freq("k"); got != 0 {
		t.Errorf("Freq after 10 idle hours = %d; want 0", got)
	}

	// Decay turned off makes the counter a lifetime total, which is what lfu-decay-time 0
	// means in Redis.
	s.SetLFUParams(0, 0)
	for i := 0; i < 200; i++ {
		s.Get("k")
	}
	before, _ := s.Freq("k")
	now = now.Add(24 * time.Hour)
	if after, _ := s.Freq("k"); after != before {
		t.Errorf("with lfu-decay-time 0, Freq moved from %d to %d over a day", before, after)
	}
}

// TestFreqIsNotTrackedWithoutAnLFUPolicy pins the gate: the counter is only maintained when
// a policy would read it, so the read path of a default server does not pay for it. It is
// the eviction-side counterpart of TestObservabilityCostsNothingWhenUnused.
func TestFreqIsNotTrackedWithoutAnLFUPolicy(t *testing.T) {
	s := New(1)
	s.SetLFUParams(0, 0) // linear, so a single tracked read would be visible
	s.Set("k", []byte("v"), 0)
	for i := 0; i < 100; i++ {
		s.Get("k")
	}
	// The counter reads as the initial value because nothing has stamped it -- not as 0,
	// which is what a fully-decayed key reads as. What "not tracked" means is that a hundred
	// reads did not move it.
	if got, _ := s.Freq("k"); got != lfuInitVal {
		t.Errorf("Freq with no policy configured = %d; want the untouched initial %d",
			got, lfuInitVal)
	}
	// And an LRU policy tracks the access time instead, which is what OBJECT IDLETIME
	// reports -- not the frequency.
	s.SetEvictionPolicy(PolicyAllKeysLRU)
	s.SetMaxMemory(1 << 30)
	for i := 0; i < 100; i++ {
		s.Get("k")
	}
	if got, _ := s.Freq("k"); got != lfuInitVal {
		t.Errorf("Freq under allkeys-lru = %d; want the untouched initial %d", got, lfuInitVal)
	}
	if _, ok := s.IdleSeconds("k"); !ok {
		t.Error("IdleSeconds should report on a live key")
	}
}

// TestSetFreqStamps covers RESTORE's FREQ operand: a key arriving from another node should
// carry the access frequency it had there rather than look brand new to the sampler.
func TestSetFreqStamps(t *testing.T) {
	s := New(1)
	s.SetEvictionPolicy(PolicyAllKeysLFU)
	s.SetMaxMemory(1 << 30)
	s.Set("k", []byte("v"), 0)

	cases := []struct{ in, want int64 }{
		{0, 0}, // an explicit 0 is stamped, so it stays 0 rather than reading as the initial 5
		{7, 7},
		{255, 255},
		// Measured on redis 7.2: RESTORE refuses a FREQ outside 0..255 up front, so the
		// clamp here is a second line of defence rather than the interface.
		{300, 255},
		{-1, 0},
	}
	for _, tc := range cases {
		if !s.SetFreq("k", tc.in) {
			t.Fatalf("SetFreq(%d) reported the key missing", tc.in)
		}
		if got, _ := s.Freq("k"); got != tc.want {
			t.Errorf("SetFreq(%d) then Freq = %d; want %d", tc.in, got, tc.want)
		}
	}
	if s.SetFreq("missing", 5) {
		t.Error("SetFreq on a missing key reported success")
	}
}

// TestEvictionIsChargedBack is the accounting half of eviction: the bytes an evicted key
// held must stop being counted, or a server that evicted its way to a stable keyspace would
// still report itself over the budget and keep evicting until it was empty.
func TestEvictionIsChargedBack(t *testing.T) {
	s := New(4)
	s.SetEvictionPolicy(PolicyAllKeysRandom)
	s.SetMaxMemory(1 << 30) // also switches the byte accounting on
	for i := 0; i < 100; i++ {
		s.Set("k"+strconv.Itoa(i), make([]byte, 512), 0)
	}
	full := s.UsedMemory()
	for i := 0; i < 50; i++ {
		if !s.EvictOne(PolicyAllKeysRandom) {
			t.Fatalf("eviction %d found nothing", i)
		}
	}
	after := s.UsedMemory()
	if after >= full {
		t.Errorf("UsedMemory did not fall after 50 evictions: %d -> %d", full, after)
	}
	if want := s.exactMemory(); after != want {
		t.Errorf("UsedMemory after eviction = %d; full recomputation = %d", after, want)
	}
	if got := s.Evicted(); got != 50 {
		t.Errorf("Evicted = %d; want 50", got)
	}
}

// TestMaxKeysStillEvictsUnderNoeviction pins the boundary between the two mechanisms. A key
// cap is a separate, older feature and an instruction to bound the keyspace; answering it
// with OOM errors -- which is what noeviction means for maxmemory -- would silently retire
// it. So a cap evicts by approximate LRU when the policy would not evict at all, and by the
// configured policy when it would.
func TestMaxKeysStillEvictsUnderNoeviction(t *testing.T) {
	s := New(64)
	s.SetEvictionPolicy(PolicyNoEviction)
	s.SetMaxKeys(8)
	for i := 0; i < 200; i++ {
		s.Set("k"+strconv.Itoa(i), []byte("v"), 0)
	}
	s.EvictToLimit()
	if got := s.Len(); got != 8 {
		t.Fatalf("Len = %d; want 8 -- a maxkeys cap must still be enforced under"+
			" maxmemory-policy noeviction", got)
	}

	// And with a policy that does evict, the cap uses that policy's ranking.
	s2 := New(1)
	s2.SetEvictionPolicy(PolicyVolatileTTL)
	s2.SetMaxMemory(1 << 30)
	s2.SetMaxKeys(2)
	s2.Set("persistent", []byte("v"), 0)
	s2.Set("late", []byte("v"), time.Hour)
	s2.Set("soon", []byte("v"), time.Minute)
	s2.EvictToLimit()
	if s2.Exists("soon") {
		t.Error("with volatile-ttl the cap should have taken the nearest deadline")
	}
	if !s2.Exists("persistent") {
		t.Error("the persistent key is not a volatile-ttl candidate and should survive")
	}
}
