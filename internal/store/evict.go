package store

// The eviction policies, and the sampling that implements them.
//
// Redis's LRU is approximate on purpose: keeping a global access order would mean
// touching a shared list on every read, which is exactly the cost the sharded keyspace
// here exists to avoid. So a victim is chosen by sampling -- maxmemory-samples keys are
// examined and the best candidate among them is evicted -- and the quality of the choice
// is a tunable rather than a guarantee. This file does the same, with the one structural
// difference that a sample is drawn from a randomly chosen shard rather than from one
// global table, because that is where the keys are.
//
// The policies are Redis's eight, and the difference between the two families is not
// which comparison they make but which keys are *candidates*: an allkeys-* policy may
// evict anything, a volatile-* policy may only evict a key that carries a TTL. That
// distinction is what makes "nothing to evict" a state a volatile policy can be in while
// the keyspace is full, and a policy in that state must refuse writes exactly as
// noeviction does -- not spin looking for a candidate that does not exist. See
// EvictOne's report and the server's oomGate.

import (
	"math"
	"math/rand"
	"strings"
	"time"
)

// EvictionPolicy is Redis's maxmemory-policy: what to do when the byte budget is
// reached.
type EvictionPolicy uint8

// The policies, in the order Redis documents them. PolicyNoEviction is the zero value,
// so a Store nobody configured refuses to evict -- which is Redis's default and the safe
// direction, since the alternative is a server that silently discards data it was never
// told it could.
const (
	PolicyNoEviction EvictionPolicy = iota
	PolicyAllKeysLRU
	PolicyAllKeysLFU
	PolicyAllKeysRandom
	PolicyVolatileLRU
	PolicyVolatileLFU
	PolicyVolatileRandom
	PolicyVolatileTTL
)

var policyNames = [...]string{
	PolicyNoEviction:     "noeviction",
	PolicyAllKeysLRU:     "allkeys-lru",
	PolicyAllKeysLFU:     "allkeys-lfu",
	PolicyAllKeysRandom:  "allkeys-random",
	PolicyVolatileLRU:    "volatile-lru",
	PolicyVolatileLFU:    "volatile-lfu",
	PolicyVolatileRandom: "volatile-random",
	PolicyVolatileTTL:    "volatile-ttl",
}

func (p EvictionPolicy) String() string {
	if int(p) >= len(policyNames) {
		return "noeviction"
	}
	return policyNames[p]
}

// ParseEvictionPolicy resolves a policy name as CONFIG SET spells it.
//
// The match is case-insensitive but not otherwise forgiving, which is measured rather than
// assumed: redis 7.2 accepts `CONFIG SET maxmemory-policy ALLKEYS-LRU` and refuses
// `allkeys_lru`. Being stricter than Redis would refuse a value a client library sends;
// being looser would accept one it will later find real Redis rejecting.
func ParseEvictionPolicy(name string) (EvictionPolicy, bool) {
	name = strings.ToLower(name)
	for i, n := range policyNames {
		if n == name {
			return EvictionPolicy(i), true
		}
	}
	return PolicyNoEviction, false
}

// Evicts reports whether the policy is willing to remove a key at all. It is the
// question the OOM gate asks first, because noeviction's answer to a full keyspace is an
// error and not a victim.
func (p EvictionPolicy) Evicts() bool { return p != PolicyNoEviction }

// volatileOnly reports whether only keys carrying a TTL are candidates.
func (p EvictionPolicy) volatileOnly() bool {
	switch p {
	case PolicyVolatileLRU, PolicyVolatileLFU, PolicyVolatileRandom, PolicyVolatileTTL:
		return true
	}
	return false
}

// accessTracking says what the read path has to record for this policy: nothing, an
// access instant (LRU), or a decaying access counter (LFU). It is what gates the write
// touch does on every read -- see Store.touch.
type accessTracking int32

const (
	trackNothing accessTracking = iota
	trackAccessTime
	trackFrequency
)

func (p EvictionPolicy) tracking() accessTracking {
	switch p {
	case PolicyAllKeysLFU, PolicyVolatileLFU:
		return trackFrequency
	case PolicyAllKeysLRU, PolicyVolatileLRU, PolicyVolatileTTL:
		// volatile-ttl ranks by deadline, which is stored anyway, so it needs nothing --
		// but it is grouped with LRU here so that a key's access time is still recorded
		// under it, which is what OBJECT IDLETIME reports and what an operator switching
		// between the two policies expects to already be there.
		return trackAccessTime
	}
	return trackNothing
}

// --- the LFU counter ----------------------------------------------------------

// The LFU state is Redis's, byte for byte in meaning if not in layout: an 8-bit
// logarithmic access counter plus the minute it was last decayed, so that a key which
// was hot an hour ago does not outrank one that is hot now. Redis packs both into the
// 24-bit lru field it already had; here they are packed into one uint32 on the entry
// (16 bits of minute, 8 bits of counter), which is read and written atomically because
// the read path holds only the shard's shared lock.
//
// The counter is *logarithmic*, which is the whole point: a linear counter is dominated
// by whatever was accessed most since the server started, so a key that was hammered
// once and abandoned is never evicted. Incrementing with probability
// 1/((counter-init)*factor+1) makes the counter grow like the logarithm of the access
// count, so 255 can represent a million accesses per minute and a key that stops being
// used falls back down as the decay subtracts from it.
const (
	// lfuInitVal is what a new key starts at: high enough that a key created and read
	// once is not instantly the best victim (which would make LFU evict exactly the keys
	// a workload has just started using), and low enough to fall to zero quickly if
	// nothing touches it. Redis's value.
	lfuInitVal = 5
	lfuMaxVal  = 255
	// lfuStamped marks a word that has actually been written, so that a counter which has
	// decayed all the way to zero is not mistaken for a key nobody has stamped yet. Without
	// it the two states are the same bits for one minute in every 45 days (when the packed
	// minute is itself zero), and they mean opposite things to the sampler.
	lfuStamped = uint32(1) << 31

	// Redis's defaults for lfu-log-factor and lfu-decay-time, reported by CONFIG GET and
	// used unless an operator changes them.
	defaultLFULogFactor = 10
	defaultLFUDecayTime = 1
)

// lfuState packs a decay minute and a counter the way the entry stores them.
func lfuState(minute uint16, counter uint8) uint32 {
	return lfuStamped | uint32(minute)<<8 | uint32(counter)
}

func lfuSplit(v uint32) (minute uint16, counter uint8) {
	return uint16(v >> 8), uint8(v & 0xff)
}

// lfuMinute is the clock LFU decay is measured against: wall-clock minutes, truncated to
// 16 bits so it wraps every 45 days. A wrap makes one round of decay compute a negative
// elapsed time, which is read as "no decay" rather than as a huge one -- the same
// direction Redis handles its own 16-bit wrap in, because the failure that matters is
// wrongly zeroing every counter at once.
func lfuMinute(now time.Time) uint16 { return uint16(now.Unix() / 60) }

// lfuCounter is the counter a key would have now, given how long it is since it was last
// touched. decayMinutes is Redis's lfu-decay-time: how many minutes of idleness cost one
// point.
//
// An unstamped word reads as lfuInitVal, not as 0, and that is load-bearing rather than
// cosmetic: Redis creates every object at LFU_INIT_VAL for the specific reason that a key
// starting at zero is the most attractive victim in the keyspace, so an LFU policy would
// evict exactly the keys a workload has just started using. Starting above zero gives a new
// key time to prove itself, and the decay takes it back down if it does not.
func lfuCounter(v uint32, now uint16, decayMinutes int64) uint8 {
	if v&lfuStamped == 0 {
		return lfuInitVal
	}
	minute, counter := lfuSplit(v)
	if decayMinutes <= 0 {
		return counter
	}
	elapsed := int64(now) - int64(minute)
	if elapsed <= 0 {
		return counter
	}
	periods := elapsed / decayMinutes
	if periods <= 0 {
		return counter
	}
	if periods >= int64(counter) {
		return 0
	}
	return counter - uint8(periods)
}

// lfuAccess is what the read path records: the counter a key should hold after being
// touched now.
//
// An entry nobody has stamped yet is stamped at the initial value *without* an increment,
// which is where Redis puts a brand-new object -- createObject sets LFU_INIT_VAL and only
// lookupKey increments. Measured on redis 7.2: `SET k v` under allkeys-lfu then
// `OBJECT FREQ k` answers 5, and it answers 6 once the key has been read. Incrementing on
// creation would report 6 for a key nothing has ever read.
func lfuAccess(v uint32, now uint16, decayMinutes, logFactor int64) uint32 {
	if v&lfuStamped == 0 {
		return lfuState(now, lfuInitVal)
	}
	return lfuIncr(v, now, decayMinutes, logFactor)
}

// lfuIncr is one access to an already-stamped key: decay first, then increment with the
// logarithmic probability. logFactor is Redis's lfu-log-factor -- larger means a
// slower-growing counter, so more accesses are needed to distinguish two hot keys.
func lfuIncr(v uint32, now uint16, decayMinutes, logFactor int64) uint32 {
	counter := lfuCounter(v, now, decayMinutes)
	if counter < lfuMaxVal {
		base := float64(counter) - lfuInitVal
		if base < 0 {
			base = 0
		}
		p := 1.0 / (base*float64(logFactor) + 1)
		if rand.Float64() < p { //nolint:gosec // frequency sampling, not security
			counter++
		}
	}
	return lfuState(now, counter)
}

// Freq reports the LFU counter of key as OBJECT FREQ does, after applying the decay the
// key has earned by being idle. ok is false when the key is missing or expired.
func (s *Store) Freq(key string) (int64, bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, false
	}
	return int64(lfuCounter(e.freq.Load(), lfuMinute(now), s.lfuDecayTime.Load())), true
}

// SetFreq stamps key's LFU counter, which is what RESTORE's FREQ option carries: a key
// arriving from another node should keep the access frequency it had there rather than
// look brand new to the sampler. It reports whether the key was live.
func (s *Store) SetFreq(key string, counter int64) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := sh.liveEntry(key, now)
	if e == nil {
		return false
	}
	if counter < 0 {
		counter = 0
	}
	if counter > lfuMaxVal {
		counter = lfuMaxVal
	}
	e.freq.Store(lfuState(lfuMinute(now), uint8(counter)))
	return true
}

// --- the sampler --------------------------------------------------------------

// EvictOne removes one key chosen by the policy and reports whether it did. false means
// the policy found nothing it is allowed to evict, which is the caller's signal to
// refuse the write rather than to try again: a volatile-* policy over a keyspace with no
// volatile keys is in exactly that state, permanently, and a retry loop there would spin
// forever with the client waiting.
//
// The victim is sampled, not chosen globally: a random non-empty shard is picked and up
// to EvictionSamples of its keys are examined. The removal hook fires outside the shard
// lock, as every other removal here does, because the server takes propMu in it to
// propagate the DEL.
func (s *Store) EvictOne(policy EvictionPolicy) bool {
	if !policy.Evicts() {
		return false
	}
	samples := int(s.evictSamples.Load())
	if samples < 1 {
		samples = 1
	}
	now := s.clock()
	// Every shard is visited at most once, from a random start. The random start is what
	// spreads eviction across the keyspace instead of draining shard 0; visiting each shard
	// exactly once rather than drawing at random is what makes a false return *mean*
	// something.
	//
	// This used to draw 2N random shards, and that was a real defect rather than a slower
	// search: with one volatile key among 200 in 16 shards, 32 random draws miss its shard
	// about 13% of the time, so a volatile-* policy refused writes while a candidate was
	// sitting there. A refusal has to be evidence that there is nothing to evict, not
	// evidence that the dice went the other way.
	start := rand.Intn(len(s.shards)) //nolint:gosec // spreading eviction, not security
	for i := range s.shards {
		sh := s.shards[(start+i)%len(s.shards)]
		sh.mu.Lock()
		victim, found := sh.pickVictim(s, policy, now, samples)
		if !found {
			sh.mu.Unlock()
			continue
		}
		e := sh.data[victim]
		delete(sh.data, victim)
		s.uncharge(sh, victim, e)
		sh.mu.Unlock()
		s.evicted.Add(1)
		s.notifyRemoved(victim, true) // eviction: outside the shard lock
		return true
	}
	return false
}

// pickVictim examines the shard's keys and returns the best candidate under the policy.
// Caller holds sh's write lock.
//
// Go randomises a map's iteration start, so the window this walks is a random one -- the
// same property Redis gets from dictGetSomeKeys, reached differently. The sample budget
// counts *candidates*, not keys looked at, which is what makes the two policy families
// behave the same way: an allkeys-* policy stops after `samples` keys, and a volatile-*
// policy stops after `samples` keys that carry a deadline.
//
// A volatile-* policy is willing to walk the whole shard to find those candidates, and that
// is deliberate. The alternative -- a scan budget -- means giving up on a shard that does
// hold candidates, and giving up is indistinguishable from "there are none", which is the
// answer that refuses a client's write. So the cost is paid where it is unavoidable and
// avoided where it is not: the volatile count below rules out a shard with no candidates in
// O(1), so the only shards ever walked in full are shards that really do hold a key the
// policy may take. Redis reaches the same place with a separate expires table; this reaches
// it with a counter maintained by the same settle that maintains the byte total.
func (sh *shard) pickVictim(s *Store, policy EvictionPolicy, now time.Time, samples int) (string, bool) {
	if len(sh.data) == 0 {
		return "", false
	}
	volatileOnly := policy.volatileOnly()
	// The candidate count is a fast negative, not a precondition: when the byte accounting is
	// running it rules out a shard holding no volatile key in O(1), and when it is not (a
	// server bounded only by maxkeys) the walk below simply finds no candidate and reports
	// the same thing after a full scan. Treating the count as authoritative in both cases
	// would be a correctness bug rather than a slow path -- it is zero when nothing is
	// maintaining it, so a volatile-* policy would silently stop evicting.
	if volatileOnly && s.memTrack.Load() && sh.volatile == 0 {
		return "", false
	}
	minute := lfuMinute(now)
	decay := s.lfuDecayTime.Load()

	var victim string
	found := false
	var best int64 = math.MaxInt64
	sampled := 0
	for k, e := range sh.data {
		hasTTL := !e.expireAt.IsZero()
		if volatileOnly && !hasTTL {
			continue
		}
		// An entry already past its deadline is the best possible victim: it is dead
		// weight, and removing it costs nothing anyone can observe.
		if e.expired(now) {
			return k, true
		}
		var rank int64
		switch policy {
		case PolicyAllKeysRandom, PolicyVolatileRandom:
			// Map iteration order is the randomness; the first candidate seen is the pick.
			return k, true
		case PolicyAllKeysLFU, PolicyVolatileLFU:
			rank = int64(lfuCounter(e.freq.Load(), minute, decay))
		case PolicyVolatileTTL:
			// The soonest deadline first, which is what volatile-ttl means: the key that
			// was going to go anyway.
			rank = e.expireAt.UnixNano()
		default:
			// LRU: the oldest access instant. A key that has never been touched since
			// tracking was enabled reads 0 and so sorts oldest, which is the right
			// direction -- nothing is known to have used it.
			rank = e.atime.Load()
		}
		if rank < best {
			best, victim, found = rank, k, true
		}
		if sampled++; sampled >= samples {
			break
		}
	}
	return victim, found
}

// --- configuration ------------------------------------------------------------

// SetMaxMemory records the byte budget the *server* holds this database's keyspace within
// (0 = unbounded).
//
// The budget is server-wide, not per database, so every database is given the same value
// and CONFIG GET reads it back from database 0 -- the discipline maxkeys and the encoding
// thresholds already use, and the reason there is exactly one spelling of the number rather
// than a store copy and a server copy that could disagree. What the store does with it is
// decide whether the read path has to record anything for the sampler to rank by; the
// comparison against the total, and the decision to refuse a command, are the server's,
// because only the server can see every database at once.
func (s *Store) SetMaxMemory(n int64) {
	if n < 0 {
		n = 0
	}
	if n > 0 {
		// A budget is only meaningful against a maintained total, and the total is only
		// maintained once something asks for it. Asking here, before the limit is published,
		// means the first comparison any write makes is against a figure derived from the
		// dataset rather than against a counter that started late.
		s.TrackMemory()
	}
	s.maxMemory.Store(n)
	s.refreshTracking()
}

// MaxMemory reports the recorded budget (0 = unbounded).
func (s *Store) MaxMemory() int64 { return s.maxMemory.Load() }

// SetEvictionPolicy selects what eviction does when the budget is reached, and records
// that a policy was chosen explicitly -- which is what stops the server's derived default
// from overriding it. See Server.evictionPolicy.
func (s *Store) SetEvictionPolicy(p EvictionPolicy) {
	s.policy.Store(int32(p))
	s.policySet.Store(true)
	s.refreshTracking()
}

// EvictionPolicyConfigured reports whether a policy was ever explicitly selected, as
// against left at the zero value.
func (s *Store) EvictionPolicyConfigured() bool { return s.policySet.Load() }

// EvictionPolicy reports the configured policy.
func (s *Store) EvictionPolicy() EvictionPolicy { return EvictionPolicy(s.policy.Load()) }

// SetEvictionSamples sets how many keys the sampler examines before choosing a victim --
// Redis's maxmemory-samples. It is a real knob rather than a reported constant: the
// number CONFIG GET answers with is the number pickVictim uses, or it would be a lie.
func (s *Store) SetEvictionSamples(n int) {
	if n < 1 {
		n = 1
	}
	s.evictSamples.Store(int64(n))
}

// EvictionSampleCount reports the configured sample size, for CONFIG GET.
func (s *Store) EvictionSampleCount() int { return int(s.evictSamples.Load()) }

// SetLFUParams sets Redis's lfu-log-factor and lfu-decay-time: how slowly the access
// counter grows, and how many idle minutes cost it a point.
func (s *Store) SetLFUParams(logFactor, decayTime int64) {
	if logFactor < 0 {
		logFactor = 0
	}
	if decayTime < 0 {
		decayTime = 0
	}
	s.lfuLogFactor.Store(logFactor)
	s.lfuDecayTime.Store(decayTime)
}

// LFUParams reports the configured lfu-log-factor and lfu-decay-time.
func (s *Store) LFUParams() (logFactor, decayTime int64) {
	return s.lfuLogFactor.Load(), s.lfuDecayTime.Load()
}

// refreshTracking recomputes what the read path has to record, so that touch can decide
// with a single atomic load instead of consulting three settings.
//
// The read path records nothing at all unless something would use it: a key cap (which
// evicts by approximate LRU), or a byte budget under a policy that ranks by access. That
// is what keeps the default configuration's reads free of the atomic write -- the same
// discipline the observability hooks follow.
func (s *Store) refreshTracking() {
	mode := trackNothing
	if s.maxMemory.Load() > 0 {
		mode = s.EvictionPolicy().tracking()
	}
	if mode == trackNothing && s.maxKeys.Load() > 0 {
		mode = trackAccessTime
	}
	s.access.Store(int32(mode))
}
