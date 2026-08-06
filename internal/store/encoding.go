package store

import "sync/atomic"

// The thresholds at which Redis switches a value's internal representation, and
// therefore the thresholds this store answers OBJECT ENCODING from.
//
// They are configuration rather than constants because they are configuration in
// Redis, and because a client that changes one expects OBJECT ENCODING to change with
// it -- Redis's own type tests are written as `foreach encoding {listpack hashtable}`
// loops that set the threshold and then assert the encoding, so a server whose
// thresholds are fixed cannot be checked against them at all.
//
// This store keeps one representation per type throughout, so a threshold does not
// change how a value is stored; what it changes is the *name* reported for it, which
// is what a client asks in order to decide whether a value is still cheap to scan.
// See Store.Encoding.

// EncodingParam names one of those thresholds. It is an index into a fixed array
// rather than a map key so that reading one costs an atomic load and no hashing:
// Encoding is on the read path of OBJECT ENCODING, which Redis's test suite calls
// after nearly every write.
type EncodingParam int

// The thresholds, under the names Redis gives them (the `ziplist` spellings are
// aliases the server layer maps onto these).
const (
	HashMaxListpackEntries EncodingParam = iota
	HashMaxListpackValue
	ListMaxListpackSize
	SetMaxIntsetEntries
	SetMaxListpackEntries
	SetMaxListpackValue
	ZSetMaxListpackEntries
	ZSetMaxListpackValue
	HLLSparseMaxBytes
	numEncodingParams
)

// encodingDefaults are Redis 7.2's own defaults, checked against a live
// redis:7.2-alpine with CONFIG GET rather than taken from documentation. Note that
// hash-max-listpack-entries is 512 (not 128, which is the sorted set's) and that
// list-max-listpack-size defaults to a *negative* value, meaning "bound the listpack
// by bytes, not by element count" -- see listEncodingName.
var encodingDefaults = [numEncodingParams]int64{
	HashMaxListpackEntries: 512,
	HashMaxListpackValue:   64,
	ListMaxListpackSize:    -2,
	SetMaxIntsetEntries:    512,
	SetMaxListpackEntries:  128,
	SetMaxListpackValue:    64,
	ZSetMaxListpackEntries: 128,
	ZSetMaxListpackValue:   64,
	HLLSparseMaxBytes:      3000,
}

// encodingLimits holds one store's thresholds. Each is an atomic so CONFIG SET can
// change it while commands are running, without a lock on a path that every
// OBJECT ENCODING and every PFADD reads.
type encodingLimits [numEncodingParams]atomic.Int64

func (l *encodingLimits) reset() {
	for i := range l {
		l[i].Store(encodingDefaults[i])
	}
}

// SetEncodingLimit changes one threshold. Callers validate the range; a negative value
// is meaningful for ListMaxListpackSize and is clamped to zero for every other
// parameter, where Redis treats it the same way.
func (s *Store) SetEncodingLimit(p EncodingParam, v int64) {
	if v < 0 && p != ListMaxListpackSize {
		v = 0
	}
	s.encoding[p].Store(v)
}

// EncodingLimit reports one threshold, for CONFIG GET.
func (s *Store) EncodingLimit(p EncodingParam) int64 { return s.encoding[p].Load() }

// listpackSizeLimits are the byte budgets Redis's negative list-max-listpack-size
// values name: -1 is 4 KB, -2 (the default) 8 KB, and so on up to -5. They are
// quicklist's optimization_level table.
var listpackSizeLimits = [5]int64{4096, 8192, 16384, 32768, 65536}

// listExceedsListpack reports whether a list of count elements totalling bytes has
// outgrown the listpack representation, applying Redis's two-sided rule:
//
//   - a negative list-max-listpack-size names a byte budget and nothing else, so a
//     list of a million one-byte elements stays a listpack while one holding a single
//     16 KB element does not;
//   - a non-negative one names an element count, but the 8 KB safety limit still
//     applies on top of it, because that is the largest listpack quicklist will build
//     whatever the count says.
func listExceedsListpack(fill int64, count int, bytes int64) bool {
	if fill < 0 {
		i := -fill - 1
		if i >= int64(len(listpackSizeLimits)) {
			i = int64(len(listpackSizeLimits)) - 1
		}
		return bytes > listpackSizeLimits[i]
	}
	return int64(count) > fill || bytes > listpackSizeLimits[1]
}
