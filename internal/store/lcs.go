package store

import "time"

// LCS: the longest common subsequence of two string values, and the aligned ranges that
// make it up.
//
// It is in the store rather than in the command layer because it has to read both values
// under one acquisition of the shard locks: computing it from two separate reads could
// align a prefix of one value against a suffix of the other from a later instant, and the
// match ranges it reported would then point into bytes that never coexisted.

// LCSMatch is one maximal run of bytes shared by the two values, as byte ranges into each.
// Both ends are inclusive, which is how Redis reports them.
type LCSMatch struct {
	AStart, AEnd int
	BStart, BEnd int
	Len          int
}

// LCSResult is what LCS computes: the subsequence itself, its length, and the aligned runs.
type LCSResult struct {
	Seq     []byte
	Len     int
	Matches []LCSMatch
}

// LCS computes the longest common subsequence of the string values at a and b.
//
// A missing key counts as the empty string, so the answer is empty rather than an error --
// which is what makes LCS usable for "how similar are these two, if they are both there".
// ErrNotString reports a key holding some other type; Redis words that refusal in terms of
// the *values* rather than as a WRONGTYPE, because the command needs two strings and does
// not care which of them was not one.
//
// Both keys are read under one acquisition of their shards' read locks, in shard-index
// order (invariant 8), so the two values are one consistent cut.
func (s *Store) LCS(a, b string, wantMatches bool) (LCSResult, error) {
	now := s.clock()
	unlock := s.rlockKeys(a, b)
	defer unlock()

	av, err := s.lcsOperand(a, now)
	if err != nil {
		return LCSResult{}, err
	}
	bv, err := s.lcsOperand(b, now)
	if err != nil {
		return LCSResult{}, err
	}
	return lcs(av, bv, wantMatches), nil
}

// lcsOperand reads one operand from an already-locked shard.
func (s *Store) lcsOperand(key string, now time.Time) ([]byte, error) {
	sh := s.getShard(key)
	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindString {
		return nil, ErrNotString
	}
	return e.str, nil
}

// lcs is the dynamic program itself: the classic O(len(a) * len(b)) table, walked back
// from the far corner to recover the subsequence and, when asked, the maximal runs.
//
// The table is int32 rather than int, which halves its memory on a 64-bit build. That
// matters because the table is the command's whole cost: two 1 MB values would need a
// terabyte of it, which is why Redis documents LCS as expensive and why a caller is
// expected to keep the operands small. (Redis allocates the same table and refuses only
// when the multiplication overflows.)
//
// The walk-back produces matches from the *end* of both strings towards the start, which is
// the order Redis reports them in -- so they are not reversed afterwards.
func lcs(a, b []byte, wantMatches bool) LCSResult {
	la, lb := len(a), len(b)
	if la == 0 || lb == 0 {
		return LCSResult{}
	}
	// One extra row and column of zeros, so the recurrence needs no bounds tests.
	stride := lb + 1
	table := make([]int32, (la+1)*stride)
	at := func(i, j int) int32 { return table[i*stride+j] }
	for i := 1; i <= la; i++ {
		for j := 1; j <= lb; j++ {
			if a[i-1] == b[j-1] {
				table[i*stride+j] = at(i-1, j-1) + 1
				continue
			}
			if x, y := at(i-1, j), at(i, j-1); x >= y {
				table[i*stride+j] = x
			} else {
				table[i*stride+j] = y
			}
		}
	}

	total := int(at(la, lb))
	out := LCSResult{Len: total}
	if total == 0 {
		return out
	}
	seq := make([]byte, total)
	idx := total
	// run is the index of the match currently being extended, or -1 between runs, so that
	// consecutive shared bytes are reported as one range rather than one per byte. An index
	// rather than a pointer, because appending to Matches may move the backing array.
	run := -1
	i, j := la, lb
	for i > 0 && j > 0 {
		if a[i-1] == b[j-1] {
			idx--
			seq[idx] = a[i-1]
			if wantMatches {
				if run < 0 {
					out.Matches = append(out.Matches, LCSMatch{AEnd: i - 1, BEnd: j - 1})
					run = len(out.Matches) - 1
				}
				m := &out.Matches[run]
				m.AStart, m.BStart = i-1, j-1
				m.Len++
			}
			i--
			j--
			continue
		}
		// The run is broken by any step that is not a match, whichever direction it takes.
		run = -1
		if at(i-1, j) > at(i, j-1) {
			i--
		} else {
			j--
		}
	}
	out.Seq = seq
	return out
}
