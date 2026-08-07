package store

import (
	"strconv"
	"testing"
)

// The write-path benchmarks. Each has an untracked and a tracked variant, because those are
// the two configurations that exist: a server nobody has given a byte budget (every existing
// deployment, and the default) pays one atomic load per mutation; a server with a budget pays
// for the accounting that budget is compared against.
//
// The untracked numbers are what must match the tree before any of this existed. The tracked
// numbers are the honest price of a byte budget.

func benchSet(b *testing.B, track bool) {
	s := New(256)
	if track {
		s.TrackMemory()
	}
	val := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key:"+strconv.Itoa(i&1023), val, 0)
	}
}

func BenchmarkWritePathSet(b *testing.B)        { benchSet(b, false) }
func BenchmarkWritePathSetTracked(b *testing.B) { benchSet(b, true) }

func benchAppend(b *testing.B, track bool) {
	s := New(256)
	if track {
		s.TrackMemory()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&255 == 0 {
			s.Del("app")
		}
		s.Append("app", []byte("0123456789")) //nolint:errcheck
	}
}

func BenchmarkWritePathAppend(b *testing.B)        { benchAppend(b, false) }
func BenchmarkWritePathAppendTracked(b *testing.B) { benchAppend(b, true) }

func benchHSet(b *testing.B, track bool) {
	s := New(256)
	if track {
		s.TrackMemory()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.HSet("h", [2][]byte{[]byte("f" + strconv.Itoa(i&255)), []byte("value")}) //nolint:errcheck
	}
}

func BenchmarkWritePathHSet(b *testing.B)        { benchHSet(b, false) }
func BenchmarkWritePathHSetTracked(b *testing.B) { benchHSet(b, true) }

func benchLPush(b *testing.B, track bool) {
	s := New(256)
	if track {
		s.TrackMemory()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&255 == 0 {
			s.Del("l")
		}
		s.LPush("l", []byte("element")) //nolint:errcheck
	}
}

func BenchmarkWritePathLPush(b *testing.B)        { benchLPush(b, false) }
func BenchmarkWritePathLPushTracked(b *testing.B) { benchLPush(b, true) }

func benchZAdd(b *testing.B, track bool) {
	s := New(256)
	if track {
		s.TrackMemory()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ZAdd("z", "m"+strconv.Itoa(i&255), float64(i)) //nolint:errcheck
	}
}

func BenchmarkWritePathZAdd(b *testing.B)        { benchZAdd(b, false) }
func BenchmarkWritePathZAddTracked(b *testing.B) { benchZAdd(b, true) }

func benchSetParallel(b *testing.B, track bool) {
	s := New(256)
	if track {
		s.TrackMemory()
	}
	val := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set("key:"+strconv.Itoa(i&1023), val, 0)
			i++
		}
	})
}

func BenchmarkWritePathSetParallel(b *testing.B)        { benchSetParallel(b, false) }
func BenchmarkWritePathSetParallelTracked(b *testing.B) { benchSetParallel(b, true) }
