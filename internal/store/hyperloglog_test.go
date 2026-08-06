package store

import (
	"fmt"
	"math"
	"strconv"
	"testing"
)

// TestHLLSelfTest runs the implementation's own packing checks, which is where an
// off-by-one in the 6-bit dense layout would show up.
func TestHLLSelfTest(t *testing.T) {
	if err := HLLSelfTest(); err != nil {
		t.Fatal(err)
	}
}

// TestHLLFormatIsRedisCompatible pins every part of the layout a Redis client (or real
// Redis itself) would read. A sketch whose header or packing drifted would count
// correctly here and be unreadable everywhere else, which is the failure this test
// exists to make loud.
func TestHLLFormatIsRedisCompatible(t *testing.T) {
	s := New(4)
	if _, err := s.PFAdd("h", [][]byte{[]byte("a")}); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.GetString("h")
	if err != nil || !ok {
		t.Fatalf("the sketch is not readable as a string: %v", err)
	}
	if string(v[:4]) != "HYLL" {
		t.Errorf("magic = %q; want HYLL", v[:4])
	}
	if v[4] != hllSparse {
		t.Errorf("a new sketch has encoding %d; want sparse (%d)", v[4], hllSparse)
	}
	if len(v) < hllHdrSize {
		t.Fatalf("the value is %d bytes; the header alone is %d", len(v), hllHdrSize)
	}
	// Every write leaves the cached cardinality stale, so a Redis client recomputes
	// rather than trusting a number this server did not compute.
	if hllValidCache(v) {
		t.Error("PFADD left the cached cardinality marked valid")
	}

	// A sketch large enough to exceed the sparse budget is promoted to dense, at exactly
	// the length Redis's geometry requires.
	elems := make([][]byte, 0, 5000)
	for i := 0; i < 5000; i++ {
		elems = append(elems, []byte("element-"+strconv.Itoa(i)))
	}
	if _, err := s.PFAdd("dense", elems); err != nil {
		t.Fatal(err)
	}
	v, _, _ = s.GetString("dense")
	if v[4] != hllDense {
		t.Fatalf("a 5000-element sketch has encoding %d; want dense", v[4])
	}
	if len(v) != hllDenseSize {
		t.Errorf("a dense sketch is %d bytes; Redis's geometry requires %d", len(v), hllDenseSize)
	}
	if hllDenseSize != 12304 {
		t.Errorf("hllDenseSize = %d; Redis's is 12304", hllDenseSize)
	}
}

// TestHLLAccuracy is the statistical check: over a large cardinality the estimate has to
// stay inside the error HyperLogLog promises.
//
// With 14 register bits the standard error is 1.04/sqrt(2^14) = 0.81%, so a correct
// implementation lands within about 2% (2.5 sigma) essentially always. The bound asserted
// here is 2%, which is loose enough not to be flaky and tight enough that a broken
// estimator -- or a broken register packing, which shows up as a systematically low count
// -- cannot pass.
func TestHLLAccuracy(t *testing.T) {
	s := New(4)
	const total = 200000
	const batch = 1000

	worst := 0.0
	for n := batch; n <= total; n += batch {
		elems := make([][]byte, 0, batch)
		for i := n - batch; i < n; i++ {
			elems = append(elems, []byte("user:"+strconv.Itoa(i)))
		}
		if _, err := s.PFAdd("card", elems); err != nil {
			t.Fatal(err)
		}
		got, err := s.PFCount([]string{"card"})
		if err != nil {
			t.Fatal(err)
		}
		relErr := math.Abs(float64(got)-float64(n)) / float64(n)
		if relErr > worst {
			worst = relErr
		}
		if relErr > 0.02 {
			t.Errorf("at %d distinct elements the estimate is %d (%.3f%% error); want under 2%%",
				n, got, relErr*100)
		}
	}
	// Reported so the number in the README is measured rather than claimed.
	t.Logf("worst relative error over 1k..%dk distinct elements: %.4f%%", total/1000, worst*100)
}

// TestHLLSmallCardinalitiesAreNearExact covers the range the sparse encoding and the
// sigma correction between them are supposed to make almost exact. A classic
// HyperLogLog without the correction is badly biased here, so this is the test that
// catches an estimator that skipped it.
func TestHLLSmallCardinalitiesAreNearExact(t *testing.T) {
	for _, n := range []int{0, 1, 2, 5, 10, 100, 1000} {
		s := New(4)
		elems := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			elems = append(elems, []byte("k"+strconv.Itoa(i)))
		}
		if n > 0 {
			if _, err := s.PFAdd("small", elems); err != nil {
				t.Fatal(err)
			}
		} else if _, err := s.PFAdd("small", nil); err != nil {
			t.Fatal(err)
		}
		got, err := s.PFCount([]string{"small"})
		if err != nil {
			t.Fatal(err)
		}
		// Under a few thousand, HyperLogLog with the sigma correction is essentially exact;
		// allow one element of slack plus 1%.
		tolerance := 1 + int64(float64(n)*0.01)
		if diff := got - int64(n); diff > tolerance || diff < -tolerance {
			t.Errorf("%d distinct elements estimated as %d; want within %d", n, got, tolerance)
		}
	}
}

// TestHLLDuplicatesDoNotCount is the property that makes the structure useful at all.
func TestHLLDuplicatesDoNotCount(t *testing.T) {
	s := New(4)
	elems := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	updated, err := s.PFAdd("dup", elems)
	if err != nil || !updated {
		t.Fatalf("the first PFAdd reported updated=%v, err=%v", updated, err)
	}
	// The same elements again change nothing, which is what PFADD's 0 reply means.
	updated, err = s.PFAdd("dup", elems)
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Error("re-adding the same elements reported a change")
	}
	if n, _ := s.PFCount([]string{"dup"}); n != 3 {
		t.Errorf("three distinct elements counted as %d", n)
	}
}

// TestHLLMergeIsTheUnion checks the property PFMERGE and multi-key PFCOUNT rest on: the
// per-register maximum of two sketches is a sketch of the union of their inputs.
func TestHLLMergeIsTheUnion(t *testing.T) {
	s := New(4)
	var a, b [][]byte
	for i := 0; i < 20000; i++ {
		a = append(a, []byte("a"+strconv.Itoa(i)))
	}
	for i := 10000; i < 30000; i++ { // 10000 of these overlap with a
		b = append(b, []byte("a"+strconv.Itoa(i)))
	}
	if _, err := s.PFAdd("A", a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PFAdd("B", b); err != nil {
		t.Fatal(err)
	}

	const trueUnion = 30000
	// Multi-key PFCOUNT counts the union without materializing it.
	got, err := s.PFCount([]string{"A", "B"})
	if err != nil {
		t.Fatal(err)
	}
	if relErr := math.Abs(float64(got)-trueUnion) / trueUnion; relErr > 0.02 {
		t.Errorf("PFCOUNT A B = %d for a true union of %d (%.2f%% error)", got, trueUnion, relErr*100)
	}
	// PFMERGE materializes the same union.
	if _, err := s.PFMerge("U", []string{"A", "B"}); err != nil {
		t.Fatal(err)
	}
	merged, err := s.PFCount([]string{"U"})
	if err != nil {
		t.Fatal(err)
	}
	if merged != got {
		t.Errorf("PFMERGE then PFCOUNT = %d but PFCOUNT A B = %d; they must agree exactly",
			merged, got)
	}
	// And PFCOUNT of a key that does not exist is 0, not an error.
	if n, err := s.PFCount([]string{"nosuch"}); err != nil || n != 0 {
		t.Errorf("PFCOUNT of a missing key = %d, %v", n, err)
	}
}

// TestHLLIsDeterministic is what makes verbatim propagation safe: the same additions in
// the same order produce byte-identical sketches, and -- because the encoding is
// canonical rather than a history of in-place patches -- so do the same additions in a
// different order.
func TestHLLIsDeterministic(t *testing.T) {
	// GetString's ok and err are checked rather than discarded: they used to be dropped,
	// and a build that produced no sketch at all returned nil -- so the comparisons below
	// were string(nil) != string(nil), which cannot fail. The determinism this test exists
	// to pin would have been reported as holding by a server that stored nothing.
	build := func(order []int) []byte {
		t.Helper()
		s := New(4)
		for _, i := range order {
			if _, err := s.PFAdd("k", [][]byte{[]byte("e" + strconv.Itoa(i))}); err != nil {
				t.Fatal(err)
			}
		}
		v, ok, err := s.GetString("k")
		if err != nil || !ok {
			t.Fatalf("GetString after %d PFADDs: ok = %v, err = %v", len(order), ok, err)
		}
		if len(v) <= hllHdrSize || string(v[:4]) != string(hllMagic[:]) {
			t.Fatalf("a %d-element sketch is %d bytes with prefix %q; want a HYLL body", len(order), len(v), v[:min(4, len(v))])
		}
		return v
	}
	forward := make([]int, 500)
	backward := make([]int, 500)
	for i := range forward {
		forward[i] = i
		backward[i] = 499 - i
	}
	if a, b := build(forward), build(forward); string(a) != string(b) {
		t.Error("two identical sequences of PFADDs produced different bytes")
	}
	if a, b := build(forward), build(backward); string(a) != string(b) {
		t.Error("the same additions in a different order produced different bytes")
	}
}

// TestHLLRejectsNonSketchStrings covers the error a client gets for using a plain string
// as a HyperLogLog, which has to be distinguishable from a wrong-type error on another
// data type.
func TestHLLRejectsNonSketchStrings(t *testing.T) {
	s := New(4)
	s.Set("plain", []byte("not a sketch"), 0)
	if _, err := s.PFAdd("plain", [][]byte{[]byte("x")}); err != ErrNotHLL {
		t.Errorf("PFAdd on a plain string returned %v; want ErrNotHLL", err)
	}
	if _, err := s.PFCount([]string{"plain"}); err != ErrNotHLL {
		t.Errorf("PFCount on a plain string returned %v; want ErrNotHLL", err)
	}
	// A truncated sketch is rejected too, rather than read past its end.
	s.Set("truncated", []byte("HYLL\x00\x00\x00"), 0)
	if _, err := s.PFCount([]string{"truncated"}); err != ErrNotHLL {
		t.Errorf("PFCount on a truncated sketch returned %v; want ErrNotHLL", err)
	}
	// And a real data type is a plain WRONGTYPE.
	if _, err := s.LPush("list", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PFAdd("list", [][]byte{[]byte("x")}); err != ErrWrongType {
		t.Errorf("PFAdd on a list returned %v; want ErrWrongType", err)
	}
}

// TestHLLSparseToDenseIsLossless checks the promotion: the registers before and after
// have to be identical, because the count must not move when the representation does.
func TestHLLSparseToDenseIsLossless(t *testing.T) {
	s := New(4)
	elems := make([][]byte, 0, 200)
	for i := 0; i < 200; i++ {
		elems = append(elems, []byte("x"+strconv.Itoa(i)))
	}
	if _, err := s.PFAdd("p", elems); err != nil {
		t.Fatal(err)
	}
	if enc, _, _ := s.PFEncoding("p"); enc != "sparse" {
		t.Fatalf("a 200-element sketch is %q; want sparse", enc)
	}
	before, _, err := s.PFRegisters("p")
	if err != nil {
		t.Fatal(err)
	}
	countBefore, _ := s.PFCount([]string{"p"})

	changed, ok, err := s.PFToDense("p")
	if err != nil || !ok || !changed {
		t.Fatalf("PFToDense reported changed=%v ok=%v err=%v", changed, ok, err)
	}
	if enc, _, _ := s.PFEncoding("p"); enc != "dense" {
		t.Fatalf("after PFToDense the encoding is %q", enc)
	}
	after, _, err := s.PFRegisters("p")
	if err != nil {
		t.Fatal(err)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("register %d changed from %d to %d across the promotion", i, before[i], after[i])
		}
	}
	if countAfter, _ := s.PFCount([]string{"p"}); countAfter != countBefore {
		t.Errorf("the count moved from %d to %d across the promotion", countBefore, countAfter)
	}
	// A second promotion is a no-op rather than an error.
	if changed, ok, _ := s.PFToDense("p"); changed || !ok {
		t.Errorf("promoting an already-dense sketch reported changed=%v ok=%v", changed, ok)
	}
}

// BenchmarkPFAdd measures the two encodings' update costs, which is the trade the
// canonical re-encode makes explicit.
func BenchmarkPFAdd(b *testing.B) {
	for _, name := range []string{"sparse", "dense"} {
		b.Run(name, func(b *testing.B) {
			s := New(4)
			if name == "dense" {
				elems := make([][]byte, 0, 5000)
				for i := 0; i < 5000; i++ {
					elems = append(elems, []byte("seed"+strconv.Itoa(i)))
				}
				if _, err := s.PFAdd("k", elems); err != nil {
					b.Fatal(err)
				}
			}
			el := [][]byte{[]byte("x")}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				el[0] = []byte(fmt.Sprintf("e%d", i))
				if _, err := s.PFAdd("k", el); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
