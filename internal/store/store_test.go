package store

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSetGetDelExists(t *testing.T) {
	s := New(8)

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss on empty store")
	}

	s.Set("k", []byte("v"), 0)
	if v, ok := s.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("Get(k) = %q, %v; want \"v\", true", v, ok)
	}
	if !s.Exists("k") {
		t.Fatal("Exists(k) = false; want true")
	}
	if !s.Del("k") {
		t.Fatal("Del(k) = false; want true")
	}
	if s.Del("k") {
		t.Fatal("second Del(k) = true; want false")
	}
	if s.Exists("k") {
		t.Fatal("Exists after Del = true; want false")
	}
}

func TestGetReturnsCopy(t *testing.T) {
	s := New(4)
	s.Set("k", []byte("abc"), 0)
	v, _ := s.Get("k")
	v[0] = 'X' // mutating the returned slice must not corrupt the store
	if v2, _ := s.Get("k"); string(v2) != "abc" {
		t.Fatalf("stored value mutated through returned slice: %q", v2)
	}
}

func TestExpiry(t *testing.T) {
	s := New(4)
	cur := time.Unix(1000, 0)
	s.clock = func() time.Time { return cur }

	s.Set("k", []byte("v"), 10*time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key should be live before TTL elapses")
	}

	cur = cur.Add(11 * time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should be expired after TTL elapses")
	}
	if s.Exists("k") {
		t.Fatal("Exists should be false for expired key")
	}
}

func TestTTLAndExpire(t *testing.T) {
	s := New(4)
	cur := time.Unix(0, 0)
	s.clock = func() time.Time { return cur }

	s.Set("k", []byte("v"), 0)
	if _, hasTTL, ok := s.TTL("k"); !ok || hasTTL {
		t.Fatalf("TTL of persistent key: hasTTL=%v ok=%v; want false,true", hasTTL, ok)
	}
	if _, _, ok := s.TTL("nope"); ok {
		t.Fatal("TTL of missing key should report ok=false")
	}

	if !s.Expire("k", 30*time.Second) {
		t.Fatal("Expire on existing key should succeed")
	}
	d, hasTTL, ok := s.TTL("k")
	if !ok || !hasTTL || d != 30*time.Second {
		t.Fatalf("TTL after Expire = %v,%v,%v; want 30s,true,true", d, hasTTL, ok)
	}
	if s.Expire("missing", time.Second) {
		t.Fatal("Expire on missing key should fail")
	}
}

func TestIncr(t *testing.T) {
	s := New(4)
	if n, err := s.Incr("c", 1); err != nil || n != 1 {
		t.Fatalf("Incr fresh = %d,%v; want 1,nil", n, err)
	}
	if n, _ := s.Incr("c", 5); n != 6 {
		t.Fatalf("Incr = %d; want 6", n)
	}
	if n, _ := s.Incr("c", -2); n != 4 {
		t.Fatalf("Incr negative = %d; want 4", n)
	}

	s.Set("text", []byte("hello"), 0)
	if _, err := s.Incr("text", 1); err != ErrNotInteger {
		t.Fatalf("Incr on non-integer = %v; want ErrNotInteger", err)
	}
}

// TestConcurrentIncr is the core race-safety test: many goroutines hammering a
// single hot key must produce an exact total. Run with `go test -race`.
func TestConcurrentIncr(t *testing.T) {
	s := New(16)
	const goroutines, perG = 64, 2000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := s.Incr("counter", 1); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	v, _ := s.Get("counter")
	got, _ := strconv.ParseInt(string(v), 10, 64)
	if want := int64(goroutines * perG); got != want {
		t.Fatalf("counter = %d; want %d", got, want)
	}
}

// TestConcurrentMixed exercises Set/Get/Del across many keys and goroutines to
// surface data races under -race.
func TestConcurrentMixed(t *testing.T) {
	s := New(32)
	const goroutines, perG = 32, 5000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := "key:" + strconv.Itoa((g+i)%256)
				switch i % 3 {
				case 0:
					s.Set(key, []byte(strconv.Itoa(i)), 0)
				case 1:
					s.Get(key)
				case 2:
					s.Del(key)
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkSet(b *testing.B) {
	s := New(256)
	val := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key:"+strconv.Itoa(i&1023), val, 0)
	}
}

func BenchmarkGetParallel(b *testing.B) {
	s := New(256)
	for i := 0; i < 1024; i++ {
		s.Set("key:"+strconv.Itoa(i), []byte("value"), 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get("key:" + strconv.Itoa(i&1023))
			i++
		}
	})
}

func BenchmarkIncrParallel(b *testing.B) {
	s := New(256)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Spread across keys to measure shard-level concurrency.
			s.Incr("counter:"+strconv.Itoa(i&63), 1)
			i++
		}
	})
}
