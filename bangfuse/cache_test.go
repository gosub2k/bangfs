package bangfuse

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func noopWrite(key uint64, data []byte) error { return nil }

func TestCacheAddAndGet(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)
	c.Add(1, []byte("hello"), false)

	data, ok := c.Get(1)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", data)
	}
}

func TestCacheGetMiss(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)

	_, ok := c.Get(42)
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestCachePeek(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)
	c.Add(1, []byte("data"), true)

	data, dirty, exists := c.Peek(1)
	if !exists {
		t.Fatal("expected entry to exist")
	}
	if !dirty {
		t.Error("expected dirty=true")
	}
	if string(data) != "data" {
		t.Errorf("expected 'data', got %q", data)
	}
}

func TestCachePeekMiss(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)

	_, _, exists := c.Peek(99)
	if exists {
		t.Fatal("expected miss")
	}
}

func TestCacheAddUpdateExisting(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)
	c.Add(1, []byte("v1"), false)
	c.Add(1, []byte("v2"), true)

	if c.Count() != 1 {
		t.Errorf("expected count 1, got %d", c.Count())
	}

	data, dirty, _ := c.Peek(1)
	if string(data) != "v2" {
		t.Errorf("expected 'v2', got %q", data)
	}
	if !dirty {
		t.Error("expected dirty after update")
	}
}

func TestCacheDelete(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)
	c.Add(1, []byte("data"), false)
	c.Delete(1)

	if c.Count() != 0 {
		t.Errorf("expected count 0, got %d", c.Count())
	}
	_, ok := c.Get(1)
	if ok {
		t.Error("expected miss after delete")
	}
}

func TestCacheDeleteNonExistent(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)
	c.Delete(999) // should not panic
}

func TestCacheDirtyCleanMaps(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)
	c.Add(1, []byte("clean"), false)
	c.Add(2, []byte("dirty"), true)

	if c.DirtyCount() != 1 {
		t.Errorf("expected 1 dirty, got %d", c.DirtyCount())
	}
	if c.Count() != 2 {
		t.Errorf("expected 2 total, got %d", c.Count())
	}
}

func TestCacheEvictionPrefersClean(t *testing.T) {
	var written []uint64
	writeFn := func(key uint64, data []byte) error {
		written = append(written, key)
		return nil
	}
	c := NewCache(3, time.Minute, writeFn)

	c.Add(1, []byte("clean1"), false)
	c.Add(2, []byte("dirty1"), true)
	c.Add(3, []byte("clean2"), false)

	// Adding a 4th should evict the oldest clean entry (key=1)
	c.Add(4, []byte("new"), false)

	if c.Count() != 3 {
		t.Errorf("expected count 3, got %d", c.Count())
	}

	// key=1 (clean) should be evicted, key=2 (dirty) should survive
	_, ok := c.Get(1)
	if ok {
		t.Error("key=1 should have been evicted")
	}
	_, ok = c.Get(2)
	if !ok {
		t.Error("key=2 (dirty) should not have been evicted")
	}

	// No writes should have been triggered (evicted a clean entry)
	if len(written) != 0 {
		t.Errorf("expected no writes, got %d", len(written))
	}
}

func TestCacheEvictionFlushesWhenAllDirty(t *testing.T) {
	var written []uint64
	writeFn := func(key uint64, data []byte) error {
		written = append(written, key)
		return nil
	}
	c := NewCache(2, time.Minute, writeFn)

	c.Add(1, []byte("d1"), true)
	c.Add(2, []byte("d2"), true)

	// Adding a 3rd forces eviction of oldest dirty (key=1), which triggers writeFn
	c.Add(3, []byte("d3"), true)

	if c.Count() != 2 {
		t.Errorf("expected count 2, got %d", c.Count())
	}
	if len(written) != 1 || written[0] != 1 {
		t.Errorf("expected write of key=1, got %v", written)
	}
}

func TestCacheEvictionWriteError(t *testing.T) {
	writeFn := func(key uint64, data []byte) error {
		return fmt.Errorf("backend down")
	}
	c := NewCache(1, time.Minute, writeFn)

	c.Add(1, []byte("d1"), true)
	c.Add(2, []byte("d2"), true) // evicts key=1, writeFn fails

	err := c.DrainErrors()
	if err == nil {
		t.Fatal("expected error from eviction write failure")
	}
	if err.Error() != "backend down" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCacheFlushKeys(t *testing.T) {
	var written []uint64
	writeFn := func(key uint64, data []byte) error {
		written = append(written, key)
		return nil
	}
	c := NewCache(10, time.Minute, writeFn)

	c.Add(1, []byte("d1"), true)
	c.Add(2, []byte("d2"), true)
	c.Add(3, []byte("clean"), false)

	err := c.Flush([]uint64{1, 3}) // key=3 is clean, should be skipped
	if err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if len(written) != 1 || written[0] != 1 {
		t.Errorf("expected write of key=1 only, got %v", written)
	}

	// key=1 should now be clean
	_, dirty, _ := c.Peek(1)
	if dirty {
		t.Error("key=1 should be clean after flush")
	}
	if c.DirtyCount() != 1 {
		t.Errorf("expected 1 dirty (key=2), got %d", c.DirtyCount())
	}
}

func TestCacheFlushError(t *testing.T) {
	writeFn := func(key uint64, data []byte) error {
		return fmt.Errorf("write failed")
	}
	c := NewCache(10, time.Minute, writeFn)
	c.Add(1, []byte("d1"), true)

	err := c.Flush([]uint64{1})
	if err == nil || err.Error() != "write failed" {
		t.Errorf("expected 'write failed', got %v", err)
	}
}

func TestCacheFlushAll(t *testing.T) {
	var mu sync.Mutex
	var written []uint64
	writeFn := func(key uint64, data []byte) error {
		mu.Lock()
		written = append(written, key)
		mu.Unlock()
		return nil
	}
	c := NewCache(10, time.Minute, writeFn)

	c.Add(1, []byte("d1"), true)
	c.Add(2, []byte("d2"), true)
	c.Add(3, []byte("clean"), false)

	err := c.FlushAll()
	if err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if len(written) != 2 {
		t.Errorf("expected 2 writes, got %d", len(written))
	}
	if c.DirtyCount() != 0 {
		t.Errorf("expected 0 dirty after FlushAll, got %d", c.DirtyCount())
	}
}

func TestCacheLRUOrder(t *testing.T) {
	c := NewCache(3, time.Minute, noopWrite)

	c.Add(1, []byte("a"), false)
	c.Add(2, []byte("b"), false)
	c.Add(3, []byte("c"), false)

	// Touch key=1 to move it to tail
	c.Get(1)

	// Add key=4 — should evict key=2 (oldest untouched)
	c.Add(4, []byte("d"), false)

	_, ok := c.Get(2)
	if ok {
		t.Error("key=2 should have been evicted (LRU)")
	}
	_, ok = c.Get(1)
	if !ok {
		t.Error("key=1 should still be present (was touched)")
	}
}

func TestCacheEvictExpired(t *testing.T) {
	var written []uint64
	writeFn := func(key uint64, data []byte) error {
		written = append(written, key)
		return nil
	}
	c := NewCache(10, 50*time.Millisecond, writeFn)

	c.Add(1, []byte("old"), true)
	c.Add(2, []byte("old-clean"), false)
	time.Sleep(80 * time.Millisecond)

	// Add a fresh entry
	c.Add(3, []byte("fresh"), false)

	c.evictExpired()

	if c.Count() != 1 {
		t.Errorf("expected 1 entry (fresh only), got %d", c.Count())
	}
	_, ok := c.Get(3)
	if !ok {
		t.Error("fresh entry should survive")
	}
	// dirty key=1 should have been written
	if len(written) != 1 || written[0] != 1 {
		t.Errorf("expected write of key=1, got %v", written)
	}
}

func TestCachePeriodicFlush(t *testing.T) {
	var mu sync.Mutex
	var written []uint64
	writeFn := func(key uint64, data []byte) error {
		mu.Lock()
		written = append(written, key)
		mu.Unlock()
		return nil
	}
	c := NewCache(10, time.Minute, writeFn)
	c.Add(1, []byte("d1"), true)

	c.Start(50 * time.Millisecond)
	time.Sleep(120 * time.Millisecond)
	c.Stop()

	mu.Lock()
	n := len(written)
	mu.Unlock()
	if n == 0 {
		t.Error("periodic flusher should have written dirty entry")
	}
	if c.DirtyCount() != 0 {
		t.Errorf("expected 0 dirty after periodic flush, got %d", c.DirtyCount())
	}
}

func TestCacheDrainErrorsEmpty(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)
	if err := c.DrainErrors(); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestCacheCounters(t *testing.T) {
	c := NewCache(10, time.Minute, noopWrite)

	if c.Count() != 0 || c.DirtyCount() != 0 {
		t.Fatal("expected both counts to be 0")
	}

	c.Add(1, []byte("a"), true)
	c.Add(2, []byte("b"), false)

	if c.Count() != 2 {
		t.Errorf("expected count 2, got %d", c.Count())
	}
	if c.DirtyCount() != 1 {
		t.Errorf("expected dirty 1, got %d", c.DirtyCount())
	}

	c.Delete(1)
	if c.Count() != 1 {
		t.Errorf("expected count 1 after delete, got %d", c.Count())
	}
	if c.DirtyCount() != 0 {
		t.Errorf("expected dirty 0 after delete, got %d", c.DirtyCount())
	}
}
