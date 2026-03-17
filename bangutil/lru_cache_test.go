package bangutil_test

import (
	"math/rand"
	"testing"

	"bangfs/bangutil"
)

func TestNewBinHeap(t *testing.T) {
	h := bangutil.NewBinHeap()
	if h.Len() != 0 {
		t.Error("heap should be empty initially")
	}
}

func TestPushAndPopSingle(t *testing.T) {
	h := bangutil.NewBinHeap()
	h.Push(bangutil.Item{Key: 1, Value: "test", Priority: 5})

	if h.Len() != 1 {
		t.Errorf("expected 1 item, got %d", h.Len())
	}

	ok, item := h.Pop()
	if !ok {
		t.Error("pop should return true for non-empty heap")
	}
	if item.Key != 1 || item.Value != "test" || item.Priority != 5 {
		t.Errorf("unexpected item: %+v", item)
	}
	if h.Len() != 0 {
		t.Error("heap should be empty after popping last item")
	}
}

func TestPopFromEmptyHeap(t *testing.T) {
	h := bangutil.NewBinHeap()

	ok, _ := h.Pop()
	if ok {
		t.Error("pop from empty heap should return false")
	}
}

func TestHeapPropertyMaintained(t *testing.T) {
	h := bangutil.NewBinHeap()
	priorities := []int{10, 5, 15, 2, 8, 12, 20, 1}

	for i, p := range priorities {
		h.Push(bangutil.Item{Key: i, Value: string(rune('a' + i)), Priority: p})
	}

	if h.Len() != len(priorities) {
		t.Errorf("expected %d items, got %d", len(priorities), h.Len())
	}

	var prev int
	for i := 0; h.Len() > 0; i++ {
		ok, item := h.Pop()
		if !ok {
			t.Fatal("pop should succeed on non-empty heap")
		}
		if i > 0 && item.Priority < prev {
			t.Errorf("heap property violated: got %d after %d", item.Priority, prev)
		}
		prev = item.Priority
	}
}

func TestPushDuplicateKeyUpdates(t *testing.T) {
	h := bangutil.NewBinHeap()
	h.Push(bangutil.Item{Key: 1, Value: "a", Priority: 10})
	h.Push(bangutil.Item{Key: 1, Value: "b", Priority: 3})

	if h.Len() != 1 {
		t.Errorf("duplicate push should update, not add; got len %d", h.Len())
	}

	ok, item := h.Pop()
	if !ok || item.Value != "b" || item.Priority != 3 {
		t.Errorf("expected updated item, got %+v", item)
	}
}

func TestDeleteMiddleElement(t *testing.T) {
	h := bangutil.NewBinHeap()
	for i, p := range []int{5, 10, 15, 20} {
		h.Push(bangutil.Item{Key: i + 1, Value: "x", Priority: p})
	}

	h.Delete(2) // delete key=2, priority=10

	if h.Len() != 3 {
		t.Errorf("expected 3 items after delete, got %d", h.Len())
	}

	// Remaining items should pop in order: 5, 15, 20
	for _, expected := range []int{5, 15, 20} {
		ok, item := h.Pop()
		if !ok || item.Priority != expected {
			t.Errorf("expected priority %d, got %+v (ok=%v)", expected, item, ok)
		}
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	h := bangutil.NewBinHeap()
	h.Push(bangutil.Item{Key: 1, Value: "a", Priority: 5})

	h.Delete(999) // should not panic

	if h.Len() != 1 {
		t.Error("heap should be unchanged after deleting non-existent key")
	}
}

func TestAdjustPriority(t *testing.T) {
	h := bangutil.NewBinHeap()
	h.Push(bangutil.Item{Key: 1, Value: "a", Priority: 10})
	h.Push(bangutil.Item{Key: 2, Value: "b", Priority: 20})
	h.Push(bangutil.Item{Key: 3, Value: "c", Priority: 30})

	h.Adjust(3, 1) // make key=3 the minimum

	ok, item := h.Pop()
	if !ok || item.Key != 3 || item.Priority != 1 {
		t.Errorf("adjusted item should be popped first, got %+v", item)
	}
}

func TestLargeRandomOperations(t *testing.T) {
	h := bangutil.NewBinHeap()

	const numOps = 1000
	keys := make(map[int]bool)

	for i := 0; i < numOps; i++ {
		switch rand.Intn(3) {
		case 0:
			key := rand.Intn(numOps)
			if !keys[key] {
				h.Push(bangutil.Item{Key: key, Value: "test", Priority: rand.Intn(100)})
				keys[key] = true
			}
		case 1:
			if h.Len() > 0 {
				ok, item := h.Pop()
				if ok {
					delete(keys, item.Key)
				}
			}
		case 2:
			for key := range keys {
				if rand.Float32() < 0.1 {
					h.Delete(key)
					delete(keys, key)
					break
				}
			}
		}
	}

	// Drain and verify order
	prev := -1
	for h.Len() > 0 {
		ok, item := h.Pop()
		if !ok {
			t.Fatal("pop should succeed on non-empty heap")
		}
		if item.Priority < prev {
			t.Fatalf("heap property violated: got %d after %d", item.Priority, prev)
		}
		prev = item.Priority
	}
}

func TestMemoryCleanup(t *testing.T) {
	h := bangutil.NewBinHeap()

	for i := 0; i < 1000; i++ {
		h.Push(bangutil.Item{Key: i, Value: "test", Priority: i})
	}

	for h.Len() > 0 {
		h.Pop()
	}

	if h.Len() != 0 {
		t.Errorf("heap should be empty, got len %d", h.Len())
	}
}

// Benchmarks
func BenchmarkPush(b *testing.B) {
	h := bangutil.NewBinHeap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Push(bangutil.Item{Key: i, Value: "test", Priority: i})
	}
}

func BenchmarkPop(b *testing.B) {
	h := bangutil.NewBinHeap()
	for i := 0; i < b.N; i++ {
		h.Push(bangutil.Item{Key: i, Value: "test", Priority: i})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Pop()
	}
}

func BenchmarkRandomOperations(b *testing.B) {
	h := bangutil.NewBinHeap()
	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch r.Intn(2) {
		case 0:
			h.Push(bangutil.Item{Key: r.Intn(1000), Value: "test", Priority: r.Intn(100)})
		case 1:
			if h.Len() > 0 {
				h.Pop()
			}
		}
	}
}
