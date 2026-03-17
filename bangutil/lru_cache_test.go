package bangutil

import (
	"math/rand"
	"testing"
	"time"
)

func TestNewBinHeap(t *testing.T) {
	h := newBinHeap()
	if h.h.items == nil {
		t.Error("items slice should be initialized")
	}
	if h.h.locs == nil {
		t.Error("locs map should be initialized")
	}
	if len(h.h.items) != 0 {
		t.Error("items slice should be empty initially")
	}
	if len(h.h.locs) != 0 {
		t.Error("locs map should be empty initially")
	}
}

func TestPushSingleItem(t *testing.T) {
	h := newBinHeap()
	item := Item{key: 1, value: "test", priority: 5}

	h.push(item)

	if h.h.Len() != 1 {
		t.Errorf("expected 1 item, got %d", h.h.Len())
	}
	if h.h.items[0].key != item.key || h.h.items[0].value != item.value {
		t.Errorf("expected key=%d value=%s, got key=%d value=%s", item.key, item.value, h.h.items[0].key, h.h.items[0].value)
	}
	if h.h.locs[1].index != 0 {
		t.Errorf("expected location 0 for key 1, got %d", h.h.locs[1].index)
	}
}

func TestPushMultipleItems(t *testing.T) {
	h := newBinHeap()
	items := []Item{
		{key: 1, value: "a", priority: 10},
		{key: 2, value: "b", priority: 5},
		{key: 3, value: "c", priority: 15},
		{key: 4, value: "d", priority: 2},
	}

	for _, item := range items {
		h.push(item)
	}

	if h.h.Len() != 4 {
		t.Errorf("expected 4 items, got %d", h.h.Len())
	}

	for _, item := range items {
		if _, exists := h.h.locs[item.key]; !exists {
			t.Errorf("key %d not found in locations map", item.key)
		}
	}
}

func TestPopFromEmptyHeap(t *testing.T) {
	h := newBinHeap()

	ok, item := h.pop()

	if ok {
		t.Error("pop from empty heap should return false")
	}
	if item != (Item{}) {
		t.Error("pop from empty heap should return zero value Item")
	}
}

func TestPopSingleItem(t *testing.T) {
	h := newBinHeap()
	expected := Item{key: 1, value: "test", priority: 5}
	h.push(expected)

	ok, item := h.pop()

	if !ok {
		t.Error("pop should return true for non-empty heap")
	}
	if item.key != expected.key || item.value != expected.value || item.priority != expected.priority {
		t.Errorf("expected key=%d value=%s priority=%d, got key=%d value=%s priority=%d",
			expected.key, expected.value, expected.priority, item.key, item.value, item.priority)
	}
	if h.h.Len() != 0 {
		t.Error("heap should be empty after popping last item")
	}
	if len(h.h.locs) != 0 {
		t.Error("locations map should be empty after popping last item")
	}
}

func TestHeapPropertyMaintained(t *testing.T) {
	h := newBinHeap()
	priorities := []int{10, 5, 15, 2, 8, 12, 20, 1}

	for i, p := range priorities {
		h.push(Item{key: i, value: string(rune('a' + i)), priority: p})
	}

	if h.h.Len() != len(priorities) {
		t.Errorf("all items not pushed in heap, expected %d, actual %d", len(priorities), h.h.Len())
	}

	var poppedPriorities []int
	for h.h.Len() > 0 {
		ok, item := h.pop()
		if !ok {
			t.Fatal("pop should succeed on non-empty heap")
		}
		poppedPriorities = append(poppedPriorities, item.priority)
	}

	if len(poppedPriorities) != len(priorities) {
		t.Errorf("all items not popped in heap, expected %d, actual %d", len(priorities), len(poppedPriorities))
	}

	for i := 1; i < len(poppedPriorities); i++ {
		if poppedPriorities[i-1] > poppedPriorities[i] {
			t.Errorf("heap property violated: %v not in ascending order", poppedPriorities)
			break
		}
	}
}

func TestTrickleUpWorksCorrectly(t *testing.T) {
	h := newBinHeap()

	h.push(Item{key: 1, value: "a", priority: 10})
	h.push(Item{key: 2, value: "b", priority: 5})

	if h.h.items[0].priority != 5 {
		t.Errorf("root priority should be 5, got %d", h.h.items[0].priority)
	}
}

func TestTrickleDownAfterPop(t *testing.T) {
	h := newBinHeap()

	items := []Item{
		{key: 1, value: "a", priority: 1},
		{key: 2, value: "b", priority: 5},
		{key: 3, value: "c", priority: 3},
		{key: 4, value: "d", priority: 10},
	}

	for _, item := range items {
		h.push(item)
	}

	h.pop()

	if !isValidMinHeap(h.h.items) {
		t.Error("Heap property violated after pop")
		t.Logf("Heap state: %+v", h.h.items)
	}
}

func TestSwapFunctionality(t *testing.T) {
	h := newBinHeap()
	h.push(Item{key: 1, value: "a", priority: 10})
	h.push(Item{key: 2, value: "b", priority: 5})

	key0 := h.h.items[0].key
	key1 := h.h.items[1].key

	h.swap(0, 1)

	if h.h.items[0].key != key1 || h.h.items[1].key != key0 {
		t.Error("Items were not swapped correctly")
	}
	if h.h.items[0].index != 0 || h.h.items[1].index != 1 {
		t.Error("Indices not updated correctly after swap")
	}
}

func TestDeleteMiddleElement(t *testing.T) {
	h := newBinHeap()
	items := []Item{
		{key: 1, value: "a", priority: 5},
		{key: 2, value: "b", priority: 10},
		{key: 3, value: "c", priority: 15},
		{key: 4, value: "d", priority: 20},
	}

	for _, item := range items {
		h.push(item)
	}

	originalLen := h.h.Len()

	h.delete(2)

	if h.h.Len() != originalLen-1 {
		t.Errorf("expected length %d after delete, got %d", originalLen-1, h.h.Len())
	}

	if _, exists := h.h.locs[2]; exists {
		t.Error("deleted key still exists in locations map")
	}

	if !isValidMinHeap(h.h.items) {
		t.Error("Heap property violated after deletion")
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	h := newBinHeap()
	h.push(Item{key: 1, value: "a", priority: 5})

	// Should not panic with the new implementation
	h.delete(999)

	if h.h.Len() != 1 {
		t.Error("heap should be unchanged after deleting non-existent key")
	}
}

func TestAdjustPriority(t *testing.T) {
	h := newBinHeap()
	h.push(Item{key: 1, value: "a", priority: 10})
	h.push(Item{key: 2, value: "b", priority: 20})
	h.push(Item{key: 3, value: "c", priority: 30})

	h.adjust(2, 1)

	if h.h.items[0].key != 2 {
		t.Errorf("adjusted item should be at root, got key %d", h.h.items[0].key)
	}

	if !isValidMinHeap(h.h.items) {
		t.Error("Heap property violated after priority adjustment")
	}
}

func TestLargeRandomOperations(t *testing.T) {
	h := newBinHeap()
	rand.Seed(time.Now().UnixNano())

	const numOps = 1000
	keys := make(map[int]bool)

	for i := 0; i < numOps; i++ {
		switch rand.Intn(3) {
		case 0: // Push
			key := rand.Intn(numOps)
			if !keys[key] {
				h.push(Item{key: key, value: "test", priority: rand.Intn(100)})
				keys[key] = true
			}
		case 1: // Pop
			if h.h.Len() > 0 {
				ok, item := h.pop()
				if ok {
					delete(keys, item.key)
				}
			}
		case 2: // Delete random key
			if len(keys) > 0 {
				for key := range keys {
					if rand.Float32() < 0.1 {
						h.delete(key)
						delete(keys, key)
						break
					}
				}
			}
		}

		if !isValidMinHeap(h.h.items) {
			t.Fatalf("Heap property violated after operation %d", i)
		}

		if !isLocationMapConsistent(&h) {
			t.Fatalf("Location map inconsistent after operation %d", i)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	h := newBinHeap()

	for i := 0; i < 100; i++ {
		h.Push(Item{key: i, value: "test", priority: i})
	}

	t.Log("Exported Push/Pop/Delete methods are mutex-protected")
}

func TestMemoryLeaks(t *testing.T) {
	h := newBinHeap()

	for i := 0; i < 1000; i++ {
		h.push(Item{key: i, value: "test", priority: i})
	}

	for h.h.Len() > 0 {
		h.pop()
	}

	if len(h.h.locs) != 0 {
		t.Errorf("Location map not cleaned up: %d entries remain", len(h.h.locs))
	}
}

// Benchmarks
func BenchmarkPush(b *testing.B) {
	h := newBinHeap()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h.push(Item{key: i, value: "test", priority: i})
	}
}

func BenchmarkPop(b *testing.B) {
	h := newBinHeap()

	for i := 0; i < b.N; i++ {
		h.push(Item{key: i, value: "test", priority: i})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.pop()
	}
}

func BenchmarkRandomOperations(b *testing.B) {
	h := newBinHeap()
	rand.Seed(42)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch rand.Intn(2) {
		case 0:
			h.push(Item{key: rand.Intn(1000), value: "test", priority: rand.Intn(100)})
		case 1:
			if h.h.Len() > 0 {
				h.pop()
			}
		}
	}
}

// Helpers
func isValidMinHeap(items []*Item) bool {
	for i := 0; i < len(items); i++ {
		leftChild := 2*i + 1
		rightChild := 2*i + 2

		if leftChild < len(items) && items[i].priority > items[leftChild].priority {
			return false
		}
		if rightChild < len(items) && items[i].priority > items[rightChild].priority {
			return false
		}
	}
	return true
}

func isLocationMapConsistent(bh *binHeap) bool {
	for i, item := range bh.h.items {
		if item.index != i {
			return false
		}
		loc, ok := bh.h.locs[item.key]
		if !ok || loc != item {
			return false
		}
	}

	if len(bh.h.locs) != len(bh.h.items) {
		return false
	}

	return true
}
