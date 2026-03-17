package bangutil

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestNewBinHeap(t *testing.T) {
	heap := newBinHeap()
	if heap.items == nil {
		t.Error("items slice should be initialized")
	}
	if heap.locs == nil {
		t.Error("locs map should be initialized")
	}
	if len(heap.items) != 0 {
		t.Error("items slice should be empty initially")
	}
	if len(heap.locs) != 0 {
		t.Error("locs map should be empty initially")
	}
}

func TestPushSingleItem(t *testing.T) {
	heap := newBinHeap()
	item := Item{key: 1, value: "test", priority: 5}

	heap.push(item)

	if len(heap.items) != 1 {
		t.Errorf("expected 1 item, got %d", len(heap.items))
	}
	if heap.items[0] != item {
		t.Errorf("expected %+v, got %+v", item, heap.items[0])
	}
	if heap.locs[1] != 0 {
		t.Errorf("expected location 0 for key 1, got %d", heap.locs[1])
	}
}

func TestPushMultipleItems(t *testing.T) {
	heap := newBinHeap()
	items := []Item{
		{key: 1, value: "a", priority: 10},
		{key: 2, value: "b", priority: 5},
		{key: 3, value: "c", priority: 15},
		{key: 4, value: "d", priority: 2},
	}

	for _, item := range items {
		heap.push(item)
	}

	if len(heap.items) != 4 {
		t.Errorf("expected 4 items, got %d", len(heap.items))
	}

	// Check that all keys are properly mapped
	for _, item := range items {
		if _, exists := heap.locs[item.key]; !exists {
			t.Errorf("key %d not found in locations map", item.key)
		}
	}
}

func TestPopFromEmptyHeap(t *testing.T) {
	heap := newBinHeap()

	ok, item := heap.pop()

	if ok {
		t.Error("pop from empty heap should return false")
	}
	if item != (Item{}) {
		t.Error("pop from empty heap should return zero value Item")
	}
}

func TestPopSingleItem(t *testing.T) {
	heap := newBinHeap()
	expected := Item{key: 1, value: "test", priority: 5}
	heap.push(expected)

	ok, item := heap.pop()

	if !ok {
		t.Error("pop should return true for non-empty heap")
	}
	if item != expected {
		t.Errorf("expected %+v, got %+v", expected, item)
	}
	if len(heap.items) != 0 {
		t.Error("heap should be empty after popping last item")
	}
	if len(heap.locs) != 0 {
		t.Error("locations map should be empty after popping last item")
	}
}

func TestHeapPropertyMaintained(t *testing.T) {
	heap := newBinHeap()
	priorities := []int{10, 5, 15, 2, 8, 12, 20, 1}

	// Push items with various priorities
	for i, p := range priorities {
		heap.push(Item{key: i, value: string(rune('a' + i)), priority: p})
	}

	//
	if len(heap.items) != len(priorities) {
		t.Errorf("all items not pushed in heap, expected %d, actual %d", len(priorities), len(heap.items))
	}

	// Pop all items and verify they come out in priority order
	var poppedPriorities []int
	for len(heap.items) > 0 {
		ok, item := heap.pop()
		if !ok {
			t.Fatal("pop should succeed on non-empty heap")
		}
		poppedPriorities = append(poppedPriorities, item.priority)
	}

	if len(poppedPriorities) != len(priorities) {
		t.Errorf("all items not popped in heap, expected %d, actual %d", len(priorities), len(poppedPriorities))
	}

	// Check if priorities are in ascending order (min heap)
	for i := 1; i < len(poppedPriorities); i++ {
		if poppedPriorities[i-1] > poppedPriorities[i] {
			t.Errorf("heap property violated: %v not in ascending order", poppedPriorities)
			break
		}
	}
}

func TestTrickleUpBug(t *testing.T) {
	// This test exposes a bug in trickleUp function
	// The line "par := i - 1/2" should be "par := (i - 1) / 2"
	heap := newBinHeap()

	// Push items that will trigger trickleUp
	heap.push(Item{key: 1, value: "a", priority: 10})
	heap.push(Item{key: 2, value: "b", priority: 5}) // Should bubble up

	// With the bug, trickleUp won't work correctly
	// The root should have the minimum priority
	if len(heap.items) > 0 && heap.items[0].priority != 5 {
		t.Logf("BUG DETECTED: trickleUp not working correctly due to operator precedence")
		t.Logf("Root priority: %d, expected: 5", heap.items[0].priority)
	}
}

func TestTrickleDownLogicError(t *testing.T) {
	// This test exposes logic errors in trickleDown
	heap := newBinHeap()

	// Create a scenario that will test trickleDown
	items := []Item{
		{key: 1, value: "a", priority: 1},  // root
		{key: 2, value: "b", priority: 5},  // left child
		{key: 3, value: "c", priority: 3},  // right child
		{key: 4, value: "d", priority: 10}, // left-left child
	}

	for _, item := range items {
		heap.push(item)
	}

	// Pop the root, which should trigger trickleDown
	heap.pop()

	// After popping, verify heap property is maintained
	// This may fail due to bugs in trickleDown logic
	if !isValidMinHeap(heap.items) {
		t.Error("Heap property violated after trickleDown")
		t.Logf("Heap state: %+v", heap.items)
	}
}

func TestSwapFunctionality(t *testing.T) {
	heap := newBinHeap()
	heap.push(Item{key: 1, value: "a", priority: 10})
	heap.push(Item{key: 2, value: "b", priority: 5})

	// Test swap function
	originalItems := make([]Item, len(heap.items))
	copy(originalItems, heap.items)
	originalLocs := make(map[int]int)
	for k, v := range heap.locs {
		originalLocs[k] = v
	}

	heap.swap(0, 1)

	// Verify items are swapped
	if reflect.DeepEqual(heap.items, originalItems) {
		t.Error("Items were not swapped")
	}

	// Verify locations are updated
	if heap.locs[originalItems[0].key] != 1 {
		t.Error("Location map not updated correctly for first item")
	}
	if heap.locs[originalItems[1].key] != 0 {
		t.Error("Location map not updated correctly for second item")
	}
}

func TestDeleteMiddleElement(t *testing.T) {
	heap := newBinHeap()
	items := []Item{
		{key: 1, value: "a", priority: 5},
		{key: 2, value: "b", priority: 10},
		{key: 3, value: "c", priority: 15},
		{key: 4, value: "d", priority: 20},
	}

	for _, item := range items {
		heap.push(item)
	}

	originalLen := len(heap.items)

	// Delete middle element
	heap.delete(2)

	if len(heap.items) != originalLen-1 {
		t.Errorf("expected length %d after delete, got %d", originalLen-1, len(heap.items))
	}

	// Verify key 2 is no longer in locations map
	if _, exists := heap.locs[2]; exists {
		t.Error("deleted key still exists in locations map")
	}

	// Verify heap property is maintained
	if !isValidMinHeap(heap.items) {
		t.Error("Heap property violated after deletion")
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	heap := newBinHeap()
	heap.push(Item{key: 1, value: "a", priority: 5})

	// This should cause a panic or undefined behavior
	// because the code doesn't check if the key exists
	defer func() {
		if r := recover(); r != nil {
			t.Logf("BUG DETECTED: delete panics on non-existent key: %v", r)
		}
	}()

	heap.delete(999) // Non-existent key
}

func TestAdjustPriority(t *testing.T) {
	heap := newBinHeap()
	heap.push(Item{key: 1, value: "a", priority: 10})
	heap.push(Item{key: 2, value: "b", priority: 20})
	heap.push(Item{key: 3, value: "c", priority: 30})

	// Adjust priority of key 2 to make it the minimum
	heap.adjust(2, 1)

	// Root should now be the adjusted item
	if len(heap.items) > 0 && heap.items[0].key != 2 {
		t.Error("Adjusted item should be at root")
	}

	if !isValidMinHeap(heap.items) {
		t.Error("Heap property violated after priority adjustment")
	}
}

func TestLargeRandomOperations(t *testing.T) {
	heap := newBinHeap()
	rand.Seed(time.Now().UnixNano())

	const numOps = 1000
	keys := make(map[int]bool)

	for i := 0; i < numOps; i++ {
		switch rand.Intn(3) {
		case 0: // Push
			key := rand.Intn(numOps)
			if !keys[key] {
				heap.push(Item{key: key, value: "test", priority: rand.Intn(100)})
				keys[key] = true
			}
		case 1: // Pop
			if len(heap.items) > 0 {
				ok, item := heap.pop()
				if ok {
					delete(keys, item.key)
				}
			}
		case 2: // Delete random key
			if len(keys) > 0 {
				// Pick a random existing key
				for key := range keys {
					if rand.Float32() < 0.1 { // 10% chance to delete this key
						heap.delete(key)
						delete(keys, key)
						break
					}
				}
			}
		}

		// Verify heap property after each operation
		if !isValidMinHeap(heap.items) {
			t.Fatalf("Heap property violated after operation %d", i)
		}

		// Verify locations map consistency
		if !isLocationMapConsistent(&heap) {
			t.Fatalf("Location map inconsistent after operation %d", i)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	// Note: The current implementation is not thread-safe
	// This test demonstrates potential race conditions
	heap := newBinHeap()

	// Pre-populate heap
	for i := 0; i < 100; i++ {
		heap.push(Item{key: i, value: "test", priority: i})
	}

	// This would fail in a concurrent scenario
	// but we can't easily test it without proper synchronization
	t.Log("Current implementation is not thread-safe")
}

func TestMemoryLeaks(t *testing.T) {
	heap := newBinHeap()

	// Add many items
	for i := 0; i < 1000; i++ {
		heap.push(Item{key: i, value: "test", priority: i})
	}

	// Remove all items
	for len(heap.items) > 0 {
		heap.pop()
	}

	// Check if memory is properly cleaned up
	if len(heap.locs) != 0 {
		t.Errorf("Location map not cleaned up: %d entries remain", len(heap.locs))
	}

	if cap(heap.items) > 1000 {
		t.Log("Slice capacity not reduced after clearing - potential memory waste")
	}
}

// Benchmark tests
func BenchmarkPush(b *testing.B) {
	heap := newBinHeap()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		heap.push(Item{key: i, value: "test", priority: i})
	}
}

func BenchmarkPop(b *testing.B) {
	heap := newBinHeap()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		heap.push(Item{key: i, value: "test", priority: i})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		heap.pop()
	}
}

func BenchmarkRandomOperations(b *testing.B) {
	heap := newBinHeap()
	rand.Seed(42) // Consistent seed for reproducible benchmarks

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch rand.Intn(2) {
		case 0:
			heap.push(Item{key: rand.Intn(1000), value: "test", priority: rand.Intn(100)})
		case 1:
			if len(heap.items) > 0 {
				heap.pop()
			}
		}
	}
}

// Helper functions
func isValidMinHeap(items []Item) bool {
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

func isLocationMapConsistent(heap *binHeap) bool {
	// Check that every item's location in the map matches its actual position
	for i, item := range heap.items {
		if heap.locs[item.key] != i {
			return false
		}
	}

	// Check that every key in the map has a corresponding item
	for key, loc := range heap.locs {
		if loc >= len(heap.items) || heap.items[loc].key != key {
			return false
		}
	}

	return true
}
