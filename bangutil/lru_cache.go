package bangutil

import (
	"container/heap"
	"sync"
)

type Item struct {
	key      int
	value    string
	priority int
	index    int // position in the heap, maintained by heap.Interface
}

// itemHeap implements heap.Interface for Item elements.
type itemHeap struct {
	items []*Item
	locs  map[int]*Item // key -> item for O(1) lookup
}

func (h *itemHeap) Len() int { return len(h.items) }

func (h *itemHeap) Less(i, j int) bool {
	return h.items[i].priority < h.items[j].priority
}

func (h *itemHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *itemHeap) Push(x any) {
	item := x.(*Item)
	item.index = len(h.items)
	h.items = append(h.items, item)
	h.locs[item.key] = item
}

func (h *itemHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.items = old[:n-1]
	item.index = -1
	delete(h.locs, item.key)
	return item
}

// binHeap is a min-heap with O(1) key lookup and O(log n) push/pop/delete/adjust.
type binHeap struct {
	h   itemHeap
	mut sync.RWMutex
}

func newBinHeap() binHeap {
	return binHeap{h: itemHeap{items: []*Item{}, locs: make(map[int]*Item)}}
}

func (bh *binHeap) push(item Item) {
	if existing, ok := bh.h.locs[item.key]; ok {
		existing.value = item.value
		existing.priority = item.priority
		heap.Fix(&bh.h, existing.index)
		return
	}
	heap.Push(&bh.h, &item)
}

func (bh *binHeap) pop() (bool, Item) {
	if bh.h.Len() == 0 {
		return false, Item{}
	}
	item := heap.Pop(&bh.h).(*Item)
	return true, *item
}

func (bh *binHeap) delete(key int) {
	item, ok := bh.h.locs[key]
	if !ok {
		return
	}
	heap.Remove(&bh.h, item.index)
}

func (bh *binHeap) adjust(key, priority int) {
	item, ok := bh.h.locs[key]
	if !ok {
		return
	}
	item.priority = priority
	heap.Fix(&bh.h, item.index)
}

func (bh *binHeap) swap(i, j int) {
	bh.h.Swap(i, j)
}

func (bh *binHeap) Push(item Item) {
	bh.mut.Lock()
	defer bh.mut.Unlock()
	bh.push(item)
}

func (bh *binHeap) Pop() (bool, Item) {
	bh.mut.Lock()
	defer bh.mut.Unlock()
	return bh.pop()
}

func (bh *binHeap) Delete(key int) {
	bh.mut.Lock()
	defer bh.mut.Unlock()
	bh.delete(key)
}
