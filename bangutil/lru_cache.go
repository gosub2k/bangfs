package bangutil

import (
	"sync"
)

type Item struct {
	key      int
	value    string
	priority int
}

func (i Item) Less(j Item) bool {
	return i.priority < j.priority
}

type binHeap struct {
	items []Item
	locs  map[int]int
	mut   sync.RWMutex
}

func newBinHeap() binHeap {
	return binHeap{items: []Item{}, locs: make(map[int]int), mut: sync.RWMutex()}
}

func (bh *binHeap) Delete(i int) {
	bh.mut.Lock()
	defer bh.mut.Unlock()
	bh.delete(i)
}

func (bh *binHeap) delete(i int) {
	loc := bh.locs[i]
	new_end := len(bh.items) - 1

	bh.swap(loc, new_end)
	delete(bh.locs, new_end)
	bh.items = bh.items[0:new_end]
	if loc < new_end {
		bh.trickleDown(loc)
	}
}

func (bh *binHeap) adjust(i, p int) {
	loc := bh.locs[i]
	end := len(bh.items) - 1
	bh.items[loc].priority = p
	bh.swap(loc, end)
	bh.trickleDown(loc)
	bh.trickleUp(end)
}

func (bh *binHeap) swap(i, j int) {
	bh.locs[i], bh.locs[j] = bh.locs[j], bh.locs[i]
	bh.items[i], bh.items[j] = bh.items[j], bh.items[i]
}

func (bh *binHeap) Push(item Item) {
	bh.mut.Lock()
	defer bh.mut.Unlock()
	bh.items = append(bh.items, item)
	n := len(bh.items)
	bh.locs[item.key] = n - 1
	bh.trickleUp(n - 1)
}

func (bh *binHeap) Pop() (bool, Item) {
	bh.mut.Lock()
	defer bh.mut.Unlock()
	if len(bh.items) == 0 {
		return false, Item{}
	}
	top := bh.items[0]
	end := len(bh.items) - 1
	bh.swap(0, end)
	bh.delete(end)
	bh.trickleDown(0)
	return true, top
}

func (bh *binHeap) trickleUp(i int) {
	b := bh.items
	for i > 0 {
		par := (i - 1) / 2
		if b[i].Less(b[par]) {
			bh.swap(i, par)
		}
		i = par
	}
}

func (bh *binHeap) trickleDown(i int) {
	b := bh.items
	n := len(b)
	if n == 0 {
		return
	}
	lc := 2*i + 1
	rc := 2*i + 2

	if rc < n {
		if b[rc].Less(b[i]) && b[rc].Less(b[lc]) {
			bh.swap(rc, i)
			bh.trickleDown(rc)
		} else if b[rc].Less(b[i]) && b[lc].Less(b[rc]) {
			bh.swap(lc, i)
			bh.trickleDown(lc)
		}
	} else if lc < n && b[lc].Less(b[i]) {
		bh.swap(lc, i)
		bh.trickleDown(lc)
	}
}
