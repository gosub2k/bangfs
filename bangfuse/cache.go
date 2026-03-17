package bangfuse

import (
	"sync"
	"time"
)

// CacheEntry is a node in the doubly linked list.
type CacheEntry struct {
	Key       uint64
	Data      []byte
	dirty     bool
	touchedAt int64 // UnixNano
	prev      *CacheEntry
	next      *CacheEntry
}

// WriteFn persists a cache entry to the backend.
type WriteFn func(key uint64, data []byte) error

// Cache is a write-back LRU cache backed by a doubly linked list and
// separate dirty/clean maps. Head is oldest, tail is newest.
type Cache struct {
	mu       sync.RWMutex
	dirty    map[uint64]*CacheEntry
	clean    map[uint64]*CacheEntry
	head     *CacheEntry
	tail     *CacheEntry
	count    int
	maxCount int

	writeFn  WriteFn
	entryTTL time.Duration
	errors   chan error
	stop     chan struct{}
}

func NewCache(maxCount int, ttl time.Duration, writeFn WriteFn) *Cache {
	return &Cache{
		dirty:    make(map[uint64]*CacheEntry),
		clean:    make(map[uint64]*CacheEntry),
		maxCount: maxCount,
		writeFn:  writeFn,
		entryTTL: ttl,
		errors:   make(chan error, 16),
	}
}

// --- linked list ops (caller must hold mu) ---

func (c *Cache) unlink(e *CacheEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (c *Cache) pushTail(e *CacheEntry) {
	e.prev = c.tail
	e.next = nil
	if c.tail != nil {
		c.tail.next = e
	} else {
		c.head = e
	}
	c.tail = e
}

func (c *Cache) moveToTail(e *CacheEntry) {
	c.unlink(e)
	c.pushTail(e)
}

// --- map ops (caller must hold mu) ---

func (c *Cache) lookup(key uint64) (*CacheEntry, bool) {
	if e, ok := c.dirty[key]; ok {
		return e, true
	}
	if e, ok := c.clean[key]; ok {
		return e, true
	}
	return nil, false
}

func (c *Cache) removeFromMaps(e *CacheEntry) {
	if e.dirty {
		delete(c.dirty, e.Key)
	} else {
		delete(c.clean, e.Key)
	}
}

func (c *Cache) setMap(e *CacheEntry) {
	if e.dirty {
		delete(c.clean, e.Key)
		c.dirty[e.Key] = e
	} else {
		delete(c.dirty, e.Key)
		c.clean[e.Key] = e
	}
}

// --- public API ---

// Get retrieves an entry and moves it to tail (most recently used).
func (c *Cache) Get(key uint64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.lookup(key)
	if !ok {
		return nil, false
	}
	c.moveToTail(e)
	e.touchedAt = time.Now().UnixNano()
	return e.Data, true
}

// Peek retrieves an entry without touching LRU order.
// Returns (data, dirty, exists).
func (c *Cache) Peek(key uint64) ([]byte, bool, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.lookup(key)
	if !ok {
		return nil, false, false
	}
	return e.Data, e.dirty, true
}

// Add inserts or updates an entry in the cache.
// When over capacity, evicts the oldest clean entry first; if none,
// flushes and evicts the oldest dirty entry.
func (c *Cache) Add(key uint64, data []byte, dirty bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixNano()

	if e, ok := c.lookup(key); ok {
		c.removeFromMaps(e)
		e.Data = data
		e.dirty = dirty
		e.touchedAt = now
		c.setMap(e)
		c.moveToTail(e)
		return
	}

	e := &CacheEntry{
		Key:       key,
		Data:      data,
		dirty:     dirty,
		touchedAt: now,
	}
	c.setMap(e)
	c.pushTail(e)
	c.count++

	for c.count > c.maxCount {
		c.evictOldest()
	}
}

// evictOldest evicts one entry. Prefers clean entries; if all dirty,
// writes the oldest dirty entry to backend first. Caller must hold mu.
func (c *Cache) evictOldest() {
	// Prefer evicting the oldest clean entry
	for e := c.head; e != nil; e = e.next {
		if !e.dirty {
			c.unlink(e)
			c.removeFromMaps(e)
			c.count--
			return
		}
	}
	// All entries are dirty; flush and evict head
	e := c.head
	if e == nil {
		return
	}
	if err := c.writeFn(e.Key, e.Data); err != nil {
		select {
		case c.errors <- err:
		default:
		}
	}
	c.unlink(e)
	c.removeFromMaps(e)
	c.count--
}

// Delete removes an entry from the cache.
func (c *Cache) Delete(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.lookup(key)
	if !ok {
		return
	}
	c.unlink(e)
	c.removeFromMaps(e)
	c.count--
}

// Flush writes the specified dirty keys to the backend and marks them clean.
func (c *Cache) Flush(keys []uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		e, ok := c.dirty[key]
		if !ok {
			continue
		}
		if err := c.writeFn(e.Key, e.Data); err != nil {
			return err
		}
		delete(c.dirty, key)
		e.dirty = false
		c.clean[key] = e
	}
	return nil
}

// FlushAll writes all dirty entries to the backend.
func (c *Cache) FlushAll() error {
	c.mu.Lock()
	keys := make([]uint64, 0, len(c.dirty))
	for k := range c.dirty {
		keys = append(keys, k)
	}
	c.mu.Unlock()
	return c.Flush(keys)
}

// evictExpired removes entries older than TTL, walking from head (oldest).
// Dirty entries are flushed before eviction.
func (c *Cache) evictExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := time.Now().Add(-c.entryTTL).UnixNano()
	for c.head != nil && c.head.touchedAt < cutoff {
		e := c.head
		if e.dirty {
			if err := c.writeFn(e.Key, e.Data); err != nil {
				select {
				case c.errors <- err:
				default:
				}
			}
		}
		c.unlink(e)
		c.removeFromMaps(e)
		c.count--
	}
}

// Start begins the periodic flusher and expiry goroutine.
func (c *Cache) Start(flushInterval time.Duration) {
	c.stop = make(chan struct{})
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := c.FlushAll(); err != nil {
					select {
					case c.errors <- err:
					default:
					}
				}
				c.evictExpired()
			case <-c.stop:
				return
			}
		}
	}()
}

// Stop stops the periodic flusher.
func (c *Cache) Stop() {
	if c.stop != nil {
		close(c.stop)
	}
}

// DrainErrors returns the first queued error, or nil.
func (c *Cache) DrainErrors() error {
	select {
	case err := <-c.errors:
		return err
	default:
		return nil
	}
}

// Count returns total number of entries.
func (c *Cache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}

// DirtyCount returns number of dirty entries.
func (c *Cache) DirtyCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.dirty)
}
