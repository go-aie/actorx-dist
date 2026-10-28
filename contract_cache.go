package actorxdist

import (
	"bytes"
	"container/list"
	"sync"

	"github.com/go-aie/actorx"
)

const maxCachedContractBytes = 64 << 10

// contractCache owns immutable contract bytes. Returned wireContract snapshots
// are read-only, like buildContract results, and survive eviction and close.
// Registry reads and fingerprint computation belong to the caller, outside mu.
type contractCache struct {
	mu            sync.Mutex
	capacity      int
	types         map[string]*list.Element
	lru           list.List // Most recently used Type first.
	pool          map[string]*sharedContract
	residentBytes int // Canonical bytes counted once per pool entry.
	closed        bool
}

type sharedContract struct {
	contract wireContract
	refs     int // Cache Type references only, excluding in-flight snapshots.
}

type cachedContractType struct {
	typ    string
	shared *sharedContract
}

// capacity must be positive and validated by normalizeLimits. Allocate indexes
// as entries arrive rather than reserving the configured maximum up front.
func newContractCache(capacity int) *contractCache {
	return &contractCache{
		capacity: capacity,
		types:    make(map[string]*list.Element),
		pool:     make(map[string]*sharedContract),
	}
}

func (c *contractCache) get(typ string) (wireContract, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.types[typ]; entry != nil {
		c.lru.MoveToFront(entry)
		return entry.Value.(cachedContractType).shared.contract, true
	}
	return wireContract{}, false
}

// put rechecks a miss after the caller's Registry read. Conflicting concurrent
// fills must fail before changing the cache, including before any eviction.
func (c *contractCache) put(typ string, contract wireContract) (wireContract, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return wireContract{}, actorx.ErrClosed
	}
	if len(contract.data) > maxCachedContractBytes {
		return wireContract{}, ErrContract
	}
	if entry := c.types[typ]; entry != nil {
		existing := entry.Value.(cachedContractType).shared.contract
		if existing.fingerprint != contract.fingerprint || !bytes.Equal(existing.data, contract.data) {
			return wireContract{}, ErrContract
		}
		c.lru.MoveToFront(entry)
		return existing, nil
	}
	if shared := c.pool[contract.fingerprint]; shared != nil && !bytes.Equal(shared.contract.data, contract.data) {
		return wireContract{}, ErrContract
	}
	if len(c.types) == c.capacity {
		c.evict()
	}
	// Eviction may have removed the last reference to the candidate fingerprint.
	shared := c.pool[contract.fingerprint]
	if shared == nil {
		contract.data = bytes.Clone(contract.data) // Never retain mutable SDK buffers.
		shared = &sharedContract{contract: contract}
		c.pool[contract.fingerprint] = shared
		c.residentBytes += len(contract.data)
	}
	shared.refs++
	c.types[typ] = c.lru.PushFront(cachedContractType{typ, shared})
	return shared.contract, nil
}

// evict runs only while mu is held and removes exactly one Type slot.
func (c *contractCache) evict() {
	entry := c.lru.Back()
	value := entry.Value.(cachedContractType)
	delete(c.types, value.typ)
	c.lru.Remove(entry)
	value.shared.refs--
	if value.shared.refs == 0 {
		delete(c.pool, value.shared.contract.fingerprint)
		c.residentBytes -= len(value.shared.contract.data)
	}
}

// close releases all cached references and rejects late fills permanently.
// Runtime work admission and draining remain the network owner's responsibility.
func (c *contractCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.types = nil
	c.lru.Init()
	c.pool = nil
	c.residentBytes = 0
}
