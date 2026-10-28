package actorxdist

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/go-aie/actorx"
)

func cacheTestContract(data []byte) wireContract {
	sum := sha256.Sum256(data)
	return wireContract{data, hex.EncodeToString(sum[:])}
}

func cachePut(t *testing.T, cache *contractCache, typ string, contract wireContract) wireContract {
	t.Helper()
	got, err := cache.put(typ, contract)
	if err != nil || got.fingerprint != contract.fingerprint || !bytes.Equal(got.data, contract.data) {
		t.Fatalf("put %q: got %v, error %v", typ, got, err)
	}
	return got
}

// Check actual ownership and byte lengths, not just the maintained counters.
func checkContractCache(t *testing.T, cache *contractCache, wantTypes, wantContracts, wantBytes int) {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.types) != wantTypes || cache.lru.Len() != wantTypes || len(cache.pool) != wantContracts || cache.residentBytes != wantBytes {
		t.Fatalf("types=%d, LRU=%d, pool=%d, bytes=%d; want %d/%d/%d", len(cache.types), cache.lru.Len(), len(cache.pool), cache.residentBytes, wantTypes, wantContracts, wantBytes)
	}
	refs := map[*sharedContract]int{}
	for entry := cache.lru.Front(); entry != nil; entry = entry.Next() {
		value := entry.Value.(cachedContractType)
		if cache.types[value.typ] != entry || cache.pool[value.shared.contract.fingerprint] != value.shared {
			t.Fatal("index, LRU and pool disagree")
		}
		refs[value.shared]++
	}
	actualBytes := 0
	for fingerprint, shared := range cache.pool {
		if fingerprint != shared.contract.fingerprint || shared.refs <= 0 || shared.refs != refs[shared] || len(shared.contract.data) > maxCachedContractBytes {
			t.Fatal("invalid pool ownership", fingerprint, shared.refs, refs[shared])
		}
		actualBytes += len(shared.contract.data)
	}
	if len(cache.types) > cache.capacity || len(cache.pool) > len(cache.types) || actualBytes != cache.residentBytes || actualBytes > cache.capacity*maxCachedContractBytes {
		t.Fatal("resident cache bounds exceeded", actualBytes)
	}
}

func TestContractCacheLRUAndSharing(t *testing.T) {
	cache := newContractCache(2)
	shared := cacheTestContract([]byte("shared"))
	other := cacheTestContract([]byte("other"))
	a := cachePut(t, cache, "a", shared)
	again := cachePut(t, cache, "a", shared)
	b := cachePut(t, cache, "b", shared)
	if &a.data[0] != &b.data[0] || &again.data[0] != &a.data[0] {
		t.Fatal("duplicate fills and Types must share canonical storage")
	}
	checkContractCache(t, cache, 2, 1, len(shared.data))
	if _, ok := cache.get("a"); !ok {
		t.Fatal("missing a")
	}
	cachePut(t, cache, "c", other) // Hit on a makes b the oldest Type.
	if _, ok := cache.get("b"); ok {
		t.Fatal("LRU Type was retained")
	}
	checkContractCache(t, cache, 2, 2, len(shared.data)+len(other.data))
	cachePut(t, cache, "d", other) // Remove the last cached reference to shared.
	checkContractCache(t, cache, 2, 1, len(other.data))
	if !bytes.Equal(a.data, shared.data) {
		t.Fatal("eviction invalidated a retained snapshot")
	}
	cachePut(t, cache, "a", shared) // Evicted Types can be filled again.
	checkContractCache(t, cache, 2, 2, len(shared.data)+len(other.data))
}

func TestContractCacheSingleSlot(t *testing.T) {
	cache := newContractCache(1)
	contract := cacheTestContract([]byte("same"))
	for i := range 100 {
		cachePut(t, cache, fmt.Sprint(i), contract)
		checkContractCache(t, cache, 1, 1, len(contract.data))
		if i > 0 {
			if _, ok := cache.get(fmt.Sprint(i - 1)); ok {
				t.Fatal("sharing bypassed Type capacity")
			}
		}
	}
}

func TestContractCacheOwnsBytes(t *testing.T) {
	cache := newContractCache(1)
	data := make([]byte, 4, maxCachedContractBytes*2)
	copy(data, "data")
	contract := cacheTestContract(data)
	got := cachePut(t, cache, "a", contract)
	data[0] = '!'
	if string(got.data) != "data" || cap(got.data) >= cap(data) {
		t.Fatal("cache retained the mutable or oversized input buffer")
	}
	cache.close()
	cache.close()
	checkContractCache(t, cache, 0, 0, 0)
	if _, ok := cache.get("a"); ok {
		t.Fatal("closed cache returned a Type")
	}
	if _, err := cache.put("late", contract); !errors.Is(err, actorx.ErrClosed) {
		t.Fatal("late fill was accepted", err)
	}
	checkContractCache(t, cache, 0, 0, 0)
	if string(got.data) != "data" {
		t.Fatal("close invalidated a retained snapshot")
	}
}

func TestContractCacheRejectsConflictsBeforeEviction(t *testing.T) {
	cache := newContractCache(2)
	first := cacheTestContract([]byte("first"))
	second := cacheTestContract([]byte("second"))
	cachePut(t, cache, "a", first)
	cachePut(t, cache, "b", second)
	for _, tc := range []struct {
		name, typ string
		contract  wireContract
	}{
		{"same Type different contract", "a", second},
		{"same Type different fingerprint", "a", wireContract{first.data, second.fingerprint}},
		{"same Type fingerprint collision", "a", wireContract{second.data, first.fingerprint}},
		{"pool fingerprint collision", "c", wireContract{second.data, first.fingerprint}},
		{"oversized", "c", cacheTestContract(make([]byte, maxCachedContractBytes+1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := cache.put(tc.typ, tc.contract); !errors.Is(err, ErrContract) {
				t.Fatal(err)
			}
			checkContractCache(t, cache, 2, 2, len(first.data)+len(second.data))
			if cache.lru.Back().Value.(cachedContractType).typ != "a" {
				t.Fatal("rejected fill changed LRU order")
			}
		})
	}
	cachePut(t, cache, "c", first) // Successful retry, including last-ref eviction.
	checkContractCache(t, cache, 2, 2, len(first.data)+len(second.data))
}

func TestContractCacheResidentBounds(t *testing.T) {
	limits, err := normalizeLimits(ExecutionLimits{}, TransportLimits{})
	if err != nil {
		t.Fatal(err)
	}
	capacity := limits.transport.ContractCacheTypes
	cache := newContractCache(capacity)
	for i := range capacity * 3 {
		// Distinct contents reach the full per-entry wire limit, including equality.
		data := bytes.Repeat([]byte{byte(i)}, maxCachedContractBytes)
		cachePut(t, cache, fmt.Sprint(i), cacheTestContract(data))
		count := min(i+1, capacity)
		checkContractCache(t, cache, count, count, count*maxCachedContractBytes)
	}
	if _, ok := cache.get("0"); ok {
		t.Fatal("old Type was not evicted")
	}
}

func TestContractCacheConcurrentFillsAndClose(t *testing.T) {
	cache := newContractCache(4)
	contract := cacheTestContract([]byte("shared"))
	var wg sync.WaitGroup
	// Every worker observes the same miss before any fill, as with concurrent I/O.
	start := make(chan struct{})
	for range 16 {
		if _, ok := cache.get("same"); ok {
			t.Fatal("expected initial miss")
		}
		wg.Go(func() {
			<-start
			cachePut(t, cache, "same", contract)
		})
	}
	close(start)
	wg.Wait()
	checkContractCache(t, cache, 1, 1, len(contract.data))
	for worker := range 16 {
		wg.Go(func() {
			for i := range 100 {
				typ := fmt.Sprintf("%d/%d", worker, i%8)
				cachePut(t, cache, typ, contract)
				cache.get(typ)
			}
		})
	}
	wg.Wait()
	checkContractCache(t, cache, 4, 1, len(contract.data))
	// Model a Registry read completing after close, without timing-based sleeps.
	late := make(chan struct{})
	wg.Go(func() {
		<-late
		if _, err := cache.put("late", contract); !errors.Is(err, actorx.ErrClosed) {
			t.Error("late fill was accepted", err)
		}
	})
	cache.close()
	close(late)
	wg.Wait()
	checkContractCache(t, cache, 0, 0, 0)
}
