/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 * Copyright 2021 The Vitess Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package ristretto is a fast, fixed size, in-memory cache with a dual focus on
// throughput and hit ratio performance. You can easily add Ristretto to an
// existing system and keep the most valuable data where you need it.
package ristretto

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"vitess.io/vitess/go/hack"
)

var (
	// TODO: find the optimal value for this or make it configurable
	setBufSize = 32 * 1024
)

func defaultStringHash(key string) (uint64, uint64) {
	const Seed1 = uint64(0x1122334455667788)
	const Seed2 = uint64(0x8877665544332211)
	return hack.RuntimeStrhash(key, Seed1), hack.RuntimeStrhash(key, Seed2)
}

type itemCallback func(*Item)

// CacheItemSize is the overhead in bytes for every stored cache item
var CacheItemSize = hack.RuntimeAllocSize(int64(unsafe.Sizeof(storeItem{})))

// Cache is a thread-safe implementation of a hashmap with a TinyLFU admission
// policy and a Sampled LFU eviction policy. You can use the same Cache instance
// from as many goroutines as you want.
type Cache struct {
	// store is the central concurrent hashmap where key-value items are stored.
	store store
	// policy determines what gets let in to the cache and what gets kicked out.
	policy policy
	// getBuf is a custom ring buffer implementation that gets pushed to when
	// keys are read.
	getBuf *ringBuffer
	// setBuf is a buffer allowing us to batch/drop Sets during times of high
	// contention.
	setBuf chan *Item
	// onEvict is called for item evictions.
	onEvict itemCallback
	// onReject is called when an item is rejected via admission policy.
	onReject itemCallback
	// onExit is called whenever a value goes out of scope from the cache.
	onExit func(any)
	// KeyToHash function is used to customize the key hashing algorithm.
	// Each key will be hashed using the provided function. If keyToHash value
	// is not set, the default keyToHash function is used.
	keyToHash func(string) (uint64, uint64)
	// stop is used to stop the processItems goroutine.
	stop chan struct{}
	// indicates whether cache is closed.
	isClosed atomic.Bool
	// cost calculates cost from a value.
	cost func(value any) int64
	// ignoreInternalCost dictates whether to ignore the cost of internally storing
	// the item in the cost calculation.
	ignoreInternalCost bool
	// Metrics contains a running log of important statistics like hits, misses,
	// and dropped items.
	Metrics *Metrics
}

// Config is passed to NewCache for creating new Cache instances.
type Config struct {
	// NumCounters determines the number of counters (keys) to keep that hold
	// access frequency information. It's generally a good idea to have more
	// counters than the max cache capacity, as this will improve eviction
	// accuracy and subsequent hit ratios.
	//
	// For example, if you expect your cache to hold 1,000,000 items when full,
	// NumCounters should be 10,000,000 (10x). Each counter takes up 4 bits, so
	// keeping 10,000,000 counters would require 5MB of memory.
	NumCounters int64
	// MaxCost can be considered as the cache capacity, in whatever units you
	// choose to use.
	//
	// For example, if you want the cache to have a max capacity of 100MB, you
	// would set MaxCost to 100,000,000 and pass an item's number of bytes as
	// the `cost` parameter for calls to Set. If new items are accepted, the
	// eviction process will take care of making room for the new item and not
	// overflowing the MaxCost value.
	MaxCost int64
	// BufferItems determines the size of Get buffers.
	//
	// Unless you have a rare use case, using `64` as the BufferItems value
	// results in good performance.
	BufferItems int64
	// Metrics determines whether cache statistics are kept during the cache's
	// lifetime. There *is* some overhead to keeping statistics, so you should
	// only set this flag to true when testing or throughput performance isn't a
	// major factor.
	Metrics bool
	// OnEvict is called for every eviction and passes the hashed key, value,
	// and cost to the function.
	OnEvict func(item *Item)
	// OnReject is called for every rejection done via the policy.
	OnReject func(item *Item)
	// OnExit is called whenever a value is removed from cache. This can be
	// used to do manual memory deallocation. Would also be called on eviction
	// and rejection of the value.
	OnExit func(val any)
	// KeyToHash function is used to customize the key hashing algorithm.
	// Each key will be hashed using the provided function. If keyToHash value
	// is not set, the default keyToHash function is used.
	KeyToHash func(string) (uint64, uint64)
	// Cost evaluates a value and outputs a corresponding cost. This function
	// is ran after Set is called for a new item or an item update with a cost
	// param of 0.
	Cost func(value any) int64
	// IgnoreInternalCost set to true indicates to the cache that the cost of
	// internally storing the value should be ignored. This is useful when the
	// cost passed to set is not using bytes as units. Keep in mind that setting
	// this to true will increase the memory usage.
	IgnoreInternalCost bool
}

type itemFlag byte

const (
	itemNew itemFlag = iota
	itemDelete
	itemUpdate
)

// Item is passed to setBuf so items can eventually be added to the cache.
type Item struct {
	flag     itemFlag
	Key      uint64
	Conflict uint64
	Value    any
	Cost     int64
	wg       *sync.WaitGroup
}

// NewCache returns a new Cache instance and any configuration errors, if any.
func NewCache(config *Config) (*Cache, error) {
	switch {
	case config.NumCounters == 0:
		return nil, errors.New("NumCounters can't be zero")
	case config.MaxCost == 0:
		return nil, errors.New("Capacity can't be zero")
	case config.BufferItems == 0:
		return nil, errors.New("BufferItems can't be zero")
	}
	policy := newPolicy(config.NumCounters, config.MaxCost)
	cache := &Cache{
		store:              newStore(),
		policy:             policy,
		getBuf:             newRingBuffer(policy, config.BufferItems),
		setBuf:             make(chan *Item, setBufSize),
		keyToHash:          config.KeyToHash,
		stop:               make(chan struct{}),
		cost:               config.Cost,
		ignoreInternalCost: config.IgnoreInternalCost,
	}
	cache.onExit = func(val any) {
		if config.OnExit != nil && val != nil {
			config.OnExit(val)
		}
	}
	cache.onEvict = func(item *Item) {
		if config.OnEvict != nil {
			config.OnEvict(item)
		}
		cache.onExit(item.Value)
	}
	cache.onReject = func(item *Item) {
		if config.OnReject != nil {
			config.OnReject(item)
		}
		cache.onExit(item.Value)
	}
	if cache.keyToHash == nil {
		cache.keyToHash = defaultStringHash
	}
	if config.Metrics {
		cache.collectMetrics()
	}
	// NOTE: benchmarks seem to show that performance decreases the more
	//       goroutines we have running cache.processItems(), so 1 should
	//       usually be sufficient
	go cache.processItems()
	return cache, nil
}

// Wait blocks until all the current cache operations have been processed in the background
func (c *Cache) Wait() {
	if c == nil || c.isClosed.Load() {
		return
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.setBuf <- &Item{wg: wg}
	wg.Wait()
}

// Get returns the value (if any) and a boolean representing whether the
// value was found or not. The value can be nil and the boolean can be true at
// the same time.
func (c *Cache) Get(key string) (any, bool) {
	if c == nil || c.isClosed.Load() {
		return nil, false
	}
	keyHash, conflictHash := c.keyToHash(key)
	c.getBuf.Push(keyHash)
	value, ok := c.store.Get(keyHash, conflictHash)
	if ok {
		c.Metrics.add(hit, keyHash, 1)
	} else {
		c.Metrics.add(miss, keyHash, 1)
	}
	return value, ok
}

// Set attempts to add the key-value item to the cache. If it returns false,
// then the Set was dropped and the key-value item isn't added to the cache. If
// it returns true, there's still a chance it could be dropped by the policy if
// its determined that the key-value item isn't worth keeping, but otherwise the
// item will be added and other items will be evicted in order to make room.
//
// The cost of the entry will be evaluated lazily by the cache's Cost function.
func (c *Cache) Set(key string, value any) bool {
	return c.SetWithCost(key, value, 0)
}

// SetWithCost works like Set but adds a key-value pair to the cache with a specific
// cost. The built-in Cost function will not be called to evaluate the object's cost
// and instead the given value will be used.
func (c *Cache) SetWithCost(key string, value any, cost int64) bool {
	if c == nil || c.isClosed.Load() {
		return false
	}

	keyHash, conflictHash := c.keyToHash(key)
	i := &Item{
		flag:     itemNew,
		Key:      keyHash,
		Conflict: conflictHash,
		Value:    value,
		Cost:     cost,
	}
	// cost is eventually updated. The expiration must also be immediately updated
	// to prevent items from being prematurely removed from the map.
	if prev, ok := c.store.Update(i); ok {
		c.onExit(prev)
		i.flag = itemUpdate
	}
	// Attempt to send item to policy.
	select {
	case c.setBuf <- i:
		return true
	default:
		if i.flag == itemUpdate {
			// Return true if this was an update operation since we've already
			// updated the store. For all the other operations (set/delete), we
			// return false which means the item was not inserted.
			return true
		}
		c.Metrics.add(dropSets, keyHash, 1)
		return false
	}
}

// Delete deletes the key-value item from the cache if it exists.
func (c *Cache) Delete(key string) {
	if c == nil || c.isClosed.Load() {
		return
	}
	keyHash, conflictHash := c.keyToHash(key)
	// Delete immediately.
	_, prev := c.store.Del(keyHash, conflictHash)
	c.onExit(prev)
	// If we've set an item, it would be applied slightly later.
	// So we must push the same item to `setBuf` with the deletion flag.
	// This ensures that if a set is followed by a delete, it will be
	// applied in the correct order.
	c.setBuf <- &Item{
		flag:     itemDelete,
		Key:      keyHash,
		Conflict: conflictHash,
	}
}

// Close stops all goroutines and closes all channels.
func (c *Cache) Close() {
	if c == nil {
		return
	}
	wasClosed := c.isClosed.Swap(true)
	if wasClosed {
		return
	}
	c.Clear()

	// Block until processItems goroutine is returned.
	c.stop <- struct{}{}
	close(c.stop)
	close(c.setBuf)
	c.policy.Close()
	c.isClosed.Store(true)
}

// Clear empties the hashmap and zeroes all policy counters. Note that this is
// not an atomic operation (but that shouldn't be a problem as it's assumed that
// Set/Get calls won't be occurring until after this).
func (c *Cache) Clear() {
	if c == nil || c.isClosed.Load() {
		return
	}
	// Block until processItems goroutine is returned.
	c.stop <- struct{}{}

	// Clear out the setBuf channel.
loop:
	for {
		select {
		case i := <-c.setBuf:
			if i.wg != nil {
				i.wg.Done()
				continue
			}
			if i.flag != itemUpdate {
				// In itemUpdate, the value is already set in the store.  So, no need to call
				// onEvict here.
				c.onEvict(i)
			}
		default:
			break loop
		}
	}

	// Clear value hashmap and policy data.
	c.policy.Clear()
	c.store.Clear(c.onEvict)
	// Only reset metrics if they're enabled.
	if c.Metrics != nil {
		c.Metrics.Clear()
	}
	// Restart processItems goroutine.
	go c.processItems()
}

// Len returns the size of the cache (in entries)
func (c *Cache) Len() int {
	if c == nil {
		return 0
	}
	return c.store.Len()
}

// UsedCapacity returns the size of the cache (in bytes)
func (c *Cache) UsedCapacity() int64 {
	if c == nil {
		return 0
	}
	return c.policy.Used()
}

// MaxCapacity returns the max cost of the cache (in bytes)
func (c *Cache) MaxCapacity() int64 {
	if c == nil {
		return 0
	}
	return c.policy.MaxCost()
}

// SetCapacity updates the maxCost of an existing cache.
func (c *Cache) SetCapacity(maxCost int64) {
	if c == nil {
		return
	}
	c.policy.UpdateMaxCost(maxCost)
}

// Evictions returns the number of evictions
func (c *Cache) Evictions() int64 {
	// TODO
	if c == nil || c.Metrics == nil {
		return 0
	}
	return int64(c.Metrics.KeysEvicted())
}

// Hits returns the number of cache hits
func (c *Cache) Hits() int64 {
	if c == nil || c.Metrics == nil {
		return 0
	}
	return int64(c.Metrics.Hits())
}

// Misses returns the number of cache misses
func (c *Cache) Misses() int64 {
	if c == nil || c.Metrics == nil {
		return 0
	}
	return int64(c.Metrics.Misses())
}

// ForEach yields all the values currently stored in the cache to the given callback.
// The callback may return `false` to stop the iteration early.
func (c *Cache) ForEach(forEach func(any) bool) {
	if c == nil {
		return
	}
	c.store.ForEach(forEach)
}

// processItems is ran by goroutines processing the Set buffer.
func (c *Cache) processItems() {
	startTs := make(map[uint64]time.Time)
	numToKeep := 100000 // TODO: Make this configurable via options.

	trackAdmission := func(key uint64) {
		if c.Metrics == nil {
			return
		}
		startTs[key] = time.Now()
		if len(startTs) > numToKeep {
			for k := range startTs {
				if len(startTs) <= numToKeep {
					break
				}
				delete(startTs, k)
			}
		}
	}
	onEvict := func(i *Item) {
		delete(startTs, i.Key)
		if c.onEvict != nil {
			c.onEvict(i)
		}
	}

	for {
		select {
		case i := <-c.setBuf:
			if i.wg != nil {
				i.wg.Done()
				continue
			}
			// Calculate item cost value if new or update.
			if i.Cost == 0 && c.cost != nil && i.flag != itemDelete {
				i.Cost = c.cost(i.Value)
			}
			if !c.ignoreInternalCost {
				// Add the cost of internally storing the object.
				i.Cost += CacheItemSize
			}

			switch i.flag {
			case itemNew:
				victims, added := c.policy.Add(i.Key, i.Cost)
				if added {
					c.store.Set(i)
					c.Metrics.add(keyAdd, i.Key, 1)
					trackAdmission(i.Key)
				} else {
					c.onReject(i)
				}
				for _, victim := range victims {
					victim.Conflict, victim.Value = c.store.Del(victim.Key, 0)
					onEvict(victim)
				}

			case itemUpdate:
				c.policy.Update(i.Key, i.Cost)

			case itemDelete:
				c.policy.Del(i.Key) // Deals with metrics updates.
				_, val := c.store.Del(i.Key, i.Conflict)
				c.onExit(val)
			}
		case <-c.stop:
			return
		}
	}
}

// collectMetrics just creates a new *Metrics instance and adds the pointers
// to the cache and policy instances.
func (c *Cache) collectMetrics() {
	c.Metrics = newMetrics()
	c.policy.CollectMetrics(c.Metrics)
}

type metricType int

const (
	// The following 2 keep track of hits and misses.
	hit = iota
	miss
	// The following 3 keep track of number of keys added, updated and evicted.
	keyAdd
	keyUpdate
	keyEvict
	// The following 2 keep track of cost of keys added and evicted.
	costAdd
	costEvict
	// The following keep track of how many sets were dropped or rejected later.
	dropSets
	rejectSets
	// The following 2 keep track of how many gets were kept and dropped on the
	// floor.
	dropGets
	keepGets
	// This should be the final enum. Other enums should be set before this.
	doNotUse
)

func stringFor(t metricType) string {
	switch t {
	case hit:
		return "hit"
	case miss:
		return "miss"
	case keyAdd:
		return "keys-added"
	case keyUpdate:
		return "keys-updated"
	case keyEvict:
		return "keys-evicted"
	case costAdd:
		return "cost-added"
	case costEvict:
		return "cost-evicted"
	case dropSets:
		return "sets-dropped"
	case rejectSets:
		return "sets-rejected" // by policy.
	case dropGets:
		return "gets-dropped"
	case keepGets:
		return "gets-kept"
	default:
		return "unidentified"
	}
}

// Metrics is a snapshot of performance statistics for the lifetime of a cache instance.
type Metrics struct {
	all [doNotUse][]*uint64
}

func newMetrics() *Metrics {
	s := &Metrics{}
	for i := 0; i < doNotUse; i++ {
		s.all[i] = make([]*uint64, 256)
		slice := s.all[i]
		for j := range slice {
			slice[j] = new(uint64)
		}
	}
	return s
}

func (p *Metrics) add(t metricType, hash, delta uint64) {
	if p == nil {
		return
	}
	valp := p.all[t]
	// Avoid false sharing by padding at least 64 bytes of space between two
	// atomic counters which would be incremented.
	idx := (hash % 25) * 10
	atomic.AddUint64(valp[idx], delta)
}

func (p *Metrics) get(t metricType) uint64 {
	if p == nil {
		return 0
	}
	valp := p.all[t]
	var total uint64
	for i := range valp {
		total += atomic.LoadUint64(valp[i])
	}
	return total
}

// Hits is the number of Get calls where a value was found for the corresponding key.
func (p *Metrics) Hits() uint64 {
	return p.get(hit)
}

// Misses is the number of Get calls where a value was not found for the corresponding key.
func (p *Metrics) Misses() uint64 {
	return p.get(miss)
}

// KeysAdded is the total number of Set calls where a new key-value item was added.
func (p *Metrics) KeysAdded() uint64 {
	return p.get(keyAdd)
}

// KeysUpdated is the total number of Set calls where the value was updated.
func (p *Metrics) KeysUpdated() uint64 {
	return p.get(keyUpdate)
}

// KeysEvicted is the total number of keys evicted.
func (p *Metrics) KeysEvicted() uint64 {
	return p.get(keyEvict)
}

// CostAdded is the sum of costs that have been added (successful Set calls).
func (p *Metrics) CostAdded() uint64 {
	return p.get(costAdd)
}

// CostEvicted is the sum of all costs that have been evicted.
func (p *Metrics) CostEvicted() uint64 {
	return p.get(costEvict)
}

// SetsDropped is the number of Set calls that don't make it into internal
// buffers (due to contention or some other reason).
func (p *Metrics) SetsDropped() uint64 {
	return p.get(dropSets)
}

// SetsRejected is the number of Set calls rejected by the policy (TinyLFU).
func (p *Metrics) SetsRejected() uint64 {
	return p.get(rejectSets)
}

// GetsDropped is the number of Get counter increments that are dropped
// internally.
func (p *Metrics) GetsDropped() uint64 {
	return p.get(dropGets)
}

// GetsKept is the number of Get counter increments that are kept.
func (p *Metrics) GetsKept() uint64 {
	return p.get(keepGets)
}

// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
// percentage of successful Get calls.
func (p *Metrics) Ratio() float64 {
	if p == nil {
		return 0.0
	}
	hits, misses := p.get(hit), p.get(miss)
	if hits == 0 && misses == 0 {
		return 0.0
	}
	return float64(hits) / float64(hits+misses)
}

// Clear resets all the metrics.
func (p *Metrics) Clear() {
	if p == nil {
		return
	}
	for i := 0; i < doNotUse; i++ {
		for j := range p.all[i] {
			atomic.StoreUint64(p.all[i][j], 0)
		}
	}
}

// String returns a string representation of the metrics.
func (p *Metrics) String() string {
	if p == nil {
		return ""
	}
	var buf bytes.Buffer
	for i := 0; i < doNotUse; i++ {
		t := metricType(i)
		fmt.Fprintf(&buf, "%s: %d ", stringFor(t), p.get(t))
	}
	fmt.Fprintf(&buf, "gets-total: %d ", p.get(hit)+p.get(miss))
	fmt.Fprintf(&buf, "hit-ratio: %.2f", p.Ratio())
	return buf.String()
}
