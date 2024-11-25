//nolint:revive // test
package warmer

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testDB struct {
	data    map[string]any
	mu      sync.RWMutex
	getFunc func(ctx context.Context, key string) (any, error)
}

func (db *testDB) get(ctx context.Context, key string) (any, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.getFunc != nil {
		return db.getFunc(ctx, key)
	}

	if value, ok := db.data[key]; ok {
		return value, nil
	}
	return nil, errors.New("not found")
}

type testCache struct {
	data map[string]any
	mu   sync.RWMutex
	err  error // Add error field to simulate cache errors
}

func (c *testCache) set(ctx context.Context, key string, value any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return c.err
	}

	c.data[key] = value
	return nil
}

func logStats(t *testing.T, w *Warmer[string, any], cache *testCache, message string) {
	t.Logf("====== %s ======", message)

	// log the stats
	t.Logf("stats:")
	w.mu.Lock()
	if len(w.stats) == 0 {
		t.Logf("empty")
	}
	for key, stat := range w.stats {
		t.Logf("key=%s, rate=%f", key, stat.getUpdateRate(w.rateInterval))
	}
	w.mu.Unlock()

	// log the cache
	t.Logf("cache:")
	cache.mu.RLock()
	if cache.err != nil {
		t.Logf("error: %v", cache.err)
	}
	if len(cache.data) == 0 {
		t.Logf("empty")
	}

	for key := range cache.data {
		t.Logf("key=%s", key)
	}
	cache.mu.RUnlock()
}

// TestStartStop tests the Start and Stop methods.
func TestStartStop(t *testing.T) {
	t.Parallel()

	// Initialize test dependencies
	db := &testDB{
		data: map[string]any{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}
	cache := &testCache{
		data: make(map[string]any),
	}

	// Create warmer with custom config
	w := New(db.get, cache.set, Config[string]{
		Interval:     time.Millisecond * 100,
		BatchSize:    2,
		MaxStatsSize: 10,
		RateInterval: RateInterval1,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Start the warmer
	w.Start(ctx)

	time.Sleep(time.Second)

	// Stop the warmer
	w.Stop()

	// Verify that the warmer is stopped
	assert.Equal(t, int32(0), atomic.LoadInt32(&w.running))

	// Start again
	w.Start(ctx)

	// Verify that the warmer is running
	assert.Equal(t, int32(1), atomic.LoadInt32(&w.running))

	// cancel the context
	cancel()

	// Wait for the context to be canceled
	time.Sleep(time.Second)

	// Verify that the warmer is stopped
	assert.Equal(t, int32(0), atomic.LoadInt32(&w.running))

	// Stop again
	w.Stop()

	// Verify that nothing happens
	assert.Equal(t, int32(0), atomic.LoadInt32(&w.running))

	// start again
	ctx, cancel = context.WithCancel(context.Background())
	w.Start(ctx)

	// stop and cancel in parallel
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		w.Stop()
	}()
	go func() {
		defer wg.Done()
		cancel()
	}()
	wg.Wait()

	// Verify that the warmer is stopped
	assert.Equal(t, int32(0), atomic.LoadInt32(&w.running))
}

// TestWarmBatch tests the probability-based key selection in warmBatch method.
func TestWarmBatch(t *testing.T) {
	t.Parallel()

	// Initialize test dependencies
	db := &testDB{
		data: map[string]any{
			"key1": "value1", // frequently updated
			"key2": "value2", // moderately updated
			"key3": "value3", // rarely updated
			"key4": "value4", // zero rate
		},
	}
	cache := &testCache{
		data: make(map[string]any),
	}

	// Create warmer with small batch size to test probability selection
	w := New(db.get, cache.set, Config[string]{
		Interval:     time.Millisecond * 100,
		BatchSize:    2, // Small batch size to test selection
		MaxStatsSize: 10,
		RateInterval: RateInterval1,
	})

	ctx := context.Background()

	// Track all keys
	for i := 1; i <= 4; i++ {
		w.TrackUpdate(ctx, fmt.Sprintf("key%d", i), 1)
	}
	// At this point, no statistics should be collected due to small time interval

	// Simulate different update frequencies
	// Need at least 10 seconds for metrics to collect reliable statistics
	start := time.Now()
	for time.Since(start) < 10*time.Second {
		// key1 - frequent updates (3 updates per iteration)
		w.TrackUpdate(ctx, "key1", 3)
		// key2 - moderate updates (2 updates per iteration)
		w.TrackUpdate(ctx, "key2", 2)
		// key3 - rare updates (1 update per iteration)
		w.TrackUpdate(ctx, "key3", 1)
		// key4 - no updates

		time.Sleep(time.Millisecond * 100)
	}

	// Log current rates before warming
	logStats(t, w, cache, "Before warming")

	// Run warmBatch multiple times to collect statistics
	selectionStats := make(map[string]int)
	const warmingIterations = 1000

	for i := 0; i < warmingIterations; i++ {
		cache.data = make(map[string]any) // Clear cache before each warming

		w.warmBatch(ctx)

		// Count selected keys
		for key := range cache.data {
			selectionStats[key]++
		}
	}

	t.Logf("Selection statistics after %d iterations:", warmingIterations)
	for key, count := range selectionStats {
		percentage := float64(count) * 100 / warmingIterations
		t.Logf("%s: selected %d times (%.2f%%)", key, count, percentage)
	}

	// Verify probability distribution
	// key1 (frequent) should be selected more often than key2 (moderate)
	assert.Greater(t, selectionStats["key1"], selectionStats["key2"],
		"frequently updated key should be selected more often than moderately updated")

	// key2 (moderate) should be selected more often than key3 (rare)
	assert.Greater(t, selectionStats["key2"], selectionStats["key3"],
		"moderately updated key should be selected more often than rarely updated")

	// key3 (rare) should be selected more often than key4 (zero rate)
	assert.Greater(t, selectionStats["key3"], selectionStats["key4"],
		"rarely updated key should be selected more often than zero rate")

	// Verify cached values are correct
	for key, value := range cache.data {
		expectedValue := fmt.Sprintf("value%s", key[3:]) // Extract number from key
		assert.Equal(t, expectedValue, value, "cached value should match expected value")
	}
}

// TestCleanupOldStats tests the cleanup of old statistics.
func TestCleanupOldStats(t *testing.T) {
	t.Parallel()

	// Initialize test dependencies
	db := &testDB{}
	cache := &testCache{}

	// Create warmer with small batch size to test probability selection
	w := New(db.get, cache.set, Config[string]{
		MaxStatsSize: 3,
		RateInterval: RateInterval1,
	})

	ctx := context.Background()

	// Helper to simulate updates over time
	simulateUpdates := func(key string, updatesPerSec int, duration time.Duration) {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		start := time.Now()
		for time.Since(start) < duration {
			<-ticker.C
			for i := 0; i < updatesPerSec; i++ {
				w.TrackUpdate(ctx, key, 1)
			}
		}
	}

	// Run updates in parallel for different keys
	var wg sync.WaitGroup
	wg.Add(4)

	// Accumulate stats for 15 seconds to ensure stable rates
	go func() {
		defer wg.Done()
		simulateUpdates("key1", 10, 10*time.Second) // highest rate: 10 updates/sec
	}()
	go func() {
		defer wg.Done()
		simulateUpdates("key2", 5, 10*time.Second) // medium rate: 5 updates/sec
	}()
	go func() {
		defer wg.Done()
		simulateUpdates("key3", 2, 10*time.Second) // low rate: 2 updates/sec
	}()
	go func() {
		defer wg.Done()
		simulateUpdates("key4", 1, time.Second) // will become zero rate (stopped early)
	}()

	// Wait for all updates to complete
	wg.Wait()

	// Add one more key to trigger cleanup
	w.TrackUpdate(ctx, "key5", 1)

	// Get rates for verification
	logStats(t, w, cache, "Before cleanup")

	// Verify final state
	assert.Len(t, w.stats, 4, "should maintain maxStatsSize")

	// Verify that key3 (lowest non-zero rate) was removed
	_, exists := w.stats["key3"]
	assert.False(t, exists, "key with lowest non-zero rate should be removed")

	// Verify that all other keys exist
	_, exists = w.stats["key1"]
	assert.True(t, exists, "key1 should exist")
	_, exists = w.stats["key2"]
	assert.True(t, exists, "key2 should exist")
	_, exists = w.stats["key4"]
	assert.True(t, exists, "key4 should exist")
	_, exists = w.stats["key5"]
	assert.True(t, exists, "key5 should exist")
}

// TestConcurrentUpdates tests concurrent key updates from multiple goroutines.
func TestConcurrentUpdates(t *testing.T) {
	t.Parallel()

	// Initialize test dependencies
	db := &testDB{
		data: map[string]any{
			"existing1": "value1",
			"existing2": "value2",
			"existing3": "value3",
		},
	}
	cache := &testCache{
		data: make(map[string]any),
	}

	const maxStatsSize = 1000

	var warmedKeysCount int64

	// Create warmer with custom config
	w := New(db.get, cache.set, Config[string]{
		Interval:     time.Millisecond * 100,
		BatchSize:    5,
		MaxStatsSize: maxStatsSize,
		RateInterval: RateInterval1,
		OnWarmTrack: func(ctx context.Context, key string, rate float64) {
			atomic.AddInt64(&warmedKeysCount, 1)
		},
	})

	ctx := context.Background()

	// Start the warmer
	w.Start(ctx)
	defer w.Stop()

	// Number of goroutines and operations
	const (
		numGoroutines   = 5
		opsPerGoroutine = 200
		// Limit new keys to stay within maxStatsSize
		maxNewKeys = maxStatsSize - 3 // subtract existing keys
		newKeyProb = 0.3              // 30% chance of new key
	)

	var (
		wg           sync.WaitGroup
		newKeysCount int64
	)
	wg.Add(numGoroutines)

	// Track start time for rate calculation
	start := time.Now()

	// Launch goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			// Each goroutine performs a mix of operations
			for time.Since(start) < 10*time.Second {
				for j := 0; j < opsPerGoroutine; j++ {
					if rand.Float64() < newKeyProb && atomic.LoadInt64(&newKeysCount) < maxNewKeys {
						// Add new key
						key := fmt.Sprintf("new_key_r%d_op%d", routineID, j)
						w.TrackUpdate(ctx, key, 1)
						atomic.AddInt64(&newKeysCount, 1)
					} else {
						// Update existing key
						key := fmt.Sprintf("existing%d", rand.Intn(3)+1)
						w.TrackUpdate(ctx, key, 1)
					}

					// Small sleep to simulate real-world load
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Log final statistics
	duration := time.Since(start)
	totalOps := numGoroutines * opsPerGoroutine
	opsPerSec := float64(totalOps) / duration.Seconds()

	t.Logf("Test completed in %.2f seconds", duration.Seconds())
	t.Logf("Total operations: %d", totalOps)
	t.Logf("Operations per second: %.2f", opsPerSec)
	t.Logf("New keys created: %d", atomic.LoadInt64(&newKeysCount))
	t.Logf("Warm operations count: %d", atomic.LoadInt64(&warmedKeysCount))

	// Verify that warmer is still running
	assert.Equal(t, int32(1), atomic.LoadInt32(&w.running))

	// Verify that stats were collected
	w.mu.Lock()
	statsCount := len(w.stats)
	w.mu.Unlock()
	assert.Greater(t, statsCount, 0, "No statistics were collected")
	assert.LessOrEqual(t, statsCount, maxStatsSize, "Statistics exceeded maxStatsSize")
}
