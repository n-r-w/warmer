package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/n-r-w/warmer"
)

// Simple in-memory cache implementation
type Cache struct {
	mu    sync.RWMutex
	items map[string]string
}

func NewCache() *Cache {
	return &Cache{
		items: make(map[string]string),
	}
}

func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = value
}

func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.items[key]
	return value, ok
}

// Simple database simulation
type DB struct {
	data map[string]string
}

func NewDB() *DB {
	return &DB{
		data: map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}
}

func (db *DB) Get(key string) (string, error) {
	if value, ok := db.data[key]; ok {
		return value, nil
	}
	return "", fmt.Errorf("key %s not found", key)
}

func main() {
	// Initialize cache and database
	cache := NewCache()
	db := NewDB()

	// Create warmer configuration
	config := warmer.Config[string]{
		Interval:     time.Second,          // Warm cache every 5 seconds
		BatchSize:    2,                  // Process 100 keys per batch
		MaxStatsSize: 1000,                 // Keep stats for 1000 keys
		RateInterval: warmer.RateInterval1, // Use 1-minute rate interval
		OnWarmTrack: func(_ context.Context, key string, rate float64) {
			log.Printf("Key warmed: %s, rate: %.2f", key, rate)
		},
		OnKeyTrack: func(_ context.Context, key string, rate float64) {
			log.Printf("Key tracked: %s, rate: %.2f", key, rate)
		},
		OnDBError: func(_ context.Context, key string, err error) {
			log.Printf("DB error for key %s: %v", key, err)
		},
		OnCacheError: func(_ context.Context, key string, err error) {
			log.Printf("Cache error for key %s: %v", key, err)
		},
	}

	// Create warmer instance
	w := warmer.New(
		// Function to get data from DB
		func(ctx context.Context, key string) (string, error) {
			return db.Get(key)
		},
		// Function to set data in cache
		func(ctx context.Context, key string, value string) error {
			cache.Set(key, value)
			return nil
		},
		config,
	)

	// Start the warmer
	ctx := context.Background()
	w.Start(ctx)

	// Simulate key updates
	var (
		now = time.Now()
		i   int
	)

	for time.Since(now) < time.Second*10 {
		i++
		w.TrackUpdate(ctx, "key1") // This key will have higher update rate
		if i%2 == 0 {
			w.TrackUpdate(ctx, "key2") // This key will have medium update rate
		}
		if i%3 == 0 {
			w.TrackUpdate(ctx, "key3") // This key will have lower update rate
		}
		time.Sleep(time.Millisecond * 500)
	}

	// Wait a bit to see the warming in action
	time.Sleep(time.Second * 2)

	log.Println("Stopping the warmer...")
	w.Stop()
	log.Println("Warmer stopped.")
}
