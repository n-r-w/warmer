# Warmer

Warmer is a Go package that provides intelligent cache warming functionality. It tracks key access patterns and proactively warms up frequently accessed keys in your cache to improve application performance.

## Features

- Automatic cache warming based on key access patterns
- Configurable warming intervals and batch sizes
- Statistical tracking of key access rates
- Support for different rate intervals (1, 5, or 15 minutes)
- Customizable error handling and monitoring callbacks
- Generic implementation supporting any comparable key type and any value type
- Thread-safe operations

## Requirements

- Go 1.18 or higher (required for generics support)

## Installation

```bash
go get github.com/n-r-w/warmer
```

## Quick Start

Here's a simple example of how to use the warmer package:

```go
package main

import (
    "context"
    "time"
    "github.com/n-r-w/warmer"
)

func main() {
    // Create warmer configuration
    config := warmer.Config[string]{
        Interval:     time.Second * 5,     // Warm cache every 5 seconds
        BatchSize:    100,                 // Process 100 keys per batch
        MaxStatsSize: 1000,               // Keep stats for 1000 keys
        RateInterval: warmer.RateInterval15, // Use 15-minute rate interval
    }

    // Create warmer instance
    w := warmer.New(
        // Function to get data from DB
        func(ctx context.Context, key string) (string, error) {
            return getFromDB(key)
        },
        // Function to set data in cache
        func(ctx context.Context, key string, value string) error {
            return setToCache(key, value)
        },
        config,
    )

    // Start the warmer
    ctx := context.Background()
    w.Start(ctx)

    // Track key updates in your application
    w.TrackUpdate(ctx, "key1", 1)

    // Stop the warmer when done
    w.Stop()
}
```

## Configuration

The `Config` struct provides the following configuration options:

| Parameter | Description | Default |
|-----------|-------------|---------|
| Interval | Duration between warming cycles | 1 minute |
| BatchSize | Number of keys to warm up in one cycle | MaxStatsSize/10 |
| MaxStatsSize | Maximum number of keys to track | 100000 |
| RateInterval | Update rate interval (1, 5, or 15 minutes) | 15 minutes |
| OnKeyTrack | Callback for key access tracking | nil |
| OnDBError | Callback for database read errors | nil |
| OnCacheError | Callback for cache write errors | nil |
| OnWarmTrack | Callback for successful key warming | nil |

Note: The default `BatchSize` is automatically calculated as one-tenth of `MaxStatsSize`. For example, with the default `MaxStatsSize` of 100000, the default `BatchSize` would be 10000.

## Callbacks

The warmer package provides several callback functions for monitoring and debugging:

```go
config := warmer.Config[string]{
    OnWarmTrack: func(ctx context.Context, key string, rate float64) {
        log.Printf("Key warmed: %s, rate: %.2f", key, rate)
    },
    OnKeyTrack: func(ctx context.Context, key string, rate float64) {
        log.Printf("Key tracked: %s, rate: %.2f", key, rate)
    },
    OnDBError: func(ctx context.Context, key string, err error) {
        log.Printf("DB error for key %s: %v", key, err)
    },
    OnCacheError: func(ctx context.Context, key string, err error) {
        log.Printf("Cache error for key %s: %v", key, err)
    },
}
```

## How It Works

1. The warmer tracks key access patterns through the `TrackUpdate` method
2. It maintains statistics about key access rates using meters
3. Periodically (based on the configured interval), it selects the most frequently accessed keys using Roulette Wheel Selection algorithm:
   - Each key's probability of being selected is proportional to its access rate
   - This probabilistic approach ensures diversity in cache warming while favoring frequently accessed keys
4. These keys are then proactively loaded from the database and stored in the cache
5. The process continues until the warmer is stopped

## Thread Safety

The warmer package is thread-safe and can be safely used from multiple goroutines.

## License

This project is released under the [MIT License](LICENSE).
