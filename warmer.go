package warmer

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

// defaultBatchToMaxMultiplier ratio between the size of the batch and the maximum size of the statistics.
const defaultBatchToMaxMultiplier = 10

const (
	// DefaultMaxStatsSize defines the default maximum size of statistics.
	DefaultMaxStatsSize = 100000
	// DefaultBatchSize defines the default size of the key batch for warming.
	DefaultBatchSize = DefaultMaxStatsSize / defaultBatchToMaxMultiplier
	// DefaultRateInterval defines the default update rate interval.
	DefaultRateInterval = RateInterval15
)

// RateIntervalType defines the update rate interval.
type RateIntervalType int

const (
	RateInterval1  RateIntervalType = iota // RateInterval1 1 minute
	RateInterval5                          // RateInterval5 5 minutes
	RateInterval15                         // RateInterval15 15 minutes
)

// GetFromDBFunc defines a function for retrieving an object from the database.
type GetFromDBFunc[K comparable, V any] func(ctx context.Context, key K) (V, error)

// SetToCacheFunc defines a function for saving an object to the cache.
type SetToCacheFunc[K comparable, V any] func(ctx context.Context, key K, value V) error

// OnErrorFunc defines a function for handling errors.
type OnErrorFunc[K comparable] func(ctx context.Context, key K, err error)

// OnKeyTrackFunc defines a function for handling key tracking.
type OnKeyTrackFunc[K comparable] func(ctx context.Context, key K, rate float64)

// OnKeyWarmFunc defines a function for handling key tracking.
type OnKeyWarmFunc[K comparable] func(ctx context.Context, key K, rate float64)

// statsData stores update statistics for a key.
type statsData[K comparable] struct {
	key   K
	meter metrics.Meter
	rate  float64 // temporary value for warming
}

// newStatsData creates a new statsData with initialized meter.
func newStatsData[K comparable](key K) *statsData[K] {
	return &statsData[K]{
		key:   key,
		meter: metrics.NewMeter(),
	}
}

// getUpdateRate returns the current update rate.
func (s *statsData[K]) getUpdateRate(interval RateIntervalType) float64 {
	if interval == RateInterval1 {
		return s.meter.Rate1()
	} else if interval == RateInterval5 {
		return s.meter.Rate5()
	}
	return s.meter.Rate15()
}

// addUpdate registers a new update.
func (s *statsData[K]) addUpdate(count int64) {
	s.meter.Mark(count)
}

// Config contains Warmer settings.
type Config[K comparable] struct {
	// Interval between warming cycles.
	Interval time.Duration

	// Number of keys to warm up in one cycle.
	// If 0, the default value is used (MaxStatsSize/10).
	BatchSize int

	// MaxStatsSize defines the maximum number of records in statistics.
	// When this value is exceeded, old records will be removed.
	// If 0, the default value is used (100000).
	MaxStatsSize int

	// RateInterval defines the update rate interval.
	// If 0, the default value is used (15 minutes).
	RateInterval RateIntervalType

	// Callback for collecting key statistics.
	// Called on each key track.
	OnKeyTrack OnKeyTrackFunc[K]

	// Callback for collecting database read error statistics.
	// Called on each read error.
	OnDBError OnErrorFunc[K]

	// Callback for collecting cache write error statistics.
	// Called on each write error.
	OnCacheError OnErrorFunc[K]

	// OnWarmTrack is called when a key is warmed up.
	OnWarmTrack OnKeyWarmFunc[K]
}

// DefaultConfig returns the default configuration.
func DefaultConfig[K comparable]() Config[K] {
	return Config[K]{
		Interval:     time.Minute,
		BatchSize:    DefaultBatchSize,
		MaxStatsSize: DefaultMaxStatsSize,
		RateInterval: DefaultRateInterval,
	}
}

// Warmer provides cache warming functionality.
type Warmer[K comparable, V any] struct {
	// Data
	stats map[K]*statsData[K]
	mu    sync.Mutex

	// Configuration
	getFromDB    GetFromDBFunc[K, V]
	setToCache   SetToCacheFunc[K, V]
	interval     time.Duration
	batchSize    int
	maxStatsSize int
	rateInterval RateIntervalType

	// State
	running    int32
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// Callbacks
	onKeyTrack   OnKeyTrackFunc[K]
	onDBError    OnErrorFunc[K]
	onCacheError OnErrorFunc[K]
	onWarmTrack  OnKeyWarmFunc[K]

	// Pools for slice reuse
	statsPool sync.Pool
}

// New creates a new warmer instance with specified database and cache access functions.
func New[K comparable, V any](
	getFromDB GetFromDBFunc[K, V], setToCache SetToCacheFunc[K, V], config Config[K],
) *Warmer[K, V] {
	// Check and correct configuration values
	if setToCache == nil {
		panic("setToCache is required")
	}
	if getFromDB == nil {
		panic("getFromDB is required")
	}

	defaultConfig := DefaultConfig[K]()

	if config.Interval <= 0 {
		config.Interval = defaultConfig.Interval
	}
	if config.MaxStatsSize <= 0 {
		config.MaxStatsSize = defaultConfig.MaxStatsSize
	}
	if config.BatchSize <= 0 {
		config.BatchSize = config.MaxStatsSize / defaultBatchToMaxMultiplier
	}
	if config.RateInterval == 0 {
		config.RateInterval = defaultConfig.RateInterval
	}

	return &Warmer[K, V]{
		getFromDB:    getFromDB,
		setToCache:   setToCache,
		stats:        make(map[K]*statsData[K]),
		interval:     config.Interval,
		batchSize:    config.BatchSize,
		maxStatsSize: config.MaxStatsSize,
		rateInterval: config.RateInterval,
		onKeyTrack:   config.OnKeyTrack,
		onDBError:    config.OnDBError,
		onCacheError: config.OnCacheError,
		onWarmTrack:  config.OnWarmTrack,
		statsPool: sync.Pool{
			New: func() any {
				s := make([]*statsData[K], 0, config.BatchSize)
				return &s
			},
		},
	}
}

// TrackUpdate registers a key update.
// count - number of updates
func (w *Warmer[K, V]) TrackUpdate(ctx context.Context, key K, count int64) {
	var rate float64

	w.mu.Lock()
	if stat, exists := w.stats[key]; exists {
		// if key exists
		stat.addUpdate(count)
		rate = stat.getUpdateRate(w.rateInterval)
	} else {
		// if key doesn't exist
		w.cleanupOldStats()
		stat = newStatsData(key)
		rate = stat.getUpdateRate(w.rateInterval)
		w.stats[key] = stat
	}
	w.mu.Unlock()

	if w.onKeyTrack != nil {
		w.onKeyTrack(ctx, key, rate)
	}
}

// cleanupOldStats removes records with the lowest number of updates,
// keeping only MaxStatsSize most frequently updated records.
// executed inside mutex lock.
func (w *Warmer[K, V]) cleanupOldStats() {
	if len(w.stats) == 0 || len(w.stats) < w.maxStatsSize {
		return
	}

	var (
		minRateKey K
		minRate    float64
		zero       K
	)

	for key, stat := range w.stats {
		rate := stat.getUpdateRate(w.rateInterval)
		if (rate < minRate || minRateKey == zero) && rate > 0 {
			minRate = rate
			minRateKey = key
		}
	}

	delete(w.stats, minRateKey)
}

// Start starts the cache warming process.
func (w *Warmer[K, V]) Start(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&w.running, 0, 1) {
		return
	}

	ctxWorker, cancelFunc := context.WithCancel(ctx)
	w.cancelFunc = cancelFunc

	w.wg.Add(1)
	go w.warmerWorker(ctxWorker)
}

func (w *Warmer[K, V]) warmerWorker(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer func() {
		ticker.Stop()
		w.wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			atomic.StoreInt32(&w.running, 0)
			return
		case <-ticker.C:
			w.warmBatch(ctx)
		}
	}
}

// Stop stops the cache warming process.
func (w *Warmer[K, V]) Stop() {
	if !atomic.CompareAndSwapInt32(&w.running, 1, 0) {
		return
	}

	w.cancelFunc()
	w.wg.Wait()
}

// warmBatch performs warming of the most frequently updated keys batch.
func (w *Warmer[K, V]) warmBatch(ctx context.Context) { //nolint:gocognit // no need
	w.mu.Lock()
	if len(w.stats) == 0 {
		w.mu.Unlock()
		return
	}

	// Get slices from pools
	statsPtr, _ := w.statsPool.Get().(*[]*statsData[K])
	stats := (*statsPtr)[:0]

	defer func() {
		// Return slices to pools
		*statsPtr = stats
		w.statsPool.Put(statsPtr)
	}()

	// Find minimal update rate

	const (
		// set minimal rate to x3 the smallest non-zero, because it will be divided by 2 for zero rates
		minRateLimit = math.SmallestNonzeroFloat64 * 3.0
	)

	// First pass: normalize rates and find minimum
	var minRate float64
	for i, stat := range w.stats {
		rate := stat.getUpdateRate(w.rateInterval)
		if rate > math.SmallestNonzeroFloat64 && rate < minRateLimit {
			// for too small non zero rates
			rate = minRateLimit
		}
		w.stats[i].rate = rate // save current rate

		if rate > math.SmallestNonzeroFloat64 && // non zero
			(minRate < math.SmallestNonzeroFloat64 || rate < minRate) { // minimal
			minRate = rate
		}
	}

	// in case of all zero rates
	if minRate < math.SmallestNonzeroFloat64 {
		minRate = minRateLimit
	}

	// Second pass: clone stats and calculate total rate
	var totalRate float64
	for _, stat := range w.stats {
		if stat.rate < minRate { // for zero or very small rate
			//nolint:mnd // If rate is zero (no stats yet), set it to half of the minimal rate.
			// change original stat object, but it's ok because no other goroutines will use it
			stat.rate = minRate / 2.0
		}

		stats = append(stats, stat)
		totalRate += stat.rate
	}
	w.mu.Unlock()

	selected := make(map[K]*statsData[K], w.batchSize)

	// Select keys based on probability. Uses "roulette wheel" selection
	remainingRate := totalRate

	for len(selected) < w.batchSize && len(stats) > 0 && remainingRate >= minRateLimit {
		// Generate random threshold from remaining total rate
		threshold := rand.Float64() * remainingRate //nolint:gosec // not a security issue

		// Select key based on cumulative probability
		var cumulative float64
		selectedIdx := -1

		for i, stat := range stats {
			cumulative += stat.rate
			if cumulative >= threshold {
				selected[stat.key] = stat
				selectedIdx = i
				break
			}
		}

		if selectedIdx >= 0 {
			// Update remaining keys and total rate
			remainingRate -= stats[selectedIdx].rate
			stats = append(stats[:selectedIdx], stats[selectedIdx+1:]...)
		}
	}

	// Warm up the selected keys
	for key, stat := range selected {
		// stat can be removed from w.stats in this moment, but it's ok

		select {
		case <-ctx.Done():
			return
		default:
			if w.onWarmTrack != nil {
				w.onWarmTrack(ctx, key, stat.rate)
			}

			updateOnError := func() {
				// need to increase the probability of the next warming of this key
				stat.addUpdate(1) // thread safe
			}

			value, err := w.getFromDB(ctx, key)
			if err != nil {
				if w.onDBError != nil {
					w.onDBError(ctx, key, err)
				}
				updateOnError()
				continue
			}

			if err = w.setToCache(ctx, key, value); err != nil {
				if w.onCacheError != nil {
					w.onCacheError(ctx, key, err)
				}
				updateOnError()
			}
		}
	}
}
