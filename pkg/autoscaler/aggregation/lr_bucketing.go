package aggregation

import (
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	lr = 0.01
)

func init() {
	lrEnv := os.Getenv("LR")
	if lrEnv != "" {
		if lrValue, err := strconv.ParseFloat(lrEnv, 64); err == nil {
			lr = lrValue
		}
	}
}

type (
	LinearRegressBuckets struct {
		bucketsMutex sync.RWMutex
		// buckets is a ring buffer indexed by timeToIndex() % len(buckets).
		// Each element represents a certain granularity of time, and the total
		// represented duration adds up to a window length of time.
		buckets []float64

		weights []float64

		// firstWrite holds the time when the first write has been made.
		// This time is reset to `now` when the very first write happens,
		// or when a first write happens after `window` time of inactivity.
		// The difference between `now` and `firstWrite` is used to compute
		// the number of eligible buckets for computation of average values.
		firstWrite time.Time

		// lastWrite stores the time when the last write was made.
		// This is used to detect when we have gaps in the data (i.e. more than a
		// granularity has expired since the last write) so that we can zero those
		// entries in the buckets array. It is also used when calculating the
		// WindowAverage to know how much of the buckets array represents valid data.
		lastWrite time.Time

		// granularity is the duration represented by each bucket in the buckets ring buffer.
		granularity time.Duration
		// window is the total time represented by the buckets ring buffer.
		window time.Duration
	}
)

// NewLinearRegressBuckets generates a new LinearRegressBuckets with the given
// granularity.
func NewLinearRegressBuckets(window, granularity time.Duration) *LinearRegressBuckets {
	// Number of buckets is `window` divided by `granularity`, rounded up.
	// e.g. 60s / 2s = 30.
	nb := math.Ceil(float64(window) / float64(granularity))
	weight := make([]float64, int(nb))
	for i := 0; i < int(nb); i++ {
		weight[i] = 1.0 / nb
	}
	return &LinearRegressBuckets{
		buckets:     make([]float64, int(nb)),
		weights:     weight,
		granularity: granularity,
		window:      window,
	}
}

func (b *LinearRegressBuckets) Record(now time.Time, value float64) {
	bucketTime := now.Truncate(b.granularity)

	predValue := math.Max(0., math.Ceil(b.WindowAverage(now)))
	b.bucketsMutex.Lock()
	defer b.bucketsMutex.Unlock()

	// Update the weights
	if !b.isEmptyLocked(now) {
		totalB := len(b.buckets)
		numB := len(b.buckets)

		// Skip invalid buckets
		numZ := 0
		if now.After(b.lastWrite) {
			numZ = int(now.Sub(b.lastWrite) / b.granularity)
			// Reduce effective number of buckets
			numB -= numZ
		}
		diff := predValue - value
		startIdx := b.timeToIndex(b.lastWrite) + totalB // To ensure always positive % operation.
		for i := 0; i < numB; i++ {
			effectiveIdx := (startIdx - i) % totalB
			b.weights[i] -= lr * diff * b.buckets[effectiveIdx] // Update rule
		}
	}

	writeIdx := b.timeToIndex(now)

	// Record new value
	if b.lastWrite != bucketTime {
		if bucketTime.Add(b.window).After(b.lastWrite) {
			// If it is the first write or it happened before the first write which we
			// have in record, update the firstWrite.
			if b.firstWrite.IsZero() || b.firstWrite.After(bucketTime) {
				b.firstWrite = bucketTime
			}

			if bucketTime.After(b.lastWrite) {
				if bucketTime.Sub(b.lastWrite) >= b.window {
					// This means we had no writes for the duration of `window`. So reset the firstWrite time.
					b.firstWrite = bucketTime
					// Reset all the buckets.
					for i := range b.buckets {
						b.buckets[i] = 0
					}
				} else {
					// In theory we might lose buckets between stats gathering.
					// Thus we need to clean not only the current index, but also
					// all the ones from the last write. This is slower than the loop above
					// due to possible wrap-around, so they are not merged together.
					for i := b.timeToIndex(b.lastWrite) + 1; i <= writeIdx; i++ {
						idx := i % len(b.buckets)
						b.buckets[idx] = 0
					}
				}
				// Update the last write time.
				b.lastWrite = bucketTime
			}
			// The else case is b.lastWrite - b.window < bucketTime < b.lastWrite, we can simply add
			// the value to the bucket.
		} else {
			// Ignore this value because it happened a window size ago.
			return
		}
	}
	b.buckets[writeIdx%len(b.buckets)] += value
}

func (b *LinearRegressBuckets) ResizeWindow(w time.Duration) {
	// Same window size, bail out.
	sameWindow := func() bool {
		b.bucketsMutex.RLock()
		defer b.bucketsMutex.RUnlock()
		return w == b.window
	}()
	if sameWindow {
		return
	}

	numBuckets := int(math.Ceil(float64(w) / float64(b.granularity)))
	newBuckets := make([]float64, numBuckets)
	newWeights := make([]float64, numBuckets)

	// We need write lock here.
	// So that we can copy the existing buckets into the new array.
	b.bucketsMutex.Lock()
	defer b.bucketsMutex.Unlock()
	// If we had written any data within `window` time, then exercise the O(N)
	// copy algorithm. Otherwise, just assign zeroes.
	if time.Now().Truncate(b.granularity).Sub(b.lastWrite) <= b.window {
		// If the window is shrinking, then we need to copy only
		// `newBuckets` buckets.
		oldNumBuckets := len(b.buckets)
		tIdx := b.timeToIndex(b.lastWrite)
		for i := 0; i < min(numBuckets, oldNumBuckets); i++ {
			oi := tIdx % oldNumBuckets
			ni := tIdx % numBuckets
			newBuckets[ni] = b.buckets[oi]
			tIdx--

			newWeights[i] = b.weights[i]
		}
		// We can reset this as well to the earliest well known time when we might have
		// written data, if it is
		b.firstWrite = b.lastWrite.Add(-time.Duration(oldNumBuckets-1) * b.granularity)
	} else {
		// No valid data so far, so reset to initial value.
		b.firstWrite = time.Time{}

	}
	b.window = w
	b.buckets = newBuckets
	b.weights = newWeights
}

// Misnomer, but needs to be this way to be consistent with the interface
// Does the weighted sum <=> Linear Regression
func (b *LinearRegressBuckets) WindowAverage(now time.Time) float64 {
	now = now.Truncate(b.granularity)
	b.bucketsMutex.RLock()
	defer b.bucketsMutex.RUnlock()
	if b.isEmptyLocked(now) {
		return 0
	}

	totalB := len(b.buckets)
	numB := len(b.buckets)

	// We start with 0es. But we know that we have _some_ data because
	// IsEmpty returned false.
	numZ := 0
	if now.After(b.lastWrite) {
		numZ = int(now.Sub(b.lastWrite) / b.granularity)
		// Reduce effective number of buckets.
		numB -= numZ
	}
	startIdx := b.timeToIndex(b.lastWrite) + totalB // To ensure always positive % operation.
	ret := 0.
	for i := 0; i < numB; i++ {
		effectiveIdx := (startIdx - i) % totalB
		v := b.buckets[effectiveIdx] * b.weights[numZ+i]
		ret += v
	}
	return ret
}

func (b *LinearRegressBuckets) timeToIndex(tm time.Time) int {
	// I don'b think this run in 2038 :-)
	// NB: we need to divide by granularity, since it's a compressing mapping
	// to buckets.
	return int(tm.Unix()) / int(b.granularity.Seconds())
}

// IsEmpty returns true if no data has been recorded for the `window` period.
func (b *LinearRegressBuckets) IsEmpty(now time.Time) bool {
	now = now.Truncate(b.granularity)
	b.bucketsMutex.RLock()
	defer b.bucketsMutex.RUnlock()
	return b.isEmptyLocked(now)
}

// isEmptyLocked expects `now` to be truncated and at least Read Lock held.
func (b *LinearRegressBuckets) isEmptyLocked(now time.Time) bool {
	return now.Sub(b.lastWrite) > b.window
}
