package aggregation

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestLinearRegressBucketsSimple(t *testing.T) {
	trunc1 := time.Now().Truncate(1 * time.Second)
	trunc5 := time.Now().Truncate(5 * time.Second)

	type args struct {
		time  time.Time
		value float64
	}
	tests := []struct {
		name        string
		granularity time.Duration
		stats       []args
		want        map[time.Time]float64
	}{{
		name:        "granularity = 1s",
		granularity: time.Second,
		stats: []args{
			{trunc1, 1.0}, // activator scale from 0.
			{trunc1.Add(100 * time.Millisecond), 10.0}, // from scraping pod/sent by activator.
			{trunc1.Add(1 * time.Second), 1.0},         // next bucket
			{trunc1.Add(3 * time.Second), 1.0},         // nextnextnext bucket
		},
		want: map[time.Time]float64{
			trunc1:                      11.0,
			trunc1.Add(1 * time.Second): 1.0,
			trunc1.Add(3 * time.Second): 1.0,
		},
	}, {
		name:        "granularity = 5s",
		granularity: 5 * time.Second,
		stats: []args{
			{trunc5, 1.0},
			{trunc5.Add(3 * time.Second), 11.0}, // same bucket
			{trunc5.Add(6 * time.Second), 1.0},  // next bucket
		},
		want: map[time.Time]float64{
			trunc5:                      12.0,
			trunc5.Add(5 * time.Second): 1.0,
		},
	}, {
		name:        "empty",
		granularity: time.Second,
		stats:       []args{},
		want:        map[time.Time]float64{},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// New implementation test.
			buckets := NewLinearRegressBuckets(2*time.Minute, tt.granularity)
			if !buckets.IsEmpty(trunc1) {
				t.Error("Unexpected non empty result")
			}
			for _, stat := range tt.stats {
				buckets.Record(stat.time, stat.value)
			}

			got := make(map[time.Time]float64)
			// Less time in future than our window is (2mins above), but more than any of the tests report.
			buckets.forEachBucket(trunc1.Add(time.Minute), func(t time.Time, b float64) {
				// Since we're storing 0s when there's no data, we need to exclude those
				// for this test.
				if b > 0 {
					got[t] = b
				}
			})

			if !cmp.Equal(tt.want, got) {
				t.Error("Unexpected values (-want +got):", cmp.Diff(tt.want, got))
			}
		})
	}
}

func TestLinearRegressBucketsWeights(t *testing.T) {
	trunc1 := time.Now().Truncate(1 * time.Second)
	trunc5 := time.Now().Truncate(5 * time.Second)

	type args struct {
		time  time.Time
		value float64
	}
	tests := []struct {
		name        string
		granularity time.Duration
		stats       []args
		want        map[time.Time]float64
	}{{
		name:        "granularity = 1s",
		granularity: time.Second,
		stats: []args{
			{trunc1, 1.0}, // activator scale from 0.
			{trunc1.Add(100 * time.Millisecond), 10.0}, // from scraping pod/sent by activator.
			{trunc1.Add(1 * time.Second), 1.0},         // next bucket
			{trunc1.Add(3 * time.Second), 1.0},         // nextnextnext bucket
		},
		want: map[time.Time]float64{
			trunc1:                      11.0,
			trunc1.Add(1 * time.Second): 1.0,
			trunc1.Add(3 * time.Second): 1.0,
		},
	}, {
		name:        "granularity = 5s",
		granularity: 5 * time.Second,
		stats: []args{
			{trunc5, 1.0},
			{trunc5.Add(3 * time.Second), 11.0}, // same bucket
			{trunc5.Add(6 * time.Second), 1.0},  // next bucket
		},
		want: map[time.Time]float64{
			trunc5:                      12.0,
			trunc5.Add(5 * time.Second): 1.0,
		},
	}, {
		name:        "empty",
		granularity: time.Second,
		stats:       []args{},
		want:        map[time.Time]float64{},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// New implementation test.
			buckets := NewLinearRegressBuckets(2*time.Minute, tt.granularity)
			if !buckets.IsEmpty(trunc1) {
				t.Error("Unexpected non empty result")
			}
			for _, stat := range tt.stats {
				buckets.Record(stat.time, stat.value)
			}

			got := make(map[int]float64)
			// Less time in future than our window is (2mins above), but more than any of the tests report.
			buckets.forEachWeight(func(i int, b float64) {
				// Since we're storing 0s when there's no data, we need to exclude those
				// for this test.
				if b > 0 {
					got[i] = b
				}
			})

			if !cmp.Equal(tt.want, got) {
				t.Error("Unexpected values (-want +got):", cmp.Diff(tt.want, got))
			}
		})
	}
}

func (t *LinearRegressBuckets) forEachBucket(now time.Time, acc func(time time.Time, bucket float64)) {
	now = now.Truncate(t.granularity)
	t.bucketsMutex.RLock()
	defer t.bucketsMutex.RUnlock()

	// So number of buckets we can process is len(buckets)-(now-lastWrite)/granularity.
	// Since empty check above failed, we know this is at least 1 bucket.
	numBuckets := len(t.buckets) - int(now.Sub(t.lastWrite)/t.granularity)
	bucketTime := t.lastWrite // Always aligned with granularity.
	si := t.timeToIndex(bucketTime)
	for i := 0; i < numBuckets; i++ {
		tIdx := si % len(t.buckets)
		acc(bucketTime, t.buckets[tIdx])
		si--
		bucketTime = bucketTime.Add(-t.granularity)
	}
}

func (t *LinearRegressBuckets) forEachWeight(acc func(i int, weight float64)) {
	t.bucketsMutex.RLock()
	defer t.bucketsMutex.RUnlock()

	for i, w := range t.weights {
		acc(i, w)
	}
}
