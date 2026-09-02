/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package net

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	pkgnet "knative.dev/networking/pkg/apis/networking"
	netcfg "knative.dev/networking/pkg/config"
	endpointsinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/logging/logkey"
	"knative.dev/pkg/reconciler"
	"knative.dev/serving/pkg/activator/handler"
	"knative.dev/serving/pkg/apis/serving"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	revisioninformer "knative.dev/serving/pkg/client/injection/informers/serving/v1/revision"
	servinglisters "knative.dev/serving/pkg/client/listers/serving/v1"
	"knative.dev/serving/pkg/networking"
	"knative.dev/serving/pkg/queue"
)

const (
	// The number of requests that are queued on the breaker before the 503s are sent.
	// The value must be adjusted depending on the actual production requirements.
	// This value is used both for the breaker in revisionThrottler (throttling
	// across the entire revision), and for the individual podTracker breakers.
	breakerQueueDepth = 10000

	// The revisionThrottler breaker's concurrency increases up to this value as
	// new endpoints show up. We need to set some value here since the breaker
	// requires an explicit buffer size (it's backed by a chan struct{}), but
	// queue.MaxBreakerCapacity is math.MaxInt32.
	revisionMaxConcurrency = queue.MaxBreakerCapacity
)

func newPodTracker(dest string, b breaker) *podTracker {
	tracker := &podTracker{
		dest: dest,
		b:    b,
	}
	tracker.decreaseWeight = func() { tracker.weight.Add(-1) }

	return tracker
}

type podTracker struct {
	dest string
	b    breaker

	// weight is used for LB policy implementations.
	weight atomic.Int32
	// decreaseWeight is an allocation optimization for the randomChoice2 policy.
	decreaseWeight func()
}

func (p *podTracker) increaseWeight() {
	p.weight.Add(1)
}

func (p *podTracker) getWeight() int32 {
	return p.weight.Load()
}

func (p *podTracker) String() string {
	if p == nil {
		return "<nil>"
	}
	return p.dest
}

func (p *podTracker) Capacity() int {
	if p.b == nil {
		return 1
	}
	return p.b.Capacity()
}

func (p *podTracker) UpdateConcurrency(c int) {
	if p.b == nil {
		return
	}
	p.b.UpdateConcurrency(c)
}

func (p *podTracker) Reserve(ctx context.Context) (func(), bool) {
	if p.b == nil {
		return noop, true
	}
	return p.b.Reserve(ctx)
}

type breaker interface {
	Capacity() int
	Maybe(ctx context.Context, thunk func()) error
	UpdateConcurrency(int)
	Reserve(ctx context.Context) (func(), bool)
}

// nodeTracker tracks live in-flight-request accounting for one worker node,
// used only as a fallback dispatch target when a revision's own breaker and
// pod trackers have no capacity. It is shared by pointer across every
// revisionThrottler, since a node's CPU is a real, physical shared resource.
type nodeTracker struct {
	ip       string
	limit    int32        // floor(cores * NODE_CPU_SHARE), computed once at startup
	inFlight atomic.Int32 // live reservation count
}

// reserve attempts to atomically claim one slot on this node. Returns false
// (and leaves inFlight unchanged) if the node has no spare quota.
func (n *nodeTracker) reserve() bool {
	if n.limit <= 0 {
		return false
	}
	if n.inFlight.Add(1) > n.limit {
		n.inFlight.Add(-1)
		return false
	}
	return true
}

func (n *nodeTracker) release() {
	n.inFlight.Add(-1)
}

func (n *nodeTracker) String() string {
	return fmt.Sprintf("%s(limit=%d)", n.ip, n.limit)
}

// nodePool is a shared, process-wide round-robin pool over worker nodes,
// used by every revisionThrottler as a fallback dispatch target once a
// revision has no breaker capacity left.
type nodePool struct {
	nodes []*nodeTracker
	next  atomic.Int32 // rotating start offset, shared across all revisions
}

func newNodePool(nodes []*nodeTracker) *nodePool {
	return &nodePool{nodes: nodes}
}

// tryReserve scans the node ring at most once around, starting at a
// rotating offset, and atomically reserves the first node with spare quota.
func (np *nodePool) tryReserve() (*nodeTracker, bool) {
	n := len(np.nodes)
	if n == 0 {
		return nil, false
	}
	// uint32 cast avoids a negative % result once next wraps past MaxInt32.
	start := int(uint32(np.next.Inc())) % n
	for i := 0; i < n; i++ {
		if nt := np.nodes[(start+i)%n]; nt.reserve() {
			return nt, true
		}
	}
	return nil, false
}

// waiterEntry represents one request blocked in revisionThrottler.queue,
// waiting either for a specific new podTracker (direct hand-off) or a
// clusterIP capacity broadcast (nil payload). taken is CAS-guarded to
// resolve the race between a producer committing a value to ch and the
// waiter giving up via ctx cancellation at the same instant.
type waiterEntry struct {
	ch    chan *podTracker // buffered size 1
	taken atomic.Bool
}

// revisionThrottler is used to throttle requests across the entire revision.
// We use a breaker across the entire revision as well as individual
// podTrackers because we need to queue requests in case no individual
// podTracker has available slots (when CC!=0).
type revisionThrottler struct {
	revID                types.NamespacedName
	containerConcurrency int
	lbPolicy             lbPolicy

	// These are used in slicing to infer which pods to assign
	// to this activator.
	numActivators atomic.Int32
	// If -1, it is presumed that this activator should not receive requests
	// for the revision. But due to the system being distributed it might take
	// time for everything to propagate. Thus when this is -1 we assign all the
	// pod trackers.
	activatorIndex atomic.Int32
	protocol       string

	// Holds the current number of backends. This is used for when we get an activatorCount update and
	// therefore need to recalculate capacity
	backendCount int

	// This is a breaker for the revision as a whole.
	breaker breaker

	// This will be non-empty when we're able to use pod addressing.
	podTrackers []*podTracker

	// Effective trackers that are assigned to this Activator.
	// This is a subset of podTrackers.
	assignedTrackers []*podTracker

	// If we don't have a healthy clusterIPTracker this is set to nil, otherwise
	// it is the l4dest for this revision's private clusterIP.
	clusterIPTracker *podTracker

	// mux guards the "throttler state" which is the state we use during the
	// request path. This is: trackers, clusterIPDest.
	mux sync.RWMutex

	cr *handler.ConcurrencyReporter

	logger *zap.SugaredLogger

	// Shared, process-wide pool of worker nodes used as a fallback dispatch
	// target once the revision breaker has no capacity. This is the same
	// *nodePool pointer for every revisionThrottler.
	nodePool *nodePool

	// claimedTrackers, toDelete and queue implement the wait-for-a-new-
	// instance fallback (see try/wait/reconcileClaimed below). They are
	// in-memory, per-activator-process state: correct only when a single
	// activator replica serves this revision, since a pod discovered by one
	// activator's endpoint watch may not be the one a request queued on a
	// different activator is waiting for.

	// Trackers that have been handed directly to a waiting request via
	// queue but not yet returned through insertTracker. Excluded from
	// assignedTrackers/capacity accounting while claimed. Guarded by mux.
	claimedTrackers []*podTracker

	// Trackers that must NOT be folded back into assignedTrackers once
	// their claim completes, because their dest disappeared from a later
	// backend update while still claimed by a waiter. Guarded by mux.
	toDelete []*podTracker

	// FIFO-ish queue (via non-blocking receive) of requests waiting for a
	// new pod tracker (direct hand-off) or a clusterIP capacity broadcast,
	// once both the revision breaker and the node fallback pool are
	// exhausted.
	queue chan *waiterEntry
}

func newRevisionThrottler(revID types.NamespacedName,
	containerConcurrency int, proto string,
	breakerParams queue.BreakerParams,
	logger *zap.SugaredLogger,
	cr *handler.ConcurrencyReporter,
	nodePool *nodePool) *revisionThrottler {
	logger = logger.With(zap.String(logkey.Key, revID.String()))
	var (
		revBreaker breaker
		lbp        lbPolicy
	)
	switch {
	case containerConcurrency == 0:
		revBreaker = newInfiniteBreaker(logger)
		lbp = randomChoice2Policy
	case containerConcurrency <= 3:
		// For very low CC values use first available pod.
		revBreaker = queue.NewBreaker(breakerParams)
		lbp = newRoundRobinPolicy()
	default:
		// Otherwise RR.
		revBreaker = queue.NewBreaker(breakerParams)
		lbp = newRoundRobinPolicy()
	}
	return &revisionThrottler{
		revID:                revID,
		containerConcurrency: containerConcurrency,
		breaker:              revBreaker,
		logger:               logger,
		protocol:             proto,
		activatorIndex:       *atomic.NewInt32(-1), // Start with unknown.
		lbPolicy:             lbp,
		cr:                   cr,
		nodePool:             nodePool,
		queue:                make(chan *waiterEntry, breakerQueueDepth),
	}
}

func noop() {}

// Returns a dest that at the moment of choosing had an open slot
// for request.
func (rt *revisionThrottler) acquireDest(ctx context.Context) (func(), *podTracker) {
	rt.mux.RLock()
	defer rt.mux.RUnlock()

	if rt.clusterIPTracker != nil {
		return noop, rt.clusterIPTracker
	}
	return rt.lbPolicy(ctx, rt.assignedTrackers)
}

func (rt *revisionThrottler) try(ctx context.Context, function func(string) error) error {
	// Retrying infinitely as long as we receive no dest. Outer semaphore and inner
	// pod capacity are not changed atomically, hence they can race each other. We
	// "reenqueue" requests should that happen.
	if release, err := rt.breaker.Reserve(ctx); err {
		defer release()
		cb, tracker := rt.acquireDest(ctx)
		if tracker == nil {
			// This can happen if individual requests raced each other or if pod
			// capacity was decreased after passing the outer semaphore.
			rt.logger.Fatalf("No tracker available for revision %s", rt.revID)
			return nil
		}
		defer cb()
		// We already reserved a guaranteed spot. So just execute the passed functor.
		return function(tracker.dest)
	}

	rt.logger.Debugf("Triggering creation of new instance for %s", rt.revID)
	// We didn't manage to reserve a spot. Kick off the creation in background.
	rt.cr.Poke()

	// Local expansion: dispatch directly to a worker node's relay, bounded
	// by that node's CPU-share quota.
	if nt, ok := rt.nodePool.tryReserve(); ok {
		defer nt.release()
		return function(nt.ip + ":8080")
	}

	// No node has spare quota either. Wait for a genuine new pod instance.
	rt.logger.Debugf("No node capacity available, waiting for new instance for %s", rt.revID)
	return rt.wait(ctx, function)
}

// wait blocks until a new podTracker is created for this revision (or, in
// clusterIP mode, until capacity is broadcast) and then dispatches to it.
func (rt *revisionThrottler) wait(ctx context.Context, function func(string) error) error {
	w := &waiterEntry{ch: make(chan *podTracker, 1)}
	rt.queue <- w

	select {
	case <-ctx.Done():
		// If a producer already committed a tracker to w concurrently, we
		// must still claim and return it -- otherwise its capacity would be
		// permanently excluded from assignedTrackers (see reconcileClaimed).
		if !w.taken.CompareAndSwap(false, true) {
			if tracker := <-w.ch; tracker != nil {
				rt.insertTracker(tracker)
			}
		}
		return ctx.Err()

	case tracker := <-w.ch:
		if tracker == nil {
			// Broadcast wake (clusterIP capacity appeared): no specific
			// tracker was handed to us, just retry from the top.
			return rt.try(ctx, function)
		}
		defer func() { rt.insertTracker(tracker) }()
		rt.logger.Debugf("Forwarding to the new instance %s", tracker.dest)
		return function(tracker.dest)
	}
}

func (rt *revisionThrottler) calculateCapacity(backendCount, numTrackers, activatorCount int) int {
	targetCapacity := 0
	if numTrackers > 0 {
		// Capacity is computed based off of number of trackers,
		// when using pod direct routing.
		// We use number of assignedTrackers (numTrackers) for calculation
		// since assignedTrackers means activator's capacity
		targetCapacity = rt.containerConcurrency * numTrackers
	} else {
		// Capacity is computed off of number of ready backends,
		// when we are using clusterIP routing.
		targetCapacity = rt.containerConcurrency * backendCount
		if targetCapacity > 0 {
			targetCapacity = minOneOrValue(targetCapacity / minOneOrValue(activatorCount))
		}
	}

	if (backendCount > 0) && (rt.containerConcurrency == 0 || targetCapacity > revisionMaxConcurrency) {
		// If cc==0, we need to pick a number, but it does not matter, since
		// infinite breaker will dole out as many tokens as it can.
		// For cc>0 we clamp targetCapacity to maxConcurrency because the backing
		// breaker requires some limit (it's backed by a chan struct{}), but the
		// limit is math.MaxInt32 so in practice this should never be a real limit.
		targetCapacity = revisionMaxConcurrency
	}

	return targetCapacity
}

// This makes sure we reset the capacity to the CC, since the pod
// might be reassigned to be exclusively used.
func (rt *revisionThrottler) resetTrackers() {
	if rt.containerConcurrency <= 0 {
		return
	}
	for _, t := range rt.podTrackers {
		// Reset to default.
		t.UpdateConcurrency(rt.containerConcurrency)
	}
}

// updateCapacity updates the capacity of the throttler and recomputes
// the assigned trackers to the Activator instance.
// Currently updateCapacity is ensured to be invoked from a single go routine
// and this does not synchronize
func (rt *revisionThrottler) updateCapacity(backendCount int) {
	// We have to make assignments on each updateCapacity, since if number
	// of activators changes, then we need to rebalance the assignedTrackers.
	ac, ai := int(rt.numActivators.Load()), int(rt.activatorIndex.Load())
	numTrackers := func() int {
		// We do not have to process the `podTrackers` under lock, since
		// updateCapacity is guaranteed to be executed by a single goroutine.
		// But `assignedTrackers` is being read by the serving thread, so the
		// actual assignment has to be done under lock.

		// We're using cluster IP.
		if rt.clusterIPTracker != nil {
			if backendCount > 0 {
				// Capacity may now be available; wake any requests that were
				// waiting on the node-quota/wait-queue fallback so they can
				// retry rather than sit blocked until their context expires.
				rt.broadcastWake()
			}
			return 0
		}

		// Sort, so we get more or less stable results.
		sort.Slice(rt.podTrackers, func(i, j int) bool {
			return rt.podTrackers[i].dest < rt.podTrackers[j].dest
		})

		// The actual read of claimedTrackers and write out of the assigned
		// trackers has to be under lock.
		rt.mux.Lock()
		defer rt.mux.Unlock()

		candidates := rt.podTrackers
		if len(rt.claimedTrackers) > 0 {
			// Exclude trackers currently claimed by a waiting request --
			// they aren't available for the normal LB path until the
			// claiming request finishes and insertTracker returns them.
			excluded := make(map[*podTracker]struct{}, len(rt.claimedTrackers))
			for _, t := range rt.claimedTrackers {
				excluded[t] = struct{}{}
			}
			candidates = make([]*podTracker, 0, len(rt.podTrackers))
			for _, t := range rt.podTrackers {
				if _, skip := excluded[t]; !skip {
					candidates = append(candidates, t)
				}
			}
		}

		assigned := candidates
		if rt.containerConcurrency > 0 {
			rt.resetTrackers()
			assigned = assignSlice(candidates, ai, ac, rt.containerConcurrency)
		}
		rt.logger.Debugf("Trackers %d/%d: assignment: %v", ai, ac, assigned)
		rt.assignedTrackers = assigned
		return len(assigned)
	}()

	capacity := rt.calculateCapacity(backendCount, numTrackers, ac)
	rt.logger.Infof("Set capacity to %d (backends: %d, index: %d/%d)",
		capacity, backendCount, ai, ac)

	rt.backendCount = backendCount
	rt.breaker.UpdateConcurrency(capacity)
}

func (rt *revisionThrottler) updateThrottlerState(backendCount int, trackers []*podTracker, clusterIPDest *podTracker) {
	rt.logger.Infof("Updating Revision Throttler with: clusterIP = %v, trackers = %d, backends = %d",
		clusterIPDest, len(trackers), backendCount)

	// Update trackers / clusterIP before capacity. Otherwise we can race updating our breaker when
	// we increase capacity, causing a request to fall through before a tracker is added, causing an
	// incorrect LB decision.
	if func() bool {
		rt.mux.Lock()
		defer rt.mux.Unlock()
		rt.podTrackers = trackers
		rt.clusterIPTracker = clusterIPDest
		return clusterIPDest != nil || len(trackers) > 0
	}() {
		// If we have an address to target, then pass through an accurate
		// accounting of the number of backends.
		rt.updateCapacity(backendCount)
	} else {
		// If we do not have an address to target, then we should treat it
		// as though we have zero backends.
		rt.updateCapacity(0)
	}
}

// reconcileClaimed must be called from handleUpdate (single-threaded) with
// the full new set of dests and the trackers that are genuinely new in this
// update, before updateThrottlerState is called. It:
//  1. hands each newly-added tracker directly to a currently-queued waiter
//     (if any), recording it in claimedTrackers so updateCapacity excludes
//     it from assignedTrackers until insertTracker returns it; and
//  2. marks any currently-claimed tracker whose dest disappeared from this
//     update as toDelete, so insertTracker won't resurrect it later.
func (rt *revisionThrottler) reconcileClaimed(dests sets.Set[string], added []*podTracker) {
	rt.mux.Lock()
	for _, t := range rt.claimedTrackers {
		if !dests.Has(t.dest) {
			rt.toDelete = append(rt.toDelete, t)
		}
	}
	rt.mux.Unlock()

	for _, t := range added {
		for {
			select {
			case w := <-rt.queue:
				if !w.taken.CompareAndSwap(false, true) {
					// Waiter already gave up (ctx cancelled); drop this dead
					// entry and try the next queued waiter for tracker t.
					continue
				}
				w.ch <- t // buffered 1, single writer, never blocks
				rt.mux.Lock()
				rt.claimedTrackers = append(rt.claimedTrackers, t)
				rt.mux.Unlock()
			default:
				// No (more) waiters; leave t for the normal LB path.
			}
			break
		}
	}
}

// broadcastWake drains rt.queue and wakes every currently-queued waiter with
// a nil payload, meaning "retry try() from the top" rather than a specific
// tracker hand-off. Used only for clusterIP mode, where there is a single
// shared dest (not individually exclusive pod trackers), so the exclusive
// hand-off used for direct pod routing doesn't apply.
func (rt *revisionThrottler) broadcastWake() {
	for {
		select {
		case w := <-rt.queue:
			if w.taken.CompareAndSwap(false, true) {
				w.ch <- nil
			}
			// else: dead entry (waiter already cancelled); just drop it.
		default:
			return
		}
	}
}

// insertTracker folds a pod tracker that was claimed directly by a waiting
// request back into the normal pool once that request completes, unless the
// pod has since been removed (toDelete).
func (rt *revisionThrottler) insertTracker(tracker *podTracker) {
	rt.mux.Lock()
	defer rt.mux.Unlock()

	rt.claimedTrackers = removePodTracker(rt.claimedTrackers, tracker)
	if idx := indexOfPodTracker(rt.toDelete, tracker); idx >= 0 {
		rt.toDelete = append(rt.toDelete[:idx], rt.toDelete[idx+1:]...)
		rt.logger.Debugf("Tracker %s was removed while claimed, not reinstating", tracker.dest)
		return
	}
	// Fold the tracker directly into the LB pool under lock so subsequent
	// requests can use it immediately. We deliberately do NOT call the full
	// updateCapacity here: it's documented as safe only when invoked from
	// the single throttler goroutine (it does unlocked sort/reset work),
	// whereas insertTracker runs on arbitrary request goroutines. The
	// revision breaker's overall capacity accounting for this tracker
	// catches up on the next backend update instead.
	rt.assignedTrackers = append(rt.assignedTrackers, tracker)
}

func removePodTracker(trackers []*podTracker, tracker *podTracker) []*podTracker {
	for i, t := range trackers {
		if t == tracker {
			return append(trackers[:i], trackers[i+1:]...)
		}
	}
	return trackers
}

func indexOfPodTracker(trackers []*podTracker, tracker *podTracker) int {
	for i, t := range trackers {
		if t == tracker {
			return i
		}
	}
	return -1
}

// pickIndices picks the indices for the slicing.
func pickIndices(numTrackers, selfIndex, numActivators int) (beginIndex, endIndex, remnants int) {
	if numActivators > numTrackers {
		// 1. We have fewer pods than than activators. Assign the pods in round robin fashion.
		// With subsetting this is less of a problem and should almost never happen.
		// e.g. lt=3, #ac = 5; for selfIdx = 3 => 3 % 3 = 0, or for si = 5 => 5%3 = 2
		beginIndex = selfIndex % numTrackers
		endIndex = beginIndex + 1
		return beginIndex, endIndex, 0
	}

	// 2. distribute equally and share the remnants
	// among all the activators, but with reduced capacity, if finite.
	sliceSize := numTrackers / numActivators
	beginIndex = selfIndex * sliceSize
	endIndex = beginIndex + sliceSize
	remnants = numTrackers % numActivators
	return beginIndex, endIndex, remnants
}

// assignSlice picks a subset of the individual pods to send requests to
// for this Activator instance. This only matters in case of direct
// to pod IP routing, and is irrelevant, when ClusterIP is used.
// assignSlice should receive podTrackers sorted by address.
func assignSlice(trackers []*podTracker, selfIndex, numActivators, cc int) []*podTracker {
	// When we're unassigned, doesn't matter what we return.
	lt := len(trackers)
	if selfIndex == -1 || lt <= 1 {
		return trackers
	}

	// If there's just a single activator. Take all the trackers.
	if numActivators == 1 {
		return trackers
	}

	// If the number of pods is not divisible by the number of activators, we allocate one pod to each activator exclusively.
	// examples
	// 1. we have 20 pods and 3 activators -> we'd get 2 remnants so activator with index 0,1 would each pick up a unique tracker
	// 2. we have 24 pods and 5 activators -> we'd get 4 remnants so the activator 0,1,2,3 would each pick up a unique tracker
	bi, ei, remnants := pickIndices(lt, selfIndex, numActivators)
	x := append(trackers[:0:0], trackers[bi:ei]...)
	if remnants > 0 {
		tail := trackers[len(trackers)-remnants:]
		if len(tail) > selfIndex {
			t := tail[selfIndex]
			x = append(x, t)
		}
	}
	return x
}

// This function will never be called in parallel but `try` can be called in parallel to this so we need
// to lock on updating concurrency / trackers
func (rt *revisionThrottler) handleUpdate(update revisionDestsUpdate) {
	rt.logger.Debugw("Handling update",
		zap.String("ClusterIP", update.ClusterIPDest), zap.Object("dests", logging.StringSet(update.Dests)))

	// ClusterIP is not yet ready, so we want to send requests directly to the pods.
	// NB: this will not be called in parallel, thus we can build a new podTrackers
	// array before taking out a lock.
	if update.ClusterIPDest == "" {
		// Create a map for fast lookup of existing trackers.
		trackersMap := make(map[string]*podTracker, len(rt.podTrackers))
		for _, tracker := range rt.podTrackers {
			trackersMap[tracker.dest] = tracker
		}

		trackers := make([]*podTracker, 0, len(update.Dests))
		added := make([]*podTracker, 0, len(update.Dests))

		// Loop over dests, reuse existing tracker if we have one, otherwise create
		// a new one.
		for newDest := range update.Dests {
			tracker, ok := trackersMap[newDest]
			if !ok {
				if rt.containerConcurrency == 0 {
					tracker = newPodTracker(newDest, nil)
				} else {
					tracker = newPodTracker(newDest, queue.NewBreaker(queue.BreakerParams{
						QueueDepth:      breakerQueueDepth,
						MaxConcurrency:  rt.containerConcurrency,
						InitialCapacity: rt.containerConcurrency, // Presume full unused capacity.
					}))
				}
				added = append(added, tracker)
			}
			trackers = append(trackers, tracker)
		}

		// Hand genuinely-new trackers directly to any requests already
		// waiting for a new instance, before they flow into the normal
		// assignedTrackers/LB pool below.
		rt.reconcileClaimed(update.Dests, added)

		rt.updateThrottlerState(len(update.Dests), trackers, nil /*clusterIP*/)
		return
	}

	rt.updateThrottlerState(len(update.Dests), nil /*trackers*/, newPodTracker(update.ClusterIPDest, nil))
}

// Throttler load balances requests to revisions based on capacity. When `Run` is called it listens for
// updates to revision backends and decides when and when and where to forward a request.
type Throttler struct {
	revisionThrottlers      map[types.NamespacedName]*revisionThrottler
	revisionThrottlersMutex sync.RWMutex
	revisionLister          servinglisters.RevisionLister
	ipAddress               string // The IP address of this activator.
	logger                  *zap.SugaredLogger
	epsUpdateCh             chan *corev1.Endpoints
	cr                      *handler.ConcurrencyReporter

	nodePool *nodePool
}

// NewThrottler creates a new Throttler
func NewThrottler(ctx context.Context, ipAddr string, cr *handler.ConcurrencyReporter) *Throttler {
	revisionInformer := revisioninformer.Get(ctx)

	// NODE_CPU_SHARE is the fraction of each worker node's CPU that may be
	// used for the "local expansion" fallback dispatch in try(). Unset or
	// unparseable values default to 1.0 (roughly the legacy, uncapped
	// behavior: bounded only by each node's full core count).
	cpuShare, err := strconv.ParseFloat(os.Getenv("NODE_CPU_SHARE"), 64)
	if err != nil {
		cpuShare = 1.0
	}

	t := &Throttler{
		revisionThrottlers: make(map[types.NamespacedName]*revisionThrottler),
		revisionLister:     revisionInformer.Lister(),
		ipAddress:          ipAddr,
		logger:             logging.FromContext(ctx),
		epsUpdateCh:        make(chan *corev1.Endpoints),
		cr:                 cr,
		nodePool:           newNodePool(getNodes(ctx, cpuShare)),
	}

	// Watch revisions to create throttler with backlog immediately and delete
	// throttlers on revision delete
	revisionInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    t.revisionUpdated,
		UpdateFunc: controller.PassNew(t.revisionUpdated),
		DeleteFunc: t.revisionDeleted,
	})

	// Watch activator endpoint to maintain activator count
	endpointsInformer := endpointsinformer.Get(ctx)

	// Handles public service updates.
	endpointsInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: reconciler.LabelFilterFunc(networking.ServiceTypeKey,
			string(networking.ServiceTypePublic), false),
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    t.publicEndpointsUpdated,
			UpdateFunc: controller.PassNew(t.publicEndpointsUpdated),
		},
	})
	return t
}

// getNodes lists eligible worker nodes and computes each one's fallback
// dispatch concurrency limit as floor(allocatable cores * cpuShare).
func getNodes(ctx context.Context, cpuShare float64) []*nodeTracker {
	logger := logging.FromContext(ctx)
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Fatalf("Error building in-cluster config: %s\n", err.Error())
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		logger.Fatalf("Error creating clientset: %s\n", err.Error())
	}

	// Get the node list
	nodeList, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logger.Fatalf("Error getting node list: %s\n", err.Error())
	}

	nodes := []*nodeTracker{}
	for _, n := range nodeList.Items {
		if n.Labels["loader-nodetype"] != "worker" && n.Labels["loader-nodetype"] != "singlenode" {
			continue
		}

		var ip string
		for _, addr := range n.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				ip = addr.Address
				break
			}
		}

		cores := n.Status.Allocatable.Cpu().Value()
		limit := int32(float64(cores) * cpuShare)
		nodes = append(nodes, &nodeTracker{ip: ip, limit: limit})
	}

	logger.Infof("Nodes: %v", nodes)
	return nodes
}

// Run starts the throttler and blocks until the context is done.
func (t *Throttler) Run(ctx context.Context, probeTransport http.RoundTripper, usePassthroughLb bool, meshMode netcfg.MeshCompatibilityMode) {
	rbm := newRevisionBackendsManager(ctx, probeTransport, usePassthroughLb, meshMode)
	// Update channel is closed when ctx is done.
	t.run(rbm.updates())
}

func (t *Throttler) run(updateCh <-chan revisionDestsUpdate) {
	for {
		select {
		case update, ok := <-updateCh:
			if !ok {
				t.logger.Info("The Throttler has stopped.")
				return
			}
			t.handleUpdate(update)
		case eps := <-t.epsUpdateCh:
			t.handlePubEpsUpdate(eps)
		}
	}
}

// Try waits for capacity and then executes function, passing in a l4 dest to send a request
func (t *Throttler) Try(ctx context.Context, revID types.NamespacedName, function func(string) error) error {
	rt, err := t.getOrCreateRevisionThrottler(revID)
	if err != nil {
		return err
	}
	return rt.try(ctx, function)
}

func (t *Throttler) getOrCreateRevisionThrottler(revID types.NamespacedName) (*revisionThrottler, error) {
	// First, see if we can succeed with just an RLock. This is in the request path so optimizing
	// for this case is important
	t.revisionThrottlersMutex.RLock()
	revThrottler, ok := t.revisionThrottlers[revID]
	t.revisionThrottlersMutex.RUnlock()
	if ok {
		return revThrottler, nil
	}

	// Redo with a write lock since we failed the first time and may need to create
	t.revisionThrottlersMutex.Lock()
	defer t.revisionThrottlersMutex.Unlock()
	revThrottler, ok = t.revisionThrottlers[revID]
	if !ok {
		rev, err := t.revisionLister.Revisions(revID.Namespace).Get(revID.Name)
		if err != nil {
			return nil, err
		}
		revThrottler = newRevisionThrottler(
			revID,
			int(rev.Spec.GetContainerConcurrency()),
			pkgnet.ServicePortName(rev.GetProtocol()),
			queue.BreakerParams{QueueDepth: breakerQueueDepth, MaxConcurrency: revisionMaxConcurrency},
			t.logger,
			t.cr,
			t.nodePool,
		)
		t.revisionThrottlers[revID] = revThrottler
	}
	return revThrottler, nil
}

// revisionUpdated is used to ensure we have a backlog set up for a revision as soon as it is created
// rather than erroring with revision not found until a networking probe succeeds
func (t *Throttler) revisionUpdated(obj interface{}) {
	rev := obj.(*v1.Revision)
	revID := types.NamespacedName{Namespace: rev.Namespace, Name: rev.Name}

	t.logger.Debug("Revision update", zap.String(logkey.Key, revID.String()))

	if _, err := t.getOrCreateRevisionThrottler(revID); err != nil {
		t.logger.Errorw("Failed to get revision throttler for revision",
			zap.Error(err), zap.String(logkey.Key, revID.String()))
	}
}

// revisionDeleted is to clean up revision throttlers after a revision is deleted to prevent unbounded
// memory growth
func (t *Throttler) revisionDeleted(obj interface{}) {
	acc, err := kmeta.DeletionHandlingAccessor(obj)
	if err != nil {
		t.logger.Warnw("Revision delete failure to process", zap.Error(err))
		return
	}

	revID := types.NamespacedName{Namespace: acc.GetNamespace(), Name: acc.GetName()}

	t.logger.Debugw("Revision delete", zap.String(logkey.Key, revID.String()))

	t.revisionThrottlersMutex.Lock()
	defer t.revisionThrottlersMutex.Unlock()
	delete(t.revisionThrottlers, revID)
}

func (t *Throttler) handleUpdate(update revisionDestsUpdate) {
	if rt, err := t.getOrCreateRevisionThrottler(update.Rev); err != nil {
		if k8serrors.IsNotFound(err) {
			t.logger.Debugw("Revision not found. It was probably removed", zap.String(logkey.Key, update.Rev.String()))
		} else {
			t.logger.Errorw("Failed to get revision throttler", zap.Error(err), zap.String(logkey.Key, update.Rev.String()))
		}
	} else {
		rt.handleUpdate(update)
	}
}

func (t *Throttler) handlePubEpsUpdate(eps *corev1.Endpoints) {
	t.logger.Infof("Public EPS updates: %#v", eps)

	revN := eps.Labels[serving.RevisionLabelKey]
	if revN == "" {
		// Perhaps, we're not the only ones using the same selector label.
		t.logger.Infof("Ignoring update for PublicService %s/%s", eps.Namespace, eps.Name)
		return
	}
	rev := types.NamespacedName{Name: revN, Namespace: eps.Namespace}
	if rt, err := t.getOrCreateRevisionThrottler(rev); err != nil {
		if k8serrors.IsNotFound(err) {
			t.logger.Debugw("Revision not found. It was probably removed", zap.String(logkey.Key, rev.String()))
		} else {
			t.logger.Errorw("Failed to get revision throttler", zap.Error(err), zap.String(logkey.Key, rev.String()))
		}
	} else {
		rt.handlePubEpsUpdate(eps, t.ipAddress)
	}
}

func (rt *revisionThrottler) handlePubEpsUpdate(eps *corev1.Endpoints, selfIP string) {
	// NB: this is guaranteed to be executed on a single thread.
	epSet := healthyAddresses(eps, rt.protocol)
	if !epSet.Has(selfIP) {
		// No need to do anything, this activator is not in path.
		return
	}

	// We are using List to have the IP addresses sorted for consistent results.
	epsL := sets.List(epSet)
	newNA, newAI := int32(len(epsL)), int32(inferIndex(epsL, selfIP))
	if newAI == -1 {
		// No need to do anything, this activator is not in path.
		return
	}

	na, ai := rt.numActivators.Load(), rt.activatorIndex.Load()
	if na == newNA && ai == newAI {
		// The state didn't change, do nothing
		return
	}

	rt.numActivators.Store(newNA)
	rt.activatorIndex.Store(newAI)
	rt.logger.Infof("This activator index is %d/%d was %d/%d",
		newAI, newNA, ai, na)
	rt.updateCapacity(rt.backendCount)
}

// inferIndex returns the index of this activator slice.
// If inferIndex returns -1, it means that this activator will not receive
// any traffic just yet so, do not participate in slicing, this happens after
// startup, but before this activator is threaded into the endpoints
// (which is up to 10s after reporting healthy).
// For now we are just sorting the IP addresses of all activators
// and finding our index in that list.
func inferIndex(eps []string, ipAddress string) int {
	idx := sort.SearchStrings(eps, ipAddress)

	// Check if this activator is part of the endpoints slice?
	if idx == len(eps) || eps[idx] != ipAddress {
		return -1
	}
	return idx
}

func (t *Throttler) publicEndpointsUpdated(newObj interface{}) {
	endpoints := newObj.(*corev1.Endpoints)
	t.logger.Info("Updated public Endpoints: ", endpoints.Name)
	t.epsUpdateCh <- endpoints
}

// minOneOrValue function returns num if its greater than 1
// else the function returns 1
func minOneOrValue(num int) int {
	if num > 1 {
		return num
	}
	return 1
}

// infiniteBreaker is basically a short circuit.
// infiniteBreaker provides us capability to send unlimited number
// of requests to the downstream system.
// This is to be used only when the container concurrency is unset
// (i.e. infinity).
// The infiniteBreaker will, though, block the requests when
// downstream capacity is 0.
type infiniteBreaker struct {
	// mu guards `broadcast` channel.
	mu sync.RWMutex

	// broadcast channel is used notify the waiting requests that
	// downstream capacity showed up.
	// When the downstream capacity switches from 0 to 1, the channel is closed.
	// When the downstream capacity disappears, the a new channel is created.
	// Reads/Writes to the `broadcast` must be guarded by `mu`.
	broadcast chan struct{}

	// concurrency in the infinite breaker takes only two values
	// 0 (no downstream capacity) and 1 (infinite downstream capacity).
	// `Maybe` checks this value to determine whether to proxy the request
	// immediately or wait for capacity to appear.
	concurrency atomic.Int32

	logger *zap.SugaredLogger
}

// newInfiniteBreaker creates an infiniteBreaker
func newInfiniteBreaker(logger *zap.SugaredLogger) *infiniteBreaker {
	return &infiniteBreaker{
		broadcast: make(chan struct{}),
		logger:    logger,
	}
}

// Capacity returns the current capacity of the breaker
func (ib *infiniteBreaker) Capacity() int {
	return int(ib.concurrency.Load())
}

func zeroOrOne(x int) int32 {
	if x == 0 {
		return 0
	}
	return 1
}

// UpdateConcurrency sets the concurrency of the breaker
func (ib *infiniteBreaker) UpdateConcurrency(cc int) {
	rcc := zeroOrOne(cc)
	// We lock here to make sure two scale up events don't
	// stomp on each other's feet.
	ib.mu.Lock()
	defer ib.mu.Unlock()
	old := ib.concurrency.Swap(rcc)

	// Scale up/down event.
	if old != rcc {
		if rcc == 0 {
			// Scaled to 0.
			ib.broadcast = make(chan struct{})
		} else {
			close(ib.broadcast)
		}
	}
}

// Maybe executes thunk when capacity is available
func (ib *infiniteBreaker) Maybe(ctx context.Context, thunk func()) error {
	has := ib.Capacity()
	// We're scaled to serve.
	if has > 0 {
		thunk()
		return nil
	}

	// Make sure we lock to get the channel, to avoid
	// race between Maybe and UpdateConcurrency.
	var ch chan struct{}
	ib.mu.RLock()
	ch = ib.broadcast
	ib.mu.RUnlock()
	select {
	case <-ch:
		// Scaled up.
		thunk()
		return nil
	case <-ctx.Done():
		ib.logger.Info("Context is closed: ", ctx.Err())
		return ctx.Err()
	}
}

func (ib *infiniteBreaker) Reserve(context.Context) (func(), bool) { return noop, true }
