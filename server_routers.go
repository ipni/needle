package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type router interface {
	providersRouter
	peersRouter
	ipnsRouter
	dhtRouter
}

type providersRouter interface {
	FindProviders(ctx context.Context, cid cid.Cid, limit int) (iter.ResultIter[types.Record], error)
}

type peersRouter interface {
	FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error)
}

type ipnsRouter interface {
	GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)
	PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
}

type dhtRouter interface {
	GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error)
}

var _ server.ContentRouter = composableRouter{}

type composableRouter struct {
	providers providersRouter
	peers     peersRouter
	ipns      ipnsRouter
	dht       dhtRouter
}

func (r composableRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	if r.providers == nil {
		return iter.ToResultIter(iter.FromSlice([]types.Record{})), nil
	}
	return r.providers.FindProviders(ctx, key, limit)
}

func (r composableRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	if r.peers == nil {
		return iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{})), nil
	}
	return r.peers.FindPeers(ctx, pid, limit)
}

func (r composableRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	if r.dht == nil {
		// Return ErrNotSupported when no DHT is available (e.g., disabled via --dht=disabled CLI param).
		// This returns HTTP 501 Not Implemented instead of misleading HTTP 200 with empty results.
		return nil, routing.ErrNotSupported
	}
	return r.dht.GetClosestPeers(ctx, key)
}

func (r composableRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	if r.ipns == nil {
		return nil, routing.ErrNotFound
	}
	return r.ipns.GetIPNS(ctx, name)
}

func (r composableRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	if r.ipns == nil {
		return nil
	}
	return r.ipns.PutIPNS(ctx, name, record)
}

//lint:ignore SA1019 // ignore staticcheck
func (r composableRouter) ProvideBitswap(ctx context.Context, req *server.BitswapWriteProvideRequest) (time.Duration, error) {
	return 0, routing.ErrNotSupported
}

// Per-router timing for the parallel router.
//
// Every record and every router completion already passes through manyIter's
// forwarding goroutines, so that is where all of this is measured. Nothing
// about what is forwarded, or when, changes: the instrumentation only reads
// the clock and counts.
const (
	routerOpProviders = "providers"
	routerOpPeers     = "peers"
	routerOpClosest   = "closest"

	// A router is "exhausted" when its iterator ran out, "cancelled" when the
	// request ended under it - the records limit was reached, or the client
	// went away.
	routerDoneExhausted = "exhausted"
	routerDoneCancelled = "cancelled"
)

// routerTimingBuckets spans the range these lookups actually live in: a
// delegated HTTP round trip is tens of milliseconds, a DHT walk runs to the
// 5s fullrt timeout.
var routerTimingBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1, 1.5, 2, 3, 5, 7.5, 10}

var (
	// routerFirstResult answers "when does this router start producing", which
	// is what a cut-the-slow-router-off policy would be set from.
	routerFirstResult = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "first_result_seconds",
		Subsystem: "router",
		Namespace: name,
		Help:      "Time from the start of a parallel routing request to a router's first forwarded record. Only observed for routers that produced at least one record",
		Buckets:   routerTimingBuckets,
	}, []string{"op", "router"})

	// routerDone answers "when is this router finished with the request". A
	// JSON response cannot be written until every router is done or the
	// records limit is hit, so the slowest of these is the response time.
	routerDone = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "done_seconds",
		Subsystem: "router",
		Namespace: name,
		Help:      "Time from the start of a parallel routing request to a router finishing, by why it finished",
		Buckets:   routerTimingBuckets,
	}, []string{"op", "router", "reason"})

	// routerRecords counts what each router contributed.
	routerRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "records",
		Subsystem: "router",
		Namespace: name,
		Help:      "Number of records forwarded per router",
	}, []string{"op", "router"})

	// routerExclusiveRecords counts what would have been lost had the router
	// not run: keys no other router produced in the same request.
	routerExclusiveRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "exclusive_records",
		Subsystem: "router",
		Namespace: name,
		Help:      "Number of records whose peer ID no other router produced in the same request",
	}, []string{"op", "router"})

	// routerTail is how long the last router kept the request open on its own,
	// which is exactly the time a cut would save.
	routerTail = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "tail_seconds",
		Subsystem: "router",
		Namespace: name,
		Help:      "Gap between the second-to-last and the last router finishing, observed once per multi-router request under the last finisher",
		Buckets:   routerTimingBuckets,
	}, []string{"router"})

	// routerLastFinisher is the denominator for routerTail: how often each
	// router was the one holding the request open.
	routerLastFinisher = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "last_finisher",
		Subsystem: "router",
		Namespace: name,
		Help:      "Number of multi-router requests in which this router was the last to finish",
	}, []string{"router"})
)

// routerName gives one member of a parallelRouter a stable, low-cardinality
// metric label. combineRouters wraps the DHT in sanitizeRouter and (when the
// address book is on) cachedRouter, so the wrappers are peeled off until the
// router that does the work is reached. The label set is therefore at most one
// "dht" plus one per configured delegated endpoint.
func routerName(r router) string {
	// Bounded so a wrapper that somehow wrapped itself cannot spin here.
	for range 8 {
		switch v := r.(type) {
		case sanitizeRouter:
			r = v.router
		case cachedRouter:
			r = v.router
		case dnsAddrRouter:
			r = v.router
		case libp2pRouter:
			return "dht"
		case clientRouter:
			return "delegated:" + v.name
		default:
			return "other"
		}
	}
	return "other"
}

// recordKey identifies a record for the exclusive-records count. peer.ID is a
// string of raw bytes underneath, so this is a free conversion rather than a
// base58 encode. Records with no peer ID, and schemas we do not know, have no
// key and are left out of the count.
func recordKey(v any) (string, bool) {
	switch r := v.(type) {
	case *types.PeerRecord:
		if r == nil || r.ID == nil {
			return "", false
		}
		return string(*r.ID), true
	//lint:ignore SA1019 // bitswap records still arrive from older routers
	case *types.BitswapRecord:
		if r == nil || r.ID == nil {
			return "", false
		}
		return string(*r.ID), true
	}
	return "", false
}

// routerTrace is one parallel routing request's per-router timing. find builds
// it, manyIter's goroutines fill it in, and finalize reads it once when the
// request ends. When the trace log is off it still carries the key map, which
// is the only per-request cost the instrumentation adds.
type routerTrace struct {
	op    string
	names []string
	start time.Time
	log   bool

	mu       sync.Mutex
	first    []time.Duration // -1 until the router forwards something
	done     []time.Duration
	records  []int
	keys     map[string]int // record key -> the only router that produced it, or -1 once shared
	lastIdx  int
	lastDone time.Duration
	prevDone time.Duration
	finished int

	once sync.Once
}

func newRouterTrace(op string, names []string, start time.Time, log bool) *routerTrace {
	rt := &routerTrace{
		op:      op,
		names:   names,
		start:   start,
		log:     log,
		first:   make([]time.Duration, len(names)),
		done:    make([]time.Duration, len(names)),
		records: make([]int, len(names)),
		keys:    make(map[string]int),
		lastIdx: -1,
	}
	for i := range rt.first {
		rt.first[i] = -1
	}
	return rt
}

// record notes one record forwarded by router i. It is called after the send
// succeeds, so the time it stamps is when the record actually left the router.
func (rt *routerTrace) record(i int, v any) {
	if rt == nil {
		return
	}
	elapsed := time.Since(rt.start)
	key, hasKey := recordKey(v)

	rt.mu.Lock()
	rt.records[i]++
	isFirst := rt.first[i] < 0
	if isFirst {
		rt.first[i] = elapsed
	}
	if hasKey {
		// -1 marks a key more than one router produced. Which router got there
		// first does not matter: the question is whether it was alone.
		if prev, seen := rt.keys[key]; !seen {
			rt.keys[key] = i
		} else if prev != i {
			rt.keys[key] = -1
		}
	}
	rt.mu.Unlock()

	routerRecords.WithLabelValues(rt.op, rt.names[i]).Inc()
	if isFirst {
		routerFirstResult.WithLabelValues(rt.op, rt.names[i]).Observe(elapsed.Seconds())
	}
}

// finish notes that router i's goroutine has exited. cancelled is true only
// when the request ended under it - the records limit was reached, or the
// client went away - as opposed to its iterator running out.
//
// The routers serialise here, so the last caller is the last finisher and
// prevDone is the one before it, which is what makes the tail a plain
// subtraction.
func (rt *routerTrace) finish(i int, cancelled bool) {
	if rt == nil {
		return
	}
	elapsed := time.Since(rt.start)
	reason := routerDoneExhausted
	if cancelled {
		reason = routerDoneCancelled
	}

	rt.mu.Lock()
	rt.done[i] = elapsed
	rt.prevDone = rt.lastDone
	rt.lastDone = elapsed
	rt.lastIdx = i
	rt.finished++
	rt.mu.Unlock()

	routerDone.WithLabelValues(rt.op, rt.names[i], reason).Observe(elapsed.Seconds())
}

// finalize closes the request out: it resolves the exclusive-record question
// once (a key produced by exactly one router is exclusive to it), observes the
// tail the last router held the request open for, and emits the trace line. It
// runs exactly once, from whichever of the closer goroutine and Close reaches
// it first.
func (rt *routerTrace) finalize(cancelled bool) {
	if rt == nil {
		return
	}
	rt.once.Do(func() { rt.emit(cancelled) })
}

// routerTraceSnapshot is one request's finished timing, taken under the lock so
// the rest of finalize needs neither the lock nor a second pass over the keys.
type routerTraceSnapshot struct {
	first     []time.Duration
	done      []time.Duration
	records   []int
	exclusive []int
	lastIdx   int
	lastDone  time.Duration
	prevDone  time.Duration
	finished  int
}

func (rt *routerTrace) snapshot() routerTraceSnapshot {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	exclusive := make([]int, len(rt.names))
	for _, i := range rt.keys {
		if i >= 0 {
			exclusive[i]++
		}
	}
	return routerTraceSnapshot{
		first:     slices.Clone(rt.first),
		done:      slices.Clone(rt.done),
		records:   slices.Clone(rt.records),
		exclusive: exclusive,
		lastIdx:   rt.lastIdx,
		lastDone:  rt.lastDone,
		prevDone:  rt.prevDone,
		finished:  rt.finished,
	}
}

func (rt *routerTrace) emit(cancelled bool) {
	snap := rt.snapshot()

	for i, n := range rt.names {
		if snap.exclusive[i] > 0 {
			routerExclusiveRecords.WithLabelValues(rt.op, n).Add(float64(snap.exclusive[i]))
		}
	}

	// The tail needs something to subtract, so it wants two routers that both
	// finished.
	if len(rt.names) > 1 && snap.finished > 1 && snap.lastIdx >= 0 {
		routerTail.WithLabelValues(rt.names[snap.lastIdx]).Observe((snap.lastDone - snap.prevDone).Seconds())
		routerLastFinisher.WithLabelValues(rt.names[snap.lastIdx]).Inc()
	}

	if !rt.log {
		return
	}
	logger.Infow("parallel routing request", rt.traceFields(snap, cancelled)...)
}

// traceFields is the one trace line's payload: the request as a whole, then
// four fields per router. A router that produced nothing has zero records and
// a first_ms of -1, which is the "never happened" marker - zero would read as
// "instantly".
func (rt *routerTrace) traceFields(snap routerTraceSnapshot, cancelled bool) []any {
	reason := routerDoneExhausted
	if cancelled {
		reason = routerDoneCancelled
	}

	fields := make([]any, 0, 6+8*len(rt.names))
	fields = append(fields, "op", rt.op, "total_ms", durationMillis(snap.lastDone), "reason", reason)
	for i, n := range rt.names {
		fields = append(fields,
			n+"_first_ms", durationMillis(snap.first[i]),
			n+"_done_ms", durationMillis(snap.done[i]),
			n+"_records", snap.records[i],
			n+"_exclusive", snap.exclusive[i],
		)
	}
	return fields
}

// durationMillis renders a duration as whole milliseconds. A negative duration
// is the "never happened" marker and stays -1.
func durationMillis(d time.Duration) int64 {
	if d < 0 {
		return -1
	}
	return d.Milliseconds()
}

var _ server.ContentRouter = parallelRouter{}

type parallelRouter struct {
	routers []router
	// trace turns on the per-request trace line (--router-trace). The metrics
	// above are always on; only the log line is gated.
	trace bool
}

func (r parallelRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	return find(ctx, r.routers, routerOpProviders, r.trace, func(ri router) (iter.ResultIter[types.Record], error) {
		return ri.FindProviders(ctx, key, limit)
	})
}

func (r parallelRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	return find(ctx, r.routers, routerOpPeers, r.trace, func(ri router) (iter.ResultIter[*types.PeerRecord], error) {
		return ri.FindPeers(ctx, pid, limit)
	})
}

func find[T any](ctx context.Context, routers []router, op string, trace bool, call func(router) (iter.ResultIter[T], error)) (iter.ResultIter[T], error) {
	// The clock starts here, not after the iterators are built: creating a
	// delegated iterator is an HTTP round trip, and that is part of what the
	// router cost the request.
	start := time.Now()

	switch len(routers) {
	case 0:
		return iter.ToResultIter(iter.FromSlice([]T{})), nil
	case 1:
		return call(routers[0])
	}

	its := make([]iter.ResultIter[T], 0, len(routers))
	names := make([]string, 0, len(routers))
	var err error
	for _, ri := range routers {
		it, itErr := call(ri)

		if itErr != nil {
			logger.Warnf("error from router: %w", itErr)
			err = errors.Join(err, itErr)
		} else {
			its = append(its, it)
			names = append(names, routerName(ri))
		}
	}

	// If all iterators failed to be created, then return the error.
	if len(its) == 0 {
		logger.Warnf("failed to create all iterators: %w", err)
		return nil, err
	} else if err != nil {
		logger.Warnf("failed to create some iterators: %w", err)
	}

	// Otherwise return manyIter with remaining iterators.
	return newManyIter(ctx, its, newRouterTrace(op, names, start, trace)), nil
}

func (r parallelRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	return find(ctx, r.routers, routerOpClosest, r.trace, func(ri router) (iter.ResultIter[*types.PeerRecord], error) {
		return ri.GetClosestPeers(ctx, key)
	})
}

type manyIter[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	its    []iter.ResultIter[T]
	ch     chan iter.Result[T]
	val    iter.Result[T]
	done   bool
	trace  *routerTrace // nil disables the timing entirely
}

func newManyIter[T any](ctx context.Context, its []iter.ResultIter[T], trace *routerTrace) *manyIter[T] {
	ctx, cancel := context.WithCancel(ctx)

	mi := &manyIter[T]{
		ctx:    ctx,
		cancel: cancel,
		its:    its,
		ch:     make(chan iter.Result[T]),
		trace:  trace,
	}

	for i, it := range its {
		mi.wg.Add(1)
		go func(i int, it iter.ResultIter[T]) {
			defer mi.wg.Done()
			for it.Next() {
				val := it.Val()
				select {
				case mi.ch <- val:
					if val.Err == nil {
						trace.record(i, val.Val)
					}
				case <-ctx.Done():
					trace.finish(i, true)
					return
				}
			}
			// The iterator ran out, but that is not on its own "exhausted":
			// Close drains the channel to unblock pending sends, so a router
			// that was still producing when the request was cancelled will
			// often push its last records into the drain and then run out. The
			// question the label answers is whether the request had already
			// ended, so that is what it is read from.
			trace.finish(i, ctx.Err() != nil)
		}(i, it)
	}

	go func() {
		mi.wg.Wait()
		// Before the close, so a consumer that returns the moment the channel
		// closes cannot race the trace line for the request it just finished.
		trace.finalize(ctx.Err() != nil)
		close(mi.ch)
	}()

	return mi
}

func (mi *manyIter[T]) Next() bool {
	if mi.done {
		return false
	}

	select {
	case val, ok := <-mi.ch:
		if ok {
			mi.val = val
		} else {
			mi.done = true
		}
	case <-mi.ctx.Done():
		mi.done = true
	}

	return !mi.done
}

func (mi *manyIter[T]) Val() iter.Result[T] {
	return mi.val
}

func (mi *manyIter[T]) Close() error {
	if mi.done {
		return nil // Already closed, idempotent
	}
	mi.done = true
	mi.cancel() // Signal goroutines to stop

	// The channel will be closed by the goroutine in newManyIter once all workers finish
	// We just need to drain it to unblock any pending sends
	// This is expected behavior when client terminates early (per HTTP routing spec)
	for range mi.ch {
		// Discard remaining values
	}

	// The closer goroutine finalizes before it closes the channel, so by here
	// it has already run; this is only for the case where it somehow has not.
	mi.trace.finalize(true)

	// Now close child iterators
	var err error
	for _, it := range mi.its {
		err = errors.Join(err, it.Close())
	}
	if err != nil {
		logger.Warnf("errors on closing iterators: %w", err)
	}
	return err
}

// isExpiredIPNSRecord reports whether an EOL-type IPNS record has passed its
// validity. A record past its EOL is cryptographically invalid, so returning it
// only hands the caller a record that fails validation. Records with a non-EOL
// validity type or an unreadable validity are treated as not expired.
//
// Every backend already validates the record it returns, so this is a
// re-check rather than the first line of defense. We still do it because a
// signature is immutable and verified once, but EOL is time-varying: the
// longer a record lingers (a cache hit, a stored copy, clock drift between
// our infrastructure and the producer), the more likely it has expired
// between the backend's check and the moment we hand it to the user.
// parallelRouter is first-result-wins with no revalidation, so without this
// the aggregator could let an expired record win the race. See
// https://github.com/ipfs/boxo/pull/1166 for the upstream cache-control fix.
func isExpiredIPNSRecord(rec *ipns.Record) bool {
	if rec == nil {
		return false
	}
	validityType, err := rec.ValidityType()
	if err != nil || validityType != ipns.ValidityEOL {
		return false
	}
	validity, err := rec.Validity()
	if err != nil {
		return false
	}
	return time.Now().After(validity)
}

func (r parallelRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	switch len(r.routers) {
	case 0:
		return nil, routing.ErrNotFound
	case 1:
		rec, err := r.routers[0].GetIPNS(ctx, name)
		if err != nil {
			return nil, err
		}
		if isExpiredIPNSRecord(rec) {
			return nil, routing.ErrNotFound
		}
		return rec, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan struct {
		val *ipns.Record
		err error
	})
	for _, ri := range r.routers {
		go func(ri router) {
			value, err := ri.GetIPNS(ctx, name)
			select {
			case results <- struct {
				val *ipns.Record
				err error
			}{
				val: value,
				err: err,
			}:
			case <-ctx.Done():
			}
		}(ri)
	}

	var errs error

	for range r.routers {
		select {
		case res := <-results:
			switch res.err {
			case nil:
				// An expired record is invalid and must not be served; treat it
				// as not found and keep waiting for another router to answer.
				if isExpiredIPNSRecord(res.val) {
					continue
				}
				return res.val, nil
			case routing.ErrNotFound, routing.ErrNotSupported:
				continue
			}
			// If the context has expired, just return that error
			// and ignore the other errors.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			errs = errors.Join(errs, res.err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if errs == nil {
		return nil, routing.ErrNotFound
	}

	return nil, errs
}

func (r parallelRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	switch len(r.routers) {
	case 0:
		return nil
	case 1:
		return r.routers[0].PutIPNS(ctx, name, record)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	results := make([]error, len(r.routers))
	wg.Add(len(r.routers))
	for i, ri := range r.routers {
		go func(ri router, i int) {
			results[i] = ri.PutIPNS(ctx, name, record)
			wg.Done()
		}(ri, i)
	}
	wg.Wait()

	var errs error
	for _, err := range results {
		errs = errors.Join(errs, err)
	}
	return errs
}

//lint:ignore SA1019 // ignore staticcheck
func (r parallelRouter) ProvideBitswap(ctx context.Context, req *server.BitswapWriteProvideRequest) (time.Duration, error) {
	return 0, routing.ErrNotSupported
}

var _ router = libp2pRouter{}

type libp2pRouter struct {
	host    host.Host
	routing routing.Routing
}

func (d libp2pRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	ctx, cancel := context.WithCancel(ctx)
	ch := d.routing.FindProvidersAsync(ctx, key, limit)
	return iter.ToResultIter(&peerChanIter{
		ch:     ch,
		cancel: cancel,
	}), nil
}

func (d libp2pRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	addr, err := d.routing.FindPeer(ctx, pid)
	if err != nil {
		return nil, err
	}

	rec := &types.PeerRecord{
		Schema: types.SchemaPeer,
		ID:     &addr.ID,
	}

	for _, addr := range addr.Addrs {
		rec.Addrs = append(rec.Addrs, types.Multiaddr{Multiaddr: addr})
	}

	return iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{rec})), nil
}

func (d libp2pRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	// Per the spec, if the peer ID is empty, we should use self.
	if key == cid.Undef {
		return nil, errors.New("GetClosestPeers: key is undefined")
	}

	keyStr := string(key.Hash())
	var peers []peer.ID
	var err error

	switch dhtClient := d.routing.(type) {
	case *dual.DHT:
		// Only use WAN DHT for public HTTP Routing API (same as Kubo)
		// LAN DHT contains private network peers that should not be exposed publicly.
		if dhtClient.WAN == nil {
			return nil, fmt.Errorf("GetClosestPeers not supported: WAN DHT is not available")
		}
		peers, err = dhtClient.WAN.GetClosestPeers(ctx, keyStr)
		if err != nil {
			return nil, err
		}
	case *fullrt.FullRT:
		peers, err = dhtClient.GetClosestPeers(ctx, keyStr)
		if err != nil {
			return nil, err
		}
	case *dht.IpfsDHT:
		peers, err = dhtClient.GetClosestPeers(ctx, keyStr)
		if err != nil {
			return nil, err
		}
	case *bundledDHT:
		// bundledDHT uses either fullRT (when ready) or standard DHT
		// We need to call GetClosestPeers on the active DHT
		activeDHT := dhtClient.getDHT()
		switch dht := activeDHT.(type) {
		case *fullrt.FullRT:
			peers, err = dht.GetClosestPeers(ctx, keyStr)
		case *dht.IpfsDHT:
			peers, err = dht.GetClosestPeers(ctx, keyStr)
		default:
			return nil, errors.New("bundledDHT returned unexpected DHT type")
		}
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("cannot call GetClosestPeers on DHT implementation")
	}

	// We have some DHT-closest peers. Find addresses for them.
	// The addresses should be in the peerstore.
	var records []*types.PeerRecord
	for _, p := range peers {
		addrs := d.host.Peerstore().Addrs(p)
		rAddrs := make([]types.Multiaddr, len(addrs))
		for i, addr := range addrs {
			rAddrs[i] = types.Multiaddr{Multiaddr: addr}
		}
		record := types.PeerRecord{
			ID:     &p,
			Schema: types.SchemaPeer,
			Addrs:  rAddrs,
			// we dont seem to care about protocol/extra infos
		}
		records = append(records, &record)
	}

	return iter.ToResultIter(iter.FromSlice(records)), nil
}

func (d libp2pRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	raw, err := d.routing.GetValue(ctx, string(name.RoutingKey()))
	if err != nil {
		return nil, err
	}

	return ipns.UnmarshalRecord(raw)
}

func (d libp2pRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	raw, err := ipns.MarshalRecord(record)
	if err != nil {
		return err
	}

	return d.routing.PutValue(ctx, string(name.RoutingKey()), raw)
}

type peerChanIter struct {
	ch     <-chan peer.AddrInfo
	cancel context.CancelFunc
	next   *peer.AddrInfo
}

func (it *peerChanIter) Next() bool {
	addr, ok := <-it.ch
	if ok {
		it.next = &addr
		return true
	}
	it.next = nil
	return false
}

func (it *peerChanIter) Val() types.Record {
	if it.next == nil {
		return nil
	}

	rec := &types.PeerRecord{
		Schema: types.SchemaPeer,
		ID:     &it.next.ID,
	}

	for _, addr := range it.next.Addrs {
		rec.Addrs = append(rec.Addrs, types.Multiaddr{Multiaddr: addr})
	}

	return rec
}

func (it *peerChanIter) Close() error {
	it.cancel()
	return nil
}

var _ server.ContentRouter = sanitizeRouter{}

type sanitizeRouter struct {
	router
}

func (r sanitizeRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	it, err := r.router.FindProviders(ctx, key, limit)
	if err != nil {
		return nil, err
	}

	return iter.Map(it, func(v iter.Result[types.Record]) iter.Result[types.Record] {
		if v.Err != nil || v.Val == nil {
			return v
		}

		switch v.Val.GetSchema() {
		case types.SchemaPeer:
			result, ok := v.Val.(*types.PeerRecord)
			if !ok {
				logger.Errorw("problem casting find providers result", "Schema", v.Val.GetSchema(), "Type", reflect.TypeOf(v).String())
				return v
			}

			result.Addrs = filterPrivateMultiaddr(result.Addrs)
			v.Val = result

		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//lint:ignore SA1019 // ignore staticcheck
			result, ok := v.Val.(*types.BitswapRecord)
			if !ok {
				logger.Errorw("problem casting find providers result", "Schema", v.Val.GetSchema(), "Type", reflect.TypeOf(v).String())
				return v
			}

			result.Addrs = filterPrivateMultiaddr(result.Addrs)
			v.Val = result
		}

		return v
	}), nil
}

func (r sanitizeRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	it, err := r.router.FindPeers(ctx, pid, limit)
	if err != nil {
		return nil, err
	}

	return iter.Map(it, func(v iter.Result[*types.PeerRecord]) iter.Result[*types.PeerRecord] {
		if v.Err != nil || v.Val == nil {
			return v
		}

		v.Val.Addrs = filterPrivateMultiaddr(v.Val.Addrs)
		return v
	}), nil
}

func (r sanitizeRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	it, err := r.router.GetClosestPeers(ctx, key)
	if err != nil {
		return nil, err
	}

	return iter.Map(it, func(v iter.Result[*types.PeerRecord]) iter.Result[*types.PeerRecord] {
		if v.Err != nil || v.Val == nil {
			return v
		}

		v.Val.Addrs = filterPrivateMultiaddr(v.Val.Addrs)
		return v
	}), nil
}

//lint:ignore SA1019 // ignore staticcheck
func (r sanitizeRouter) ProvideBitswap(ctx context.Context, req *server.BitswapWriteProvideRequest) (time.Duration, error) {
	return 0, routing.ErrNotSupported
}

func filterPrivateMultiaddr(a []types.Multiaddr) []types.Multiaddr {
	b := make([]types.Multiaddr, 0, len(a))

	for _, addr := range a {
		if manet.IsPrivateAddr(addr.Multiaddr) {
			continue
		}

		b = append(b, addr)
	}

	slices.SortFunc(b, compareAddrs)

	return b
}

// compareAddrs sorts for a stable response across requests, ordered by how
// directly a client can dial each address: IP addresses first, then DNS names,
// then /dnsaddr indirections that need a TXT lookup, then anything else, with
// relay (/p2p-circuit) addresses as the last resort. Addresses can arrive in
// nondeterministic order (e.g. the peerstore stores them in a map).
func compareAddrs(x, y types.Multiaddr) int {
	if d := addrSortRank(x.Multiaddr) - addrSortRank(y.Multiaddr); d != 0 {
		return d
	}
	return bytes.Compare(x.Multiaddr.Bytes(), y.Multiaddr.Bytes())
}

// sortedAddrs returns addrs in compareAddrs order without dropping any. It is
// for records that pass through otherwise untouched, so every record in a
// response comes out ordered the same way.
func sortedAddrs(addrs []types.Multiaddr) []types.Multiaddr {
	out := slices.Clone(addrs)
	slices.SortFunc(out, compareAddrs)
	return out
}

// addrSortRank buckets an address by how directly a client can dial it: IP
// addresses, then DNS names that cost one lookup, then /dnsaddr indirections
// that need a TXT lookup, then everything else, then /p2p-circuit relays. A
// relay reached through any transport counts as a relay.
func addrSortRank(addr ma.Multiaddr) int {
	if isRelayAddr(addr) {
		return 4
	}
	protos := addr.Protocols()
	if len(protos) == 0 {
		return 3
	}
	switch protos[0].Code {
	case ma.P_IP4, ma.P_IP6:
		return 0
	case ma.P_DNS, ma.P_DNS4, ma.P_DNS6:
		return 1
	case ma.P_DNSADDR:
		return 2
	default:
		return 3
	}
}

// dnsAddrRouter replaces /dnsaddr addresses with the addresses they name,
// before the /routing/v1 handler applies filter-addrs.
//
// It wraps each whole composed router rather than sitting next to
// sanitizeRouter, because sanitizeRouter only covers the DHT branch while
// /dnsaddr records reach someguy from delegated HTTP routers.
type dnsAddrRouter struct {
	router
	resolver *dnsAddrResolver
	mode     DNSAddrResolution
}

var _ server.ContentRouter = dnsAddrRouter{}

//lint:ignore SA1019 // ignore staticcheck
func (r dnsAddrRouter) ProvideBitswap(ctx context.Context, req *server.BitswapWriteProvideRequest) (time.Duration, error) {
	return 0, routing.ErrNotSupported
}

// resolveRecordAddrs resolves a record's addresses and re-runs the private
// address filter over the result. Resolution has to happen before that filter,
// not after: manet treats /dnsaddr/anything as public, so a name that resolves
// to an RFC1918 address would otherwise be served to clients.
//
// Records that need no resolution are still sorted, so the whole response is
// ordered the same way, delegated-router records included.
func (r dnsAddrRouter) resolveRecordAddrs(ctx context.Context, pid *peer.ID, addrs []types.Multiaddr, budget *dnsAddrBudget) []types.Multiaddr {
	if pid == nil || !containsDNSAddr(addrs) {
		return sortedAddrs(addrs)
	}
	action := r.mode.action(ctx)
	if action == dnsAddrSkip {
		return sortedAddrs(addrs)
	}
	return filterPrivateMultiaddr(r.resolver.resolveAddrs(ctx, *pid, addrs, budget, action == dnsAddrAppend))
}

func (r dnsAddrRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	it, err := r.router.FindProviders(ctx, key, limit)
	if err != nil {
		return nil, err
	}

	budget := newDNSAddrBudget()
	return iter.Map(it, func(v iter.Result[types.Record]) iter.Result[types.Record] {
		if v.Err != nil || v.Val == nil {
			return v
		}

		switch v.Val.GetSchema() {
		case types.SchemaPeer:
			result, ok := v.Val.(*types.PeerRecord)
			if !ok {
				return v
			}
			result.Addrs = r.resolveRecordAddrs(ctx, result.ID, result.Addrs, budget)
			v.Val = result

		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//lint:ignore SA1019 // ignore staticcheck
			result, ok := v.Val.(*types.BitswapRecord)
			if !ok {
				return v
			}
			result.Addrs = r.resolveRecordAddrs(ctx, result.ID, result.Addrs, budget)
			v.Val = result
		}

		return v
	}), nil
}

// resolvePeerRecords resolves every record of one response, sharing one
// budget across it.
func (r dnsAddrRouter) resolvePeerRecords(ctx context.Context, it iter.ResultIter[*types.PeerRecord]) iter.ResultIter[*types.PeerRecord] {
	budget := newDNSAddrBudget()
	return iter.Map(it, func(v iter.Result[*types.PeerRecord]) iter.Result[*types.PeerRecord] {
		if v.Err != nil || v.Val == nil {
			return v
		}
		v.Val.Addrs = r.resolveRecordAddrs(ctx, v.Val.ID, v.Val.Addrs, budget)
		return v
	})
}

func (r dnsAddrRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	it, err := r.router.FindPeers(ctx, pid, limit)
	if err != nil {
		return nil, err
	}
	return r.resolvePeerRecords(ctx, it), nil
}

func (r dnsAddrRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	it, err := r.router.GetClosestPeers(ctx, key)
	if err != nil {
		return nil, err
	}
	return r.resolvePeerRecords(ctx, it), nil
}
