package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	traceapi "go.opentelemetry.io/otel/trace"
)

func TestSampler(t *testing.T) {
	t.Parallel()

	traceID := traceapi.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	remoteParent := func(sampled bool) context.Context {
		var flags traceapi.TraceFlags
		if sampled {
			flags = traceapi.FlagsSampled
		}
		sc := traceapi.NewSpanContext(traceapi.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     traceapi.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
			TraceFlags: flags,
			Remote:     true,
		})
		return traceapi.ContextWithRemoteSpanContext(context.Background(), sc)
	}

	cases := []struct {
		name     string
		fraction float64
		parent   context.Context
		kind     traceapi.SpanKind
		want     sdktrace.SamplingDecision
	}{
		{"sampled parent, fraction 0", 0, remoteParent(true), traceapi.SpanKindServer, sdktrace.RecordAndSample},
		{"sampled parent, fraction 1", 1, remoteParent(true), traceapi.SpanKindServer, sdktrace.RecordAndSample},
		{"unsampled parent, fraction 1", 1, remoteParent(false), traceapi.SpanKindServer, sdktrace.Drop},
		{"no parent, fraction 0", 0, context.Background(), traceapi.SpanKindServer, sdktrace.Drop},
		{"no parent, fraction 1", 1, context.Background(), traceapi.SpanKindServer, sdktrace.RecordAndSample},
		{"background root, fraction 1", 1, context.Background(), traceapi.SpanKindInternal, sdktrace.Drop},
		{"background root, client kind, fraction 1", 1, context.Background(), traceapi.SpanKindClient, sdktrace.Drop},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := newSampler(tc.fraction).ShouldSample(sdktrace.SamplingParameters{
				ParentContext: tc.parent,
				TraceID:       traceID,
				Name:          "needle GET /routing/v1/providers/{cid}",
				Kind:          tc.kind,
			})
			assert.Equal(t, tc.want, res.Decision)
		})
	}

	// The fraction applies to root requests: over many trace IDs, about half
	// are kept at 0.5.
	sampler := newSampler(0.5)
	kept := 0
	for i := range 1000 {
		id := traceapi.TraceID{}
		for j := range id {
			id[j] = byte(i * (j + 7) * 31)
		}
		res := sampler.ShouldSample(sdktrace.SamplingParameters{ParentContext: context.Background(), TraceID: id, Kind: traceapi.SpanKindServer})
		if res.Decision == sdktrace.RecordAndSample {
			kept++
		}
	}
	assert.InDelta(t, 500, kept, 150)
}

func TestRequestSpanName(t *testing.T) {
	t.Parallel()

	const c = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
	cases := []struct{ method, path, want string }{
		{http.MethodGet, "/routing/v1/providers/" + c, "needle GET /routing/v1/providers/{cid}"},
		{http.MethodGet, "/routing/v1/peers/12D3KooWExample", "needle GET /routing/v1/peers/{peer-id}"},
		{http.MethodGet, "/routing/v1/ipns/" + c, "needle GET /routing/v1/ipns/{cid}"},
		{http.MethodPut, "/routing/v1/ipns/" + c, "needle PUT /routing/v1/ipns/{cid}"},
		{http.MethodGet, "/routing/v1/dht/closest/peers/" + c, "needle GET /routing/v1/dht/closest/peers/{key}"},
		{http.MethodPut, "/routing/v1/providers", "needle PUT /routing/v1/providers"},
		{http.MethodPut, "/routing/v1/providers/", "needle PUT /routing/v1/providers"},
		{http.MethodGet, "/routing/v1/providers/", "needle GET /routing/v1/providers"},
		{http.MethodGet, "/something/else/" + c, "needle GET"},
		{"BREW", "/routing/v1/providers/" + c, "needle HTTP /routing/v1/providers/{cid}"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		got := requestSpanName("", r)
		assert.Equal(t, tc.want, got, tc.path)
		assert.NotContains(t, got, c)
	}
}

func TestRouterOutcome(t *testing.T) {
	t.Parallel()

	live := context.Background()
	timedOut, cancel := context.WithDeadline(live, time.Now().Add(-time.Second))
	defer cancel()
	gone, cancelGone := context.WithCancel(live)
	cancelGone()

	assert.Equal(t, routerDoneExhausted, routerOutcome(routerDoneExhausted, gone))
	assert.Equal(t, routerDoneCut, routerOutcome(routerDoneCut, timedOut))
	assert.Equal(t, routerOutcomeTimeout, routerOutcome(routerDoneCancelled, timedOut))
	assert.Equal(t, routerOutcomeClientCancelled, routerOutcome(routerDoneCancelled, gone))
	assert.Equal(t, routerOutcomeClosed, routerOutcome(routerDoneCancelled, live))
}

func TestServiceInstanceID(t *testing.T) {
	t.Parallel()

	id := serviceInstanceID("127.0.0.1:8190")
	assert.True(t, strings.HasSuffix(id, ":8190"), id)
	assert.NotEqual(t, id, serviceInstanceID("127.0.0.1:8191"))
}

func TestResourceHonoursOTELEnv(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "needle-canary")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=test,service.instance.id=custom")

	r, err := newResource(t.Context(), "127.0.0.1:8190")
	require.NoError(t, err)
	attrs := map[attribute.Key]string{}
	for _, kv := range r.Attributes() {
		attrs[kv.Key] = kv.Value.String()
	}
	assert.Equal(t, "needle-canary", attrs["service.name"])
	assert.Equal(t, "test", attrs["deployment.environment"])
	assert.Equal(t, "custom", attrs["service.instance.id"])
	assert.Equal(t, version, attrs["service.version"])
}

func TestOTLPEndpointSelectsExporter(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	exporters, err := newSpanExporters(t.Context())
	require.NoError(t, err)
	assert.Len(t, exporters, 1)
	for _, e := range exporters {
		_ = e.Shutdown(t.Context())
	}
}

// tracedServer serves h behind withTracing, recording every span it samples.
func tracedServer(t *testing.T, h http.Handler, fraction float64, authToken string, trust bool) (*httptest.Server, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(newSampler(fraction)), sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	srv := httptest.NewServer(withTracing(h, authToken, trust,
		otelhttp.WithTracerProvider(tp),
		otelhttp.WithPropagators(propagation.TraceContext{}),
	))
	t.Cleanup(srv.Close)
	return srv, rec
}

const (
	testTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	testParentSpan  = "00f067aa0ba902b7"
	testTraceparent = "00-" + testTraceID + "-" + testParentSpan + "-01"
)

func get(t *testing.T, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestTraceparentTrust(t *testing.T) {
	t.Parallel()

	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	path := "/routing/v1/providers/" + makeCID().String()
	unsampled := "00-" + testTraceID + "-" + testParentSpan + "-00"

	cases := []struct {
		name     string
		fraction float64
		auth     string
		trust    bool
		headers  map[string]string
		// joins is true when the span must continue the caller's trace,
		// false when it must start its own, and nil when nothing is sampled.
		joins *bool
	}{
		{name: "untrusted traceparent does not force sampling", headers: map[string]string{"Traceparent": testTraceparent}},
		{name: "untrusted traceparent becomes a link", fraction: 1, headers: map[string]string{"Traceparent": testTraceparent}, joins: ptr(false)},
		{name: "trusted proxy traceparent joins", trust: true, headers: map[string]string{"Traceparent": testTraceparent}, joins: ptr(true)},
		{name: "trusted proxy unsampled traceparent is not sampled", fraction: 1, trust: true, headers: map[string]string{"Traceparent": unsampled}},
		{name: "matching tracing auth joins", auth: "secret", headers: map[string]string{"Traceparent": testTraceparent, "Authorization": "secret"}, joins: ptr(true)},
		{name: "wrong tracing auth does not join", auth: "secret", headers: map[string]string{"Traceparent": testTraceparent, "Authorization": "nope"}},
		{name: "no traceparent, fraction 1", fraction: 1, joins: ptr(false)},
		{name: "no traceparent, fraction 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, rec := tracedServer(t, ok, tc.fraction, tc.auth, tc.trust)
			get(t, srv.URL+path, tc.headers)

			spans := rec.Ended()
			if tc.joins == nil {
				assert.Empty(t, spans)
				return
			}
			require.Len(t, spans, 1)
			s := spans[0]
			assert.Equal(t, "needle GET /routing/v1/providers/{cid}", s.Name())
			assert.Equal(t, traceapi.SpanKindServer, s.SpanKind())
			if *tc.joins {
				assert.Equal(t, testTraceID, s.SpanContext().TraceID().String())
				assert.Equal(t, testParentSpan, s.Parent().SpanID().String())
				return
			}
			assert.NotEqual(t, testTraceID, s.SpanContext().TraceID().String())
			assert.False(t, s.Parent().IsValid())
			if _, sent := tc.headers["Traceparent"]; sent {
				require.Len(t, s.Links(), 1)
				assert.Equal(t, testTraceID, s.Links()[0].SpanContext.TraceID().String())
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestRequestSpanStatus(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		status int
		want   codes.Code
	}{
		{http.StatusOK, codes.Unset},
		{http.StatusNotFound, codes.Unset},
		{http.StatusInternalServerError, codes.Error},
	} {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) })
		srv, rec := tracedServer(t, h, 1, "", false)
		get(t, srv.URL+"/routing/v1/providers/"+makeCID().String(), nil)
		spans := rec.Ended()
		require.Len(t, spans, 1)
		assert.Equal(t, tc.want, spans[0].Status().Code, "status %d", tc.status)
	}
}

// routingServer serves a providers request through boxo's handler and the
// given parallel router, the way needle wires them.
func routingServer(t *testing.T, routers []router, timeout time.Duration) (*httptest.Server, *tracetest.SpanRecorder) {
	t.Helper()
	h := server.Handler(&composableRouter{providers: parallelRouter{routers: routers}}, server.WithRoutingTimeout(timeout))
	return tracedServer(t, h, 1, "", false)
}

func spansByName(spans []sdktrace.ReadOnlySpan) map[string]sdktrace.ReadOnlySpan {
	m := make(map[string]sdktrace.ReadOnlySpan, len(spans))
	for _, s := range spans {
		m[s.Name()] = s
	}
	return m
}

func attrMap(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := map[attribute.Key]attribute.Value{}
	for _, kv := range s.Attributes() {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestRouterSpans(t *testing.T) {
	t.Parallel()

	failing := errors.New("upstream 502")
	routers := []router{
		namedRouter{name: "delegated:a", providers: func(ctx context.Context) iter.ResultIter[types.Record] {
			return &ctxIter{ctx: ctx, vals: []iter.Result[types.Record]{peerRec("A"), peerRec("B")}}
		}},
		namedRouter{name: "delegated:b", providers: func(ctx context.Context) iter.ResultIter[types.Record] {
			return &ctxIter{ctx: ctx, vals: []iter.Result[types.Record]{peerRec("B"), peerRec("C"), {Err: failing}}}
		}},
		// No providers iterator: the call itself fails.
		namedRouter{name: "delegated:broken"},
		sanitizeRouter{dhtTestRouter{namedRouter{name: "dht", providers: func(ctx context.Context) iter.ResultIter[types.Record] {
			return &ctxIter{ctx: ctx}
		}}}},
	}
	srv, rec := routingServer(t, routers, 5*time.Second)
	resp := get(t, srv.URL+"/routing/v1/providers/"+makeCID().String(), map[string]string{"Accept": "application/x-ndjson"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	require.Eventually(t, func() bool { return len(rec.Ended()) == 5 }, 5*time.Second, 10*time.Millisecond)
	spans := spansByName(rec.Ended())

	req := spans["needle GET /routing/v1/providers/{cid}"]
	require.NotNil(t, req)
	reqAttrs := attrMap(req)
	assert.Equal(t, "/routing/v1/providers/{cid}", reqAttrs["http.route"].AsString())
	assert.Equal(t, routerDoneExhausted, reqAttrs[attrRoutingOutcome].AsString())
	assert.Equal(t, int64(3), reqAttrs[attrRoutingRouters].AsInt64())

	for _, n := range []string{"delegated:a", "delegated:b", "delegated:broken", "dht"} {
		s := spans["needle router providers "+n]
		require.NotNil(t, s, n)
		assert.Equal(t, req.SpanContext().SpanID(), s.Parent().SpanID(), n)
		assert.Equal(t, req.SpanContext().TraceID(), s.SpanContext().TraceID(), n)
		assert.Equal(t, n, attrMap(s)[attrRouterName].AsString())
		assert.Equal(t, "providers", attrMap(s)[attrRouterOp].AsString())
	}

	a := attrMap(spans["needle router providers delegated:a"])
	assert.Equal(t, int64(2), a[attrRouterRecords].AsInt64())
	assert.Equal(t, int64(1), a[attrRouterExclusive].AsInt64()) // A
	assert.Equal(t, int64(0), a[attrRouterErrors].AsInt64())
	assert.Equal(t, routerDoneExhausted, a[attrRouterOutcome].AsString())
	assert.True(t, a[attrRouterFirstMs].AsInt64() >= 0)
	assert.Equal(t, codes.Unset, spans["needle router providers delegated:a"].Status().Code)

	b := attrMap(spans["needle router providers delegated:b"])
	assert.Equal(t, int64(2), b[attrRouterRecords].AsInt64())
	assert.Equal(t, int64(1), b[attrRouterExclusive].AsInt64()) // C
	assert.Equal(t, int64(1), b[attrRouterErrors].AsInt64())
	assert.Equal(t, codes.Error, spans["needle router providers delegated:b"].Status().Code)

	broken := spans["needle router providers delegated:broken"]
	assert.Equal(t, routerOutcomeError, attrMap(broken)[attrRouterOutcome].AsString())
	assert.Equal(t, codes.Error, broken.Status().Code)

	// A router that answered with nothing is not an error.
	dht := spans["needle router providers dht"]
	assert.Equal(t, int64(0), attrMap(dht)[attrRouterRecords].AsInt64())
	_, hasFirst := attrMap(dht)[attrRouterFirstMs]
	assert.False(t, hasFirst)
	assert.Equal(t, codes.Unset, dht.Status().Code)

	// The request found records, so it is not an error either.
	assert.Equal(t, codes.Unset, req.Status().Code)
}

func TestRouterSpanTimeout(t *testing.T) {
	t.Parallel()

	routers := []router{
		namedRouter{name: "delegated:a", providers: func(ctx context.Context) iter.ResultIter[types.Record] {
			return &ctxIter{ctx: ctx, vals: []iter.Result[types.Record]{peerRec("A")}}
		}},
		namedRouter{name: "slow", providers: func(ctx context.Context) iter.ResultIter[types.Record] {
			return &ctxIter{ctx: ctx, blockUntilCancel: true}
		}},
	}
	srv, rec := routingServer(t, routers, 100*time.Millisecond)
	resp := get(t, srv.URL+"/routing/v1/providers/"+makeCID().String(), map[string]string{"Accept": "application/x-ndjson"})
	_, _ = io.Copy(io.Discard, resp.Body)

	require.Eventually(t, func() bool { return len(rec.Ended()) == 3 }, 5*time.Second, 10*time.Millisecond)
	spans := spansByName(rec.Ended())
	assert.Equal(t, routerOutcomeTimeout, attrMap(spans["needle router providers slow"])[attrRouterOutcome].AsString())
	assert.Equal(t, codes.Unset, spans["needle router providers slow"].Status().Code)
	assert.Equal(t, routerDoneExhausted, attrMap(spans["needle router providers delegated:a"])[attrRouterOutcome].AsString())
	// The request span itself may close before the routers report: once the
	// timeout ends the response, nothing waits for the slow router. Its
	// span is the record of what the timeout cost.
	assert.Equal(t, spans["needle GET /routing/v1/providers/{cid}"].SpanContext().SpanID(), spans["needle router providers slow"].Parent().SpanID())
}

func TestAllRoutersFailedIsAnError(t *testing.T) {
	t.Parallel()

	srv, rec := routingServer(t, []router{namedRouter{name: "x"}, namedRouter{name: "y"}}, time.Second)
	resp := get(t, srv.URL+"/routing/v1/providers/"+makeCID().String(), nil)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	spans := spansByName(rec.Ended())
	require.Len(t, spans, 3)
	assert.Equal(t, codes.Error, spans["needle GET /routing/v1/providers/{cid}"].Status().Code)
	assert.Equal(t, codes.Error, spans["needle router providers x"].Status().Code)
}

func TestNoRecordsIsNotAnError(t *testing.T) {
	t.Parallel()

	empty := func(ctx context.Context) iter.ResultIter[types.Record] { return &ctxIter{ctx: ctx} }
	srv, rec := routingServer(t, []router{namedRouter{name: "x", providers: empty}, namedRouter{name: "y", providers: empty}}, time.Second)
	resp := get(t, srv.URL+"/routing/v1/providers/"+makeCID().String(), nil)
	require.Less(t, resp.StatusCode, http.StatusInternalServerError)

	require.Eventually(t, func() bool { return len(rec.Ended()) == 3 }, 5*time.Second, 10*time.Millisecond)
	for _, s := range rec.Ended() {
		assert.Equal(t, codes.Unset, s.Status().Code, s.Name())
	}
}

func TestSingleRouterSpan(t *testing.T) {
	t.Parallel()

	routers := []router{namedRouter{name: "only", providers: func(ctx context.Context) iter.ResultIter[types.Record] {
		return &ctxIter{ctx: ctx, vals: []iter.Result[types.Record]{peerRec("A"), peerRec("B")}}
	}}}
	srv, rec := routingServer(t, routers, time.Second)
	resp := get(t, srv.URL+"/routing/v1/providers/"+makeCID().String(), map[string]string{"Accept": "application/x-ndjson"})
	_, _ = io.Copy(io.Discard, resp.Body)

	require.Eventually(t, func() bool { return len(rec.Ended()) == 2 }, 5*time.Second, 10*time.Millisecond)
	s := spansByName(rec.Ended())["needle router providers only"]
	require.NotNil(t, s)
	assert.Equal(t, int64(2), attrMap(s)[attrRouterRecords].AsInt64())
	assert.Equal(t, routerDoneExhausted, attrMap(s)[attrRouterOutcome].AsString())
}

// An unsampled request must not pay for router spans: find gets the
// non-recording request span and builds none.
func TestUnsampledRequestHasNoRouterSpans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, span, counters := startRouterSpan(ctx, routerOpProviders, "dht")
	assert.False(t, span.IsRecording())
	assert.Nil(t, counters)
	counters.add(attrAddrBookHits, 1) // a nil counter ignores adds
	assert.Empty(t, counters.attributes())
}

// The cached address book reports, on the router span through the context,
// how it answered each record.
func TestAddrBookCountsOnRouterSpan(t *testing.T) {
	t.Parallel()

	c := makeCID()
	cached, sourced, missing := peer.ID("cached"), peer.ID("sourced"), peer.ID("missing")
	publicAddr := mustMultiaddr(t, "/ip4/137.21.14.12/tcp/4001")

	mr := &mockRouter{}
	mr.On("FindProviders", mock.Anything, c, 10).Return(newMockResultIter([]iter.Result[types.Record]{
		{Val: &types.PeerRecord{Schema: types.SchemaPeer, ID: &cached}},
		{Val: &types.PeerRecord{Schema: types.SchemaPeer, ID: &sourced, Addrs: []types.Multiaddr{publicAddr}}},
		{Val: &types.PeerRecord{Schema: types.SchemaPeer, ID: &missing}},
	}), nil)
	mr.On("FindPeers", mock.Anything, missing, 1).Return(nil, routing.ErrNotFound).Maybe()

	cab, err := newCachedAddrBook()
	require.NoError(t, err)
	cab.addrBook.AddAddrs(cached, []multiaddr.Multiaddr{publicAddr.Multiaddr}, time.Hour)

	ctx, counters := contextWithSpanCounters(t.Context())
	it, err := NewCachedRouter(mr, cab, 0).FindProviders(ctx, c, 10)
	require.NoError(t, err)
	_, err = iter.ReadAllResults(it)
	require.NoError(t, err)

	got := map[attribute.Key]int64{}
	for _, kv := range counters.attributes() {
		got[kv.Key] = kv.Value.AsInt64()
	}
	assert.Equal(t, int64(1), got[attrAddrBookHits])
	assert.Equal(t, int64(1), got[attrAddrBookUnused])
	assert.Equal(t, int64(1), got[attrAddrBookMisses])
	assert.Equal(t, int64(1), got[attrAddrBookDispatched])
}

// A request served by the DHT alone (no delegated endpoint for the operation)
// still gets a router span.
func TestTracedRouterSpan(t *testing.T) {
	t.Parallel()

	dht := sanitizeRouter{dhtTestRouter{namedRouter{name: "dht", peers: func(ctx context.Context) iter.ResultIter[*types.PeerRecord] {
		pid := peer.ID("p")
		return iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{{Schema: types.SchemaPeer, ID: &pid}}))
	}}}}
	h := server.Handler(&composableRouter{peers: newTracedRouter(dht)})
	srv, rec := tracedServer(t, h, 1, "", false)
	resp := get(t, srv.URL+"/routing/v1/peers/12D3KooWRBy97UB99e3J6hiPesre1MZeuNQvfan4gBziswrRJsNK", map[string]string{"Accept": "application/x-ndjson"})
	_, _ = io.Copy(io.Discard, resp.Body)

	require.Eventually(t, func() bool { return len(rec.Ended()) == 2 }, 5*time.Second, 10*time.Millisecond)
	spans := spansByName(rec.Ended())
	s := spans["needle router peers dht"]
	require.NotNil(t, s)
	assert.Equal(t, spans["needle GET /routing/v1/peers/{peer-id}"].SpanContext().SpanID(), s.Parent().SpanID())
	assert.Equal(t, int64(1), attrMap(s)[attrRouterRecords].AsInt64())
}

// Spans a library starts under an untraced context are not part of the
// request's trace and are not sampled.
func TestUntracedDropsLibrarySpans(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(newSampler(1)), sdktrace.WithSpanProcessor(rec))
	ctx, req := tp.Tracer("test").Start(context.Background(), "needle GET /routing/v1/providers/{cid}", traceapi.WithSpanKind(traceapi.SpanKindServer))
	require.True(t, req.IsRecording())

	_, lib := tp.Tracer("go-libp2p-kad-dht").Start(untraced(ctx), "IpfsDHT.FindProvidersAsync")
	assert.False(t, lib.IsRecording())
	lib.End()
	req.End()
	require.Len(t, rec.Ended(), 1)
}
