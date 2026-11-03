package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/ipfs/boxo/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	traceapi "go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope of the spans needle starts itself.
const tracerName = "github.com/ipni/needle"

// setupTracing sets up tracing from the OTEL_* environment variables and
// returns the TracerProvider, which the caller shuts down to flush spans.
func setupTracing(ctx context.Context, traceFraction float64, listenAddress string) (*trace.TracerProvider, error) {
	tp, exporting, err := newTracerProvider(ctx, traceFraction, listenAddress)
	if err != nil {
		return nil, err
	}
	if exporting {
		fmt.Printf("Tracing: exporting spans, NEEDLE_SAMPLING_FRACTION = %g\n", traceFraction)
	}

	// Sets the default trace provider for this process. If this is not done, tracing
	// will not be enabled. Please note that this will apply to the entire process
	// as it is set as the default tracer, as per OTel recommendations.
	otel.SetTracerProvider(tp)

	// Configures the default propagators used by the Open Telemetry library. By
	// using autoprop.NewTextMapPropagator, we ensure the value of the environmental
	// variable OTEL_PROPAGATORS is respected, if set. By default, Trace Context
	// and Baggage are used. More details on:
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/sdk-environment-variables.md
	otel.SetTextMapPropagator(autoprop.NewTextMapPropagator())

	return tp, nil
}

// newTracerProvider creates and configures a TracerProvider. exporting is
// false when no exporter is configured, in which case nothing is sampled, so
// the spans needle would otherwise build are never recorded.
func newTracerProvider(ctx context.Context, traceFraction float64, listenAddress string) (tp *trace.TracerProvider, exporting bool, err error) {
	exporters, err := newSpanExporters(ctx)
	if err != nil {
		return nil, false, err
	}

	options := []trace.TracerProviderOption{}
	for _, exporter := range exporters {
		options = append(options, trace.WithBatcher(exporter))
	}

	r, err := newResource(ctx, listenAddress)
	if err != nil {
		return nil, false, err
	}

	sampler := newSampler(traceFraction)
	if len(exporters) == 0 {
		sampler = trace.NeverSample()
	}

	options = append(options, trace.WithResource(r), trace.WithSampler(sampler))
	return trace.NewTracerProvider(options...), len(exporters) > 0, nil
}

// newSpanExporters builds the exporters boxo's tracing package reads from
// OTEL_TRACES_EXPORTER. That variable defaults to otlp in the OpenTelemetry
// spec but to nothing in boxo, so setting only OTEL_EXPORTER_OTLP_ENDPOINT
// would silently export nothing. An OTLP endpoint with no exporter named is
// taken to mean otlp.
func newSpanExporters(ctx context.Context) ([]trace.SpanExporter, error) {
	if os.Getenv("OTEL_TRACES_EXPORTER") == "" &&
		(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "") {
		if err := os.Setenv("OTEL_TRACES_EXPORTER", "otlp"); err != nil {
			return nil, err
		}
	}
	return tracing.NewSpanExporters(ctx)
}

// newResource describes this process. needle's own values come first so
// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES, read last, override them.
func newResource(ctx context.Context, listenAddress string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(name),
		semconv.ServiceVersion(version),
	}
	if id := serviceInstanceID(listenAddress); id != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(id))
	}
	r, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
	)
	// A partial resource is still worth exporting with; only a resource that
	// could not be built at all is fatal.
	if err != nil && r == nil {
		return nil, err
	}
	if err != nil {
		logger.Warnw("building tracing resource", "err", err)
	}
	return r, nil
}

// serviceInstanceID tells needle processes apart: the host, plus the API port,
// since instances that share a host (and its network) must listen on different
// ports. Empty when neither is known.
func serviceInstanceID(listenAddress string) string {
	host, _ := os.Hostname()
	_, port, _ := net.SplitHostPort(listenAddress)
	switch {
	case host != "" && port != "":
		return host + ":" + port
	default:
		return host
	}
}

// newSampler samples a request whose caller sent a sampled traceparent, and
// NEEDLE_SAMPLING_FRACTION of the requests that arrive without one. Every span
// below a request follows the request's decision.
//
// A span with no parent that is not a request is dropped: libp2p and the DHT
// start spans of their own for background work (the cached address book's
// lookups, the accelerated DHT's crawl), and needle traces requests, not
// those.
func newSampler(traceFraction float64) trace.Sampler {
	return trace.ParentBased(requestSampler{ratio: trace.TraceIDRatioBased(traceFraction)})
}

// requestSampler is the root sampler: it only ever sees spans without a
// parent, and samples the HTTP server span of a request at ratio.
type requestSampler struct {
	ratio trace.Sampler
}

func (s requestSampler) ShouldSample(p trace.SamplingParameters) trace.SamplingResult {
	if p.Kind != traceapi.SpanKindServer {
		return trace.SamplingResult{
			Decision:   trace.Drop,
			Tracestate: traceapi.SpanContextFromContext(p.ParentContext).TraceState(),
		}
	}
	return s.ratio.ShouldSample(p)
}

func (s requestSampler) Description() string {
	return "NeedleRequestSampler{" + s.ratio.Description() + "}"
}

// routingRoutes maps a /routing/v1 path to the route boxo serves it under, so
// the request span is named after the endpoint and never the CID, peer ID or
// IPNS name in the path. These are boxo's own route templates.
var routingRoutes = []struct{ prefix, route string }{
	{"/routing/v1/providers/", "/routing/v1/providers/{cid}"},
	{"/routing/v1/peers/", "/routing/v1/peers/{peer-id}"},
	{"/routing/v1/ipns/", "/routing/v1/ipns/{cid}"},
	{"/routing/v1/dht/closest/peers/", "/routing/v1/dht/closest/peers/{key}"},
}

// routingRoute is the route template for path, or "" for a path no handler
// serves.
func routingRoute(path string) string {
	// PUT /routing/v1/providers (the deprecated provide endpoint) has no key.
	if strings.TrimSuffix(path, "/") == "/routing/v1/providers" {
		return "/routing/v1/providers"
	}
	for _, r := range routingRoutes {
		if rest, ok := strings.CutPrefix(path, r.prefix); ok && rest != "" {
			return r.route
		}
	}
	return ""
}

// knownMethods are the methods a span name may carry. Anything else is named
// HTTP, as the semantic conventions do, so a client cannot mint span names.
var knownMethods = map[string]struct{}{
	http.MethodGet: {}, http.MethodHead: {}, http.MethodPost: {}, http.MethodPut: {},
	http.MethodDelete: {}, http.MethodOptions: {}, http.MethodPatch: {},
	http.MethodConnect: {}, http.MethodTrace: {},
}

// requestSpanName names a request span "needle GET /routing/v1/providers/{cid}":
// low cardinality, and distinct per endpoint so span metrics split by it.
func requestSpanName(_ string, r *http.Request) string {
	method := r.Method
	if _, ok := knownMethods[method]; !ok {
		method = "HTTP"
	}
	if route := routingRoute(r.URL.Path); route != "" {
		return name + " " + method + " " + route
	}
	return name + " " + method
}

// withTracing starts a server span for every /routing/v1 request.
//
// The span continues the caller's trace only when the caller is trusted to
// start one: with trustTraceparent (needle sits behind a proxy that strips
// client traceparent headers and sends its own), or when the request carries
// an Authorization header equal to authToken. Any other request starts a new
// trace, sampled at NEEDLE_SAMPLING_FRACTION, and keeps the traceparent it sent
// only as a span link. That stops an arbitrary client from forcing its
// requests to be sampled, and so from deciding how much needle exports.
// opts are extra otelhttp options, which tests use to supply their own tracer
// provider and propagator.
func withTracing(next http.Handler, authToken string, trustTraceparent bool, opts ...otelhttp.Option) http.Handler {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := traceapi.SpanFromContext(r.Context())
		if !span.IsRecording() {
			next.ServeHTTP(w, r)
			return
		}
		if route := routingRoute(r.URL.Path); route != "" {
			span.SetAttributes(semconv.HTTPRoute(route))
		}
		ctx, counters := contextWithSpanCounters(r.Context())
		next.ServeHTTP(w, r.WithContext(ctx))
		span.SetAttributes(counters.attributes()...)
	})

	opts = append([]otelhttp.Option{
		otelhttp.WithSpanNameFormatter(requestSpanName),
		otelhttp.WithPublicEndpointFn(func(r *http.Request) bool {
			if trustTraceparent {
				return false
			}
			return authToken == "" || r.Header.Get("Authorization") != authToken
		}),
	}, opts...)
	return otelhttp.NewHandler(inner, name, opts...)
}

// Attributes needle sets on its spans.
const (
	attrRouterName      = attribute.Key("needle.router.name")
	attrRouterOp        = attribute.Key("needle.router.op")
	attrRouterRecords   = attribute.Key("needle.router.records")
	attrRouterExclusive = attribute.Key("needle.router.exclusive_records")
	attrRouterErrors    = attribute.Key("needle.router.errors")
	attrRouterFirstMs   = attribute.Key("needle.router.first_record_ms")
	attrRouterOutcome   = attribute.Key("needle.router.outcome")
	attrRouterFound     = attribute.Key("needle.router.found")

	attrRoutingOutcome    = attribute.Key("needle.routing.outcome")
	attrRoutingRouters    = attribute.Key("needle.routing.routers")
	attrRoutingLastRouter = attribute.Key("needle.routing.last_router")
	attrRoutingTailMs     = attribute.Key("needle.routing.tail_ms")

	attrAddrBookHits       = attribute.Key("needle.addr_book.hits")
	attrAddrBookMisses     = attribute.Key("needle.addr_book.misses")
	attrAddrBookUnused     = attribute.Key("needle.addr_book.unused")
	attrAddrBookNegative   = attribute.Key("needle.addr_book.negative_hits")
	attrAddrBookDispatched = attribute.Key("needle.addr_book.lookups_dispatched")
	attrAddrBookJoined     = attribute.Key("needle.addr_book.lookups_joined")

	attrDNSAddrCacheHits = attribute.Key("needle.dnsaddr.cache_hits")
	attrDNSAddrLookups   = attribute.Key("needle.dnsaddr.lookups")
	attrDNSAddrThrottled = attribute.Key("needle.dnsaddr.throttled")
	attrDNSAddrResult    = attribute.Key("needle.dnsaddr.result")
	attrDNSAddrAddrs     = attribute.Key("needle.dnsaddr.addrs")
	attrDNSAddrShared    = attribute.Key("needle.dnsaddr.shared")
)

// The outcomes for needle.router.outcome. exhausted and cut are the
// router-metric reasons of the same name. The metrics' cancelled is split
// three ways here, because which one it was is what a trace is read for:
// timeout when the routing timeout ended the request, client_cancelled when
// the client went away, and closed when needle stopped reading - the records
// limit was reached, or another router already answered.
const (
	routerOutcomeTimeout         = "timeout"
	routerOutcomeClientCancelled = "client_cancelled"
	routerOutcomeClosed          = "closed"
	routerOutcomeError           = "error"
)

// routerOutcome refines a router-metric finish reason with why the request
// context ended. reqCtx is the context the request handed to the routers,
// which carries the routing timeout and ends when the client goes away.
func routerOutcome(reason string, reqCtx context.Context) string {
	if reason != routerDoneCancelled {
		return reason
	}
	switch err := reqCtx.Err(); {
	case errors.Is(err, context.DeadlineExceeded):
		return routerOutcomeTimeout
	case err != nil:
		return routerOutcomeClientCancelled
	default:
		return routerOutcomeClosed
	}
}

// startRouterSpan starts the span for one router's part of a request, named
// "needle router <op> <router>" so span metrics give per-router latency. It
// returns a non-recording span, and no counters, when the request is not
// sampled. The span comes from the request span's provider, so a router call
// outside a traced request never records anything.
func startRouterSpan(ctx context.Context, op, router string) (context.Context, traceapi.Span, *spanCounters) {
	parent := traceapi.SpanFromContext(ctx)
	if !parent.IsRecording() {
		return ctx, traceapi.SpanFromContext(context.Background()), nil
	}
	ctx, span := parent.TracerProvider().Tracer(tracerName).Start(ctx, name+" router "+op+" "+router,
		traceapi.WithSpanKind(traceapi.SpanKindInternal),
		traceapi.WithAttributes(attrRouterName.String(router), attrRouterOp.String(op)),
	)
	ctx, counters := contextWithSpanCounters(ctx)
	return ctx, span, counters
}

// untraced detaches ctx from the trace it is in. The DHT starts spans of its
// own under the context it is given, one for every peer a lookup queries, so a
// single provider lookup adds a few hundred spans to its request's trace.
// Under a detached context they start without a parent and the sampler drops
// them. The router span needle starts around the call already measures it,
// which is what a trace is read for.
func untraced(ctx context.Context) context.Context {
	return traceapi.ContextWithSpanContext(ctx, traceapi.SpanContext{})
}

// spanCounters accumulates counts for the span that owns it, which sets them
// as attributes when it ends. Code that has no handle on the span - the cached
// address book, /dnsaddr resolution - finds the counters for the nearest span
// through the context. A nil *spanCounters (the request is not sampled)
// ignores every add, so callers need no check of their own.
type spanCounters struct {
	mu     sync.Mutex
	counts map[attribute.Key]int64
}

type spanCountersKey struct{}

func contextWithSpanCounters(ctx context.Context) (context.Context, *spanCounters) {
	c := &spanCounters{counts: make(map[attribute.Key]int64)}
	return context.WithValue(ctx, spanCountersKey{}, c), c
}

func spanCountersFromContext(ctx context.Context) *spanCounters {
	c, _ := ctx.Value(spanCountersKey{}).(*spanCounters)
	return c
}

func (c *spanCounters) add(key attribute.Key, n int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.counts[key] += n
	c.mu.Unlock()
}

// attributes returns the counts, in key order so a span reads the same way
// every time.
func (c *spanCounters) attributes() []attribute.KeyValue {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := slices.Sorted(maps.Keys(c.counts))
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, k.Int64(c.counts[k]))
	}
	return attrs
}
