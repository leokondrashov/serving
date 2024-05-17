package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httputil"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	netheader "knative.dev/networking/pkg/http/header"
	netproxy "knative.dev/networking/pkg/http/proxy"
	"knative.dev/pkg/logging/logkey"
	pkghandler "knative.dev/pkg/network/handlers"
	tracingconfig "knative.dev/pkg/tracing/config"
	"knative.dev/pkg/tracing/propagation/tracecontextb3"
	"knative.dev/serving/pkg/activator"
	activatorconfig "knative.dev/serving/pkg/activator/config"
	pkghttp "knative.dev/serving/pkg/http"
	"knative.dev/serving/pkg/queue"
	"knative.dev/serving/pkg/reconciler/serverlessservice/resources/names"
)

// activationHandler will wait for an active endpoint for a revision
// to be available before proxying the request
type synchronousHandler struct {
	transport        http.RoundTripper
	tracingTransport http.RoundTripper
	usePassthroughLb bool
	throttler        Throttler
	bufferPool       httputil.BufferPool
	logger           *zap.SugaredLogger
	tls              bool
	cr               *ConcurrencyReporter
}

func NewSynchronousHandler(_ context.Context, t Throttler, transport http.RoundTripper, usePassthroughLb bool, logger *zap.SugaredLogger, tlsEnabled bool, cr *ConcurrencyReporter) http.Handler {
	return &synchronousHandler{
		transport: transport,
		tracingTransport: &ochttp.Transport{
			Base:        transport,
			Propagation: tracecontextb3.TraceContextB3Egress,
		},
		usePassthroughLb: usePassthroughLb,
		throttler:        t,
		bufferPool:       netproxy.NewBufferPool(),
		logger:           logger,
		tls:              tlsEnabled,
		cr:               cr,
	}
}

func (a *synchronousHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	config := activatorconfig.FromContext(r.Context())
	tracingEnabled := config.Tracing.Backend != tracingconfig.None

	tryContext, trySpan := r.Context(), (*trace.Span)(nil)
	if tracingEnabled {
		tryContext, trySpan = trace.StartSpan(r.Context(), "throttler_try")
	}

	revID := RevIDFrom(r.Context())
	if err := a.throttler.Try(tryContext, revID, func(dest string) error {
		trySpan.End()

		proxyCtx, proxySpan := r.Context(), (*trace.Span)(nil)
		if tracingEnabled {
			proxyCtx, proxySpan = trace.StartSpan(r.Context(), "activator_proxy")
		}
		a.proxyRequest(revID, w, r.WithContext(proxyCtx), dest, tracingEnabled, a.usePassthroughLb)
		proxySpan.End()

		return nil
	}); err != nil {
		// Set error on our capacity waiting span and end it.
		trySpan.Annotate([]trace.Attribute{trace.StringAttribute("activator.throttler.error", err.Error())}, "ThrottlerTry")
		trySpan.End()

		a.logger.Errorw("Throttler try error", zap.String(logkey.Key, revID.String()), zap.Error(err))

		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, queue.ErrRequestQueueFull) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func (a *synchronousHandler) proxyRequest(revID types.NamespacedName, w http.ResponseWriter,
	r *http.Request, target string, tracingEnabled bool, usePassthroughLb bool) {
	netheader.RewriteHostIn(r)
	r.Header.Set(netheader.ProxyKey, activator.Name)

	// Set up the reverse proxy.
	hostOverride := pkghttp.NoHostOverride
	if usePassthroughLb {
		hostOverride = names.PrivateService(revID.Name) + "." + revID.Namespace
	}

	var proxy *httputil.ReverseProxy
	if a.tls {
		proxy = pkghttp.NewHeaderPruningReverseProxy(useSecurePort(target), hostOverride, activator.RevisionHeaders, true /* uss HTTPS */)
	} else {
		proxy = pkghttp.NewHeaderPruningReverseProxy(target, hostOverride, activator.RevisionHeaders, false /* use HTTPS */)
	}

	proxy.BufferPool = a.bufferPool
	proxy.Transport = a.transport
	if tracingEnabled {
		proxy.Transport = a.tracingTransport
	}
	proxy.FlushInterval = netproxy.FlushInterval
	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		pkghandler.Error(a.logger.With(zap.String(logkey.Key, revID.String())))(w, req, err)
	}

	proxy.ServeHTTP(w, r)
}
