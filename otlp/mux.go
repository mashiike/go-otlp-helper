package otlp

import (
	"context"
	"net/http"
	"sync"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type (
	ProtoHandlerFunc func(context.Context, proto.Message) (proto.Message, error)
	MiddlewareFunc   func(next ProtoHandlerFunc) ProtoHandlerFunc
)

type ServerMux struct {
	mu          sync.RWMutex
	httpMux     *http.ServeMux
	trace       *traceEntry
	metrics     *metricsEntry
	logs        *logsEntry
	middlewares []MiddlewareFunc
}

var DefaultServerMux = NewServerMux()

func NewServerMux() *ServerMux {
	return &ServerMux{
		httpMux:     http.NewServeMux(),
		middlewares: make([]MiddlewareFunc, 0),
	}
}

func (mux *ServerMux) Use(m ...MiddlewareFunc) *ServerMux {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	mux.middlewares = append(mux.middlewares, m...)
	return mux
}

func (mux *ServerMux) chainedMiddleware() MiddlewareFunc {
	mux.mu.RLock()
	defer mux.mu.RUnlock()
	if len(mux.middlewares) == 0 {
		return MiddlewareFunc(func(next ProtoHandlerFunc) ProtoHandlerFunc {
			return next
		})
	}
	chained := mux.middlewares[len(mux.middlewares)-1]
	for i := len(mux.middlewares) - 2; i >= 0; i-- {
		chained = func(next, mw MiddlewareFunc) MiddlewareFunc {
			return MiddlewareFunc(func(h ProtoHandlerFunc) ProtoHandlerFunc {
				return mw(next(h))
			})
		}(chained, mux.middlewares[i])
	}
	return chained
}

func (mux *ServerMux) Register(reg grpc.ServiceRegistrar) {
	if trace, ok := mux.getTraceEntry(); ok {
		tracepb.RegisterTraceServiceServer(reg, trace)
	}
	if metrics, ok := mux.getMetricsEntry(); ok {
		metricspb.RegisterMetricsServiceServer(reg, metrics)
	}
	if logs, ok := mux.getLogsEntry(); ok {
		logspb.RegisterLogsServiceServer(reg, logs)
	}
}

func (mux *ServerMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	md := make(metadata.MD, len(r.Header))
	for k, v := range r.Header {
		md[k] = v
	}
	r = r.WithContext(metadata.NewIncomingContext(r.Context(), md))
	if handler, pattern := mux.httpMux.Handler(r); pattern != "" {
		handler.ServeHTTP(w, r)
		return
	}
	st := status.New(codes.NotFound, "no handler registered for path")
	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		errorProto(w, st)
	case "application/json":
		errorJSON(w, st)
	default:
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	}
}

type (
	TraceRequest  = tracepb.ExportTraceServiceRequest
	TraceResponse = tracepb.ExportTraceServiceResponse
)

// TraceHandler is an interface that must be implemented by the user of the Server to handle trace requests.
type TraceHandler interface {
	HandleTrace(ctx context.Context, request *TraceRequest) (*TraceResponse, error)
}

// TraceHandlerFunc is a function type that implements the TraceHandler interface.
type TraceHandlerFunc func(ctx context.Context, request *TraceRequest) (*TraceResponse, error)

func (f TraceHandlerFunc) HandleTrace(ctx context.Context, request *TraceRequest) (*TraceResponse, error) {
	return f(ctx, request)
}

type TraceMiddlewareFunc func(next TraceHandler) TraceHandler

type TraceEntry interface {
	Handle(handler TraceHandler)
	HandleFunc(handler func(ctx context.Context, request *TraceRequest) (*TraceResponse, error))
	Use(m ...TraceMiddlewareFunc) TraceEntry
}

type traceEntry struct {
	mux *ServerMux
	tracepb.UnimplementedTraceServiceServer
	mu          sync.RWMutex
	middlewares []TraceMiddlewareFunc
	h           TraceHandler
	ph          http.Handler
}

func (mux *ServerMux) getTraceEntry() (*traceEntry, bool) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()
	return mux.trace, mux.trace != nil
}

func (mux *ServerMux) newTraceEntry() *traceEntry {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.trace == nil {
		mux.trace = &traceEntry{
			mux: mux,
		}
		mux.trace.ph = newProxyHandler(
			func(_ context.Context) *TraceRequest {
				return &TraceRequest{}
			},
			mux.trace.Export,
		)
		mux.httpMux.Handle("/v1/traces", mux.trace)
	}
	return mux.trace
}

func (e *traceEntry) Use(m ...TraceMiddlewareFunc) TraceEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.middlewares = append(e.middlewares, m...)
	return e
}

func (e *traceEntry) Handle(handler TraceHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.h = handler
}

func (e *traceEntry) HandleFunc(handler func(ctx context.Context, request *TraceRequest) (*TraceResponse, error)) {
	e.Handle(TraceHandlerFunc(handler))
}

func (e *traceEntry) getHandler() (TraceHandler, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.h == nil {
		return nil, false
	}
	wrapped := e.h
	for i := len(e.middlewares) - 1; i >= 0; i-- {
		wrapped = e.middlewares[i](wrapped)
	}
	return wrapped, true
}

func (e *traceEntry) Export(ctx context.Context, req *TraceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	base, ok := e.getHandler()
	if !ok {
		return e.UnimplementedTraceServiceServer.Export(ctx, req)
	}
	h := e.mux.chainedMiddleware()(func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return base.HandleTrace(ctx, req.(*TraceRequest))
	})
	resp, err := h(ctx, req)
	if err != nil {
		return nil, err
	}
	if traceResp, ok := resp.(*TraceResponse); ok {
		return traceResp, nil
	}
	return nil, status.Error(codes.Internal, "unexpected response type")
}

func (e *traceEntry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.ph.ServeHTTP(w, r)
}

func (mux *ServerMux) Trace() TraceEntry {
	if trace, ok := mux.getTraceEntry(); ok {
		return trace
	}
	return mux.newTraceEntry()
}

type (
	MetricsRequest  = metricspb.ExportMetricsServiceRequest
	MetricsResponse = metricspb.ExportMetricsServiceResponse
)

type MetricsHandler interface {
	HandleMetrics(ctx context.Context, request *MetricsRequest) (*MetricsResponse, error)
}

type MetricsHandlerFunc func(ctx context.Context, request *MetricsRequest) (*MetricsResponse, error)

func (f MetricsHandlerFunc) HandleMetrics(ctx context.Context, request *MetricsRequest) (*MetricsResponse, error) {
	return f(ctx, request)
}

type MetricsMiddlewareFunc func(next MetricsHandler) MetricsHandler

type MetricsEntry interface {
	Handle(handler MetricsHandler)
	HandleFunc(handler func(ctx context.Context, request *MetricsRequest) (*MetricsResponse, error))
	Use(m ...MetricsMiddlewareFunc) MetricsEntry
}

type metricsEntry struct {
	mux *ServerMux
	metricspb.UnimplementedMetricsServiceServer
	mu sync.RWMutex
	h  MetricsHandler
	ph http.Handler

	middlewares []MetricsMiddlewareFunc
}

func (mux *ServerMux) getMetricsEntry() (*metricsEntry, bool) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()
	return mux.metrics, mux.metrics != nil
}

func (mux *ServerMux) newMetricsEntry() *metricsEntry {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.metrics == nil {
		mux.metrics = &metricsEntry{
			mux: mux,
		}
		mux.metrics.ph = newProxyHandler(
			func(_ context.Context) *MetricsRequest {
				return &MetricsRequest{}
			},
			mux.metrics.Export,
		)
		mux.httpMux.Handle("/v1/metrics", mux.metrics)
	}
	return mux.metrics
}

func (e *metricsEntry) Use(m ...MetricsMiddlewareFunc) MetricsEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.middlewares = append(e.middlewares, m...)
	return e
}

func (e *metricsEntry) Handle(handler MetricsHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.h = handler
}

func (e *metricsEntry) HandleFunc(handler func(ctx context.Context, request *MetricsRequest) (*MetricsResponse, error)) {
	e.Handle(MetricsHandlerFunc(handler))
}

func (e *metricsEntry) getHandler() (MetricsHandler, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.h == nil {
		return nil, false
	}
	wrapped := e.h
	for i := len(e.middlewares) - 1; i >= 0; i-- {
		wrapped = e.middlewares[i](wrapped)
	}
	return wrapped, true
}

func (e *metricsEntry) Export(ctx context.Context, req *MetricsRequest) (*MetricsResponse, error) {
	base, ok := e.getHandler()
	if !ok {
		return e.UnimplementedMetricsServiceServer.Export(ctx, req)
	}
	h := e.mux.chainedMiddleware()(func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return base.HandleMetrics(ctx, req.(*MetricsRequest))
	})
	resp, err := h(ctx, req)
	if err != nil {
		return nil, err
	}
	if metricsResp, ok := resp.(*MetricsResponse); ok {
		return metricsResp, nil
	}
	return nil, status.Error(codes.Internal, "unexpected response type")
}

func (e *metricsEntry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.ph.ServeHTTP(w, r)
}

func (mux *ServerMux) Metrics() MetricsEntry {
	if metrics, ok := mux.getMetricsEntry(); ok {
		return metrics
	}
	return mux.newMetricsEntry()
}

type (
	LogsRequest  = logspb.ExportLogsServiceRequest
	LogsResponse = logspb.ExportLogsServiceResponse
)

type LogsHandler interface {
	HandleLogs(ctx context.Context, request *LogsRequest) (*LogsResponse, error)
}

type LogsHandlerFunc func(ctx context.Context, request *LogsRequest) (*LogsResponse, error)

func (f LogsHandlerFunc) HandleLogs(ctx context.Context, request *LogsRequest) (*LogsResponse, error) {
	return f(ctx, request)
}

type LogsMiddlewareFunc func(next LogsHandler) LogsHandler

type LogsEntry interface {
	Handle(handler LogsHandler)
	HandleFunc(handler func(ctx context.Context, request *LogsRequest) (*LogsResponse, error))
	Use(m ...LogsMiddlewareFunc) LogsEntry
}

type logsEntry struct {
	mux *ServerMux
	logspb.UnimplementedLogsServiceServer
	mu sync.RWMutex
	h  LogsHandler
	ph http.Handler

	middlewares []LogsMiddlewareFunc
}

func (mux *ServerMux) getLogsEntry() (*logsEntry, bool) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()
	return mux.logs, mux.logs != nil
}

func (mux *ServerMux) newLogsEntry() *logsEntry {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.logs == nil {
		mux.logs = &logsEntry{
			mux: mux,
		}
		mux.logs.ph = newProxyHandler(
			func(_ context.Context) *LogsRequest {
				return &logspb.ExportLogsServiceRequest{}
			},
			mux.logs.Export,
		)
		mux.httpMux.Handle("/v1/logs", mux.logs)
	}
	return mux.logs
}

func (e *logsEntry) Use(m ...LogsMiddlewareFunc) LogsEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.middlewares = append(e.middlewares, m...)
	return e
}

func (e *logsEntry) Handle(handler LogsHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.h = handler
}

func (e *logsEntry) HandleFunc(handler func(ctx context.Context, request *LogsRequest) (*LogsResponse, error)) {
	e.Handle(LogsHandlerFunc(handler))
}

func (e *logsEntry) getHandler() (LogsHandler, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.h == nil {
		return nil, false
	}
	wrapped := e.h
	for i := len(e.middlewares) - 1; i >= 0; i-- {
		wrapped = e.middlewares[i](wrapped)
	}
	return wrapped, true
}

func (e *logsEntry) Export(ctx context.Context, req *LogsRequest) (*LogsResponse, error) {
	base, ok := e.getHandler()
	if !ok {
		return e.UnimplementedLogsServiceServer.Export(ctx, req)
	}
	h := e.mux.chainedMiddleware()(func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return base.HandleLogs(ctx, req.(*LogsRequest))
	})
	resp, err := h(ctx, req)
	if err != nil {
		return nil, err
	}
	if logsResp, ok := resp.(*LogsResponse); ok {
		return logsResp, nil
	}
	return nil, status.Error(codes.Internal, "unexpected response type")
}

func (e *logsEntry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.ph.ServeHTTP(w, r)
}

func (mux *ServerMux) Logs() LogsEntry {
	if logs, ok := mux.getLogsEntry(); ok {
		return logs
	}
	return mux.newLogsEntry()
}
