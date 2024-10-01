package otlp_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mashiike/go-otlp-helper/otlp"
	"github.com/mashiike/go-otlp-helper/otlp/otlptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log"
	"google.golang.org/protobuf/proto"
)

type testContextKey string

func TestMux__HTTP_Trace(t *testing.T) {
	traceData, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	var expected otlp.TraceRequest
	require.NoError(t, otlp.UnmarshalJSON(traceData, &expected))
	mux := otlp.NewServerMux()
	handleCount := 0
	mux.Trace().HandleFunc(func(_ context.Context, req *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		assertEqualMessage(t, &expected, req)
		handleCount++
		return &otlp.TraceResponse{}, nil
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(traceData))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	t.Log(w.Body.String())
	require.Equal(t, 1, handleCount)
}

func TestMux__HTTP_Metrics(t *testing.T) {
	metricsData, err := os.ReadFile("testdata/metrics.json")
	require.NoError(t, err)

	var expected otlp.MetricsRequest
	require.NoError(t, otlp.UnmarshalJSON(metricsData, &expected))

	mux := otlp.NewServerMux()
	handleCount := 0
	mux.Metrics().HandleFunc(func(_ context.Context, req *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		assertEqualMessage(t, &expected, req)
		handleCount++
		return &otlp.MetricsResponse{}, nil
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(metricsData))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	t.Log(w.Body.String())
	require.Equal(t, 1, handleCount)
}

func TestMux__HTTP_Logs(t *testing.T) {
	logsData, err := os.ReadFile("testdata/logs.json")
	require.NoError(t, err)
	var expected otlp.LogsRequest
	require.NoError(t, otlp.UnmarshalJSON(logsData, &expected))

	mux := otlp.NewServerMux()
	handleCount := 0
	mux.Logs().HandleFunc(func(_ context.Context, req *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		assertEqualMessage(t, &expected, req)
		handleCount++
		return &otlp.LogsResponse{}, nil
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(logsData))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	t.Log(w.Body.String())
	require.Equal(t, 1, handleCount)
}

func TestServer__HTTP_Trace(t *testing.T) {
	mux := otlp.NewServerMux()
	traceCount := int32(0)
	mux.Trace().HandleFunc(
		func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
			atomic.AddInt32(&traceCount, 1)
			return &otlp.TraceResponse{}, nil
		},
	)
	var existsHeader atomic.Bool
	mux.Use(func(next otlp.ProtoHandlerFunc) otlp.ProtoHandlerFunc {
		return func(ctx context.Context, request proto.Message) (proto.Message, error) {
			headers, ok := otlp.HeadersFromContext(ctx)
			require.True(t, ok)
			if headers.Get("test") == "test" {
				existsHeader.Store(true)
			}
			return next(ctx, request)
		}
	})
	server := otlptest.NewHTTPServer(mux)
	defer server.Close()
	tracerProvider, err := server.Trace.Provider(
		otlptracehttp.WithHeaders(map[string]string{
			"test": "test",
		}),
	)
	require.NoError(t, err)

	ctx := context.Background()
	tracer := tracerProvider.Tracer("test")
	_, span := tracer.Start(ctx, "test")
	span.End()
	err = tracerProvider.ForceFlush(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, atomic.LoadInt32(&traceCount))
	require.True(t, existsHeader.Load())
}

func TestServer__HTTP_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	metricCount := int32(0)
	mux.Metrics().HandleFunc(
		func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
			atomic.AddInt32(&metricCount, 1)
			return &otlp.MetricsResponse{}, nil
		},
	)
	var existsHeader atomic.Bool
	mux.Use(func(next otlp.ProtoHandlerFunc) otlp.ProtoHandlerFunc {
		return func(ctx context.Context, request proto.Message) (proto.Message, error) {
			headers, ok := otlp.HeadersFromContext(ctx)
			require.True(t, ok)
			if headers.Get("test") == "test" {
				existsHeader.Store(true)
			}
			return next(ctx, request)
		}
	})
	server := otlptest.NewHTTPServer(mux)
	defer server.Close()

	meterProvider, err := server.Metrics.Provider(
		otlpmetrichttp.WithHeaders(map[string]string{
			"test": "test",
		}),
	)
	require.NoError(t, err)
	ctx := context.Background()
	meter := meterProvider.Meter("test")
	counter, err := meter.Int64Counter("test")
	require.NoError(t, err)
	counter.Add(ctx, 1)
	err = meterProvider.ForceFlush(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, atomic.LoadInt32(&metricCount))
	require.True(t, existsHeader.Load())
}

func TestServer__HTTP_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	logCount := int32(0)
	mux.Logs().HandleFunc(
		func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
			atomic.AddInt32(&logCount, 1)
			return &otlp.LogsResponse{}, nil
		},
	)
	var existsHeader atomic.Bool
	mux.Use(func(next otlp.ProtoHandlerFunc) otlp.ProtoHandlerFunc {
		return func(ctx context.Context, request proto.Message) (proto.Message, error) {
			headers, ok := otlp.HeadersFromContext(ctx)
			require.True(t, ok)
			if headers.Get("test") == "test" {
				existsHeader.Store(true)
			}
			return next(ctx, request)
		}
	})
	server := otlptest.NewHTTPServer(mux)
	defer server.Close()

	loggerProvider, err := server.Logs.Provider(
		otlploghttp.WithHeaders(map[string]string{
			"test": "test",
		}),
	)
	require.NoError(t, err)
	ctx := context.Background()
	logger := loggerProvider.Logger("test")
	var recored log.Record
	recored.SetTimestamp(time.Now())
	recored.SetBody(log.StringValue("test log"))
	recored.SetSeverity(log.SeverityInfo)
	recored.AddAttributes(
		log.KeyValue{
			Key:   "user",
			Value: log.StringValue("test user"),
		},
		log.KeyValue{
			Key:   "admin",
			Value: log.BoolValue(true),
		},
	)
	logger.Emit(ctx, recored)
	err = loggerProvider.ForceFlush(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, atomic.LoadInt32(&logCount))
	require.True(t, existsHeader.Load())
}

func TestServer__gRPC_Trace(t *testing.T) {
	mux := otlp.NewServerMux()
	traceCount := int32(0)
	mux.Trace().HandleFunc(
		func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
			lastMiddeleware, ok := ctx.Value(testContextKey("last_middeleware")).(string)
			require.True(t, ok)
			require.Equal(t, "inner", lastMiddeleware)
			atomic.AddInt32(&traceCount, 1)
			return &otlp.TraceResponse{}, nil
		},
	)
	mux.Trace().Use(func(next otlp.TraceHandler) otlp.TraceHandler {
		return otlp.TraceHandlerFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
			lastMiddeleware, ok := ctx.Value(testContextKey("last_middeleware")).(string)
			require.True(t, ok)
			require.Equal(t, "outer", lastMiddeleware)
			ctx = context.WithValue(ctx, testContextKey("last_middeleware"), "inner")
			return next.HandleTrace(ctx, request)
		})
	})
	var existsHeader atomic.Bool
	mux.Use(func(next otlp.ProtoHandlerFunc) otlp.ProtoHandlerFunc {
		return func(ctx context.Context, request proto.Message) (proto.Message, error) {
			headers, ok := otlp.HeadersFromContext(ctx)
			require.True(t, ok)
			if headers.Get("test") == "test" {
				existsHeader.Store(true)
			}
			ctx = context.WithValue(ctx, testContextKey("last_middeleware"), "outer")
			return next(ctx, request)
		}
	})
	server := otlptest.NewServer(mux)
	defer server.Close()

	tracerProvider, err := server.Trace.Provider(
		otlptracegrpc.WithHeaders(map[string]string{
			"test": "test",
		}),
	)
	require.NoError(t, err)

	ctx := context.Background()
	tracer := tracerProvider.Tracer("test")
	_, span := tracer.Start(ctx, "test")
	span.End()
	err = tracerProvider.ForceFlush(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, atomic.LoadInt32(&traceCount))
	require.True(t, existsHeader.Load())
}

func TestServer__gRPC_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	metricCount := int32(0)
	mux.Metrics().HandleFunc(
		func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
			lastMiddeleware, ok := ctx.Value(testContextKey("last_middeleware")).(string)
			require.True(t, ok)
			require.Equal(t, "inner", lastMiddeleware)
			atomic.AddInt32(&metricCount, 1)
			return &otlp.MetricsResponse{}, nil
		},
	)
	mux.Metrics().Use(func(next otlp.MetricsHandler) otlp.MetricsHandler {
		return otlp.MetricsHandlerFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
			lastMiddeleware, ok := ctx.Value(testContextKey("last_middeleware")).(string)
			require.True(t, ok)
			require.Equal(t, "outer", lastMiddeleware)
			ctx = context.WithValue(ctx, testContextKey("last_middeleware"), "inner")
			return next.HandleMetrics(ctx, request)
		})
	})
	var existsHeader atomic.Bool
	mux.Use(func(next otlp.ProtoHandlerFunc) otlp.ProtoHandlerFunc {
		return func(ctx context.Context, request proto.Message) (proto.Message, error) {
			headers, ok := otlp.HeadersFromContext(ctx)
			require.True(t, ok)
			if headers.Get("test") == "test" {
				existsHeader.Store(true)
			}
			ctx = context.WithValue(ctx, testContextKey("last_middeleware"), "outer")
			return next(ctx, request)
		}
	})
	server := otlptest.NewServer(mux)
	defer server.Close()

	meterProvider, err := server.Metrics.Provider(
		otlpmetricgrpc.WithHeaders(map[string]string{
			"test": "test",
		}),
	)
	require.NoError(t, err)
	ctx := context.Background()
	meter := meterProvider.Meter("test")
	counter, err := meter.Int64Counter("test")
	require.NoError(t, err)
	counter.Add(ctx, 1)
	err = meterProvider.ForceFlush(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, atomic.LoadInt32(&metricCount))
	require.True(t, existsHeader.Load())
}

func TestServer__gRPC_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	logCount := int32(0)
	mux.Logs().HandleFunc(
		func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
			lastMiddeleware, ok := ctx.Value(testContextKey("last_middeleware")).(string)
			require.True(t, ok)
			require.Equal(t, "inner", lastMiddeleware)
			atomic.AddInt32(&logCount, 1)
			return &otlp.LogsResponse{}, nil
		},
	)
	mux.Logs().Use(func(next otlp.LogsHandler) otlp.LogsHandler {
		return otlp.LogsHandlerFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
			lastMiddeleware, ok := ctx.Value(testContextKey("last_middeleware")).(string)
			require.True(t, ok)
			require.Equal(t, "outer", lastMiddeleware)
			ctx = context.WithValue(ctx, testContextKey("last_middeleware"), "inner")
			return next.HandleLogs(ctx, request)
		})
	})
	var existsHeader atomic.Bool
	mux.Use(func(next otlp.ProtoHandlerFunc) otlp.ProtoHandlerFunc {
		return func(ctx context.Context, request proto.Message) (proto.Message, error) {
			headers, ok := otlp.HeadersFromContext(ctx)
			require.True(t, ok)
			if headers.Get("test") == "test" {
				existsHeader.Store(true)
			}
			ctx = context.WithValue(ctx, testContextKey("last_middeleware"), "outer")
			return next(ctx, request)
		}
	})
	server := otlptest.NewServer(mux)
	defer server.Close()

	loggerProvider, err := server.Logs.Provider(
		otlploggrpc.WithHeaders(map[string]string{
			"test": "test",
		}),
	)
	require.NoError(t, err)
	ctx := context.Background()
	logger := loggerProvider.Logger("test")
	var recored log.Record
	recored.SetTimestamp(time.Now())
	recored.SetBody(log.StringValue("test log"))
	recored.SetSeverity(log.SeverityInfo)
	recored.AddAttributes(
		log.KeyValue{
			Key:   "user",
			Value: log.StringValue("test user"),
		},
		log.KeyValue{
			Key:   "admin",
			Value: log.BoolValue(true),
		},
	)
	logger.Emit(ctx, recored)
	err = loggerProvider.ForceFlush(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, atomic.LoadInt32(&logCount))
	require.True(t, existsHeader.Load())
}
