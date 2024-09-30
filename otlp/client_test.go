package otlp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mashiike/go-otlp-helper/otlp"
	"github.com/mashiike/go-otlp-helper/otlp/otlptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func assertEqualMessage[T proto.Message](t *testing.T, expected, actual T) {
	t.Helper()
	acutalJSON, err := otlp.MarshalJSON(actual)
	assert.NoError(t, err)
	expectedJSON, err := otlp.MarshalJSON(expected)
	assert.NoError(t, err)
	assert.JSONEq(t, string(expectedJSON), string(acutalJSON))
}

func TestClient_GRPC_Traces(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.TraceRequest
	mux.Trace().HandleFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.TraceResponse{}, nil
	})
	server := otlptest.NewServer(mux)
	defer server.Close()
	expected, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("grpc"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.TraceRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadTraces(ctx, req.GetResourceSpans())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_GRPC_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.MetricsRequest
	mux.Metrics().HandleFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.MetricsResponse{}, nil
	})
	server := otlptest.NewServer(mux)
	defer server.Close()
	expected, err := os.ReadFile("testdata/metrics.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("grpc"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.MetricsRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadMetrics(ctx, req.GetResourceMetrics())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_GRPC_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.LogsRequest
	mux.Logs().HandleFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.LogsResponse{}, nil
	})
	server := otlptest.NewServer(mux)
	defer server.Close()
	expected, err := os.ReadFile("testdata/logs.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("grpc"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.LogsRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadLogs(ctx, req.GetResourceLogs())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_HTTP_ProtoBuf_Traces(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.TraceRequest
	mux.Trace().HandleFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.TraceResponse{}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))
			assert.Equal(t, "/v1/traces", r.URL.String())
			mux.ServeHTTP(w, r)
		},
	))
	defer server.Close()
	expected, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("http/protobuf"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.TraceRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadTraces(ctx, req.GetResourceSpans())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_HTTP_JSON_Traces(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.TraceRequest
	mux.Trace().HandleFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.TraceResponse{}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "/v1/traces", r.URL.String())
			mux.ServeHTTP(w, r)
		},
	))
	defer server.Close()
	expected, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("http/json"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.TraceRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadTraces(ctx, req.GetResourceSpans())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_HTTP_ProtoBuf_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.MetricsRequest
	mux.Metrics().HandleFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.MetricsResponse{}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))
			assert.Equal(t, "/v1/metrics", r.URL.String())
			mux.ServeHTTP(w, r)
		},
	))
	defer server.Close()
	expected, err := os.ReadFile("testdata/metrics.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("http/protobuf"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.MetricsRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadMetrics(ctx, req.GetResourceMetrics())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_HTTP_JSON_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.MetricsRequest
	mux.Metrics().HandleFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.MetricsResponse{}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "/v1/metrics", r.URL.String())
			mux.ServeHTTP(w, r)
		},
	))
	defer server.Close()
	expected, err := os.ReadFile("testdata/metrics.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("http/json"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.MetricsRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadMetrics(ctx, req.GetResourceMetrics())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_HTTP_ProtoBuf_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.LogsRequest
	mux.Logs().HandleFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.LogsResponse{}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))
			assert.Equal(t, "/v1/logs", r.URL.String())
			mux.ServeHTTP(w, r)
		},
	))
	defer server.Close()
	expected, err := os.ReadFile("testdata/logs.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("http/protobuf"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.LogsRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadLogs(ctx, req.GetResourceLogs())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_HTTP_JSON_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual *otlp.LogsRequest
	mux.Logs().HandleFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		actual = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummy", headers.Get("Api-Key"))
		assert.True(t, strings.HasPrefix(headers.Get("User-Agent"), "test"))
		return &otlp.LogsResponse{}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "/v1/logs", r.URL.String())
			mux.ServeHTTP(w, r)
		},
	))
	defer server.Close()
	expected, err := os.ReadFile("testdata/logs.json")
	require.NoError(t, err)
	client, err := otlp.NewClient(
		server.URL,
		otlp.WithProtocol("http/json"),
		otlp.WithHeaders(map[string]string{"Api-Key": "dummy"}),
		otlp.WithUserAgent("test"),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var req otlp.LogsRequest
	err = otlp.UnmarshalJSON(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadLogs(ctx, req.GetResourceLogs())
	require.NoError(t, err)

	assertEqualMessage(t, &req, actual)
}

func TestClient_EnvOptions(t *testing.T) {
	mux := otlp.NewServerMux()
	var (
		actualTraces  *otlp.TraceRequest
		actualMetrics *otlp.MetricsRequest
		actualLogs    *otlp.LogsRequest
	)
	var actualTraceProtocol, actualMetricsProtocol, actualLogsProtocol string
	mux.Trace().HandleFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		actualTraces = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummyTraces", headers.Get("Api-Key"))
		assert.Equal(t, "fuga", headers.Get("Hoge"))
		actualTraceProtocol = headers.Get("Content-Type")
		return &otlp.TraceResponse{}, nil
	})
	mux.Metrics().HandleFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		actualMetrics = request
		headers, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummyMetrics", headers.Get("Api-Key"))
		assert.Equal(t, "fuga", headers.Get("Hoge"))
		actualMetricsProtocol = headers.Get("Content-Type")
		return &otlp.MetricsResponse{}, nil
	})
	mux.Logs().HandleFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		actualLogs = request
		header, ok := otlp.HeadersFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "dummyTraces", header.Get("Api-Key"))
		assert.Equal(t, "tora", header.Get("Hoge"))
		actualLogsProtocol = header.Get("Content-Type")
		return &otlp.LogsResponse{}, nil
	})
	server := otlptest.NewServer(mux)
	defer server.Close()
	expectedTraces, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	expectedMetrics, err := os.ReadFile("testdata/metrics.json")
	require.NoError(t, err)
	expectedLogs, err := os.ReadFile("testdata/logs.json")
	require.NoError(t, err)
	t.Setenv("OTLP_HEADERS", "Hoge=fuga")
	t.Setenv("OTLP_TRACES_HEADERS", "Api-Key=dummyTraces")
	t.Setenv("OTLP_METRICS_HEADERS", "Api-Key=dummyMetrics")
	t.Setenv("OTLP_LOGS_HEADERS", "Api-Key=dummyTraces,Hoge=tora")
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	grpcServer := otlptest.NewServer(mux)
	defer grpcServer.Close()
	t.Setenv("OTLP_PROTOCOL", "grpc")
	t.Setenv("OTLP_TRACES_ENDPOINT", httpServer.URL+"/v1/traces")
	t.Setenv("OTLP_TRACES_PROTOCOL", "http/json")

	client, err := otlp.NewClient(grpcServer.URL, otlp.DefaultClientOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var reqTraces otlp.TraceRequest
	err = otlp.UnmarshalJSON(expectedTraces, &reqTraces)
	require.NoError(t, err)
	var reqMetrics otlp.MetricsRequest
	err = otlp.UnmarshalJSON(expectedMetrics, &reqMetrics)
	require.NoError(t, err)
	var reqLogs otlp.LogsRequest
	err = otlp.UnmarshalJSON(expectedLogs, &reqLogs)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadTraces(ctx, reqTraces.GetResourceSpans())
	require.NoError(t, err)
	err = client.UploadMetrics(ctx, reqMetrics.GetResourceMetrics())
	require.NoError(t, err)
	err = client.UploadLogs(ctx, reqLogs.GetResourceLogs())
	require.NoError(t, err)

	assertEqualMessage(t, &reqTraces, actualTraces)
	assertEqualMessage(t, &reqMetrics, actualMetrics)
	assertEqualMessage(t, &reqLogs, actualLogs)
	assert.Equal(t, "application/json", actualTraceProtocol)
	assert.Equal(t, "application/grpc", actualMetricsProtocol)
	assert.Equal(t, "application/grpc", actualLogsProtocol)
}
