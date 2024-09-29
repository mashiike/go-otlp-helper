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
	"google.golang.org/protobuf/encoding/protojson"
)

func TestClient_GRPC_Traces(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Trace().HandleFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadTraces(ctx, req.GetResourceSpans())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_GRPC_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Metrics().HandleFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadMetrics(ctx, req.GetResourceMetrics())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_GRPC_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Logs().HandleFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadLogs(ctx, req.GetResourceLogs())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_HTTP_ProtoBuf_Traces(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Trace().HandleFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadTraces(ctx, req.GetResourceSpans())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_HTTP_JSON_Traces(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Trace().HandleFunc(func(ctx context.Context, request *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadTraces(ctx, req.GetResourceSpans())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_HTTP_ProtoBuf_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Metrics().HandleFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadMetrics(ctx, req.GetResourceMetrics())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_HTTP_JSON_Metrics(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Metrics().HandleFunc(func(ctx context.Context, request *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadMetrics(ctx, req.GetResourceMetrics())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_HTTP_ProtoBuf_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Logs().HandleFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadLogs(ctx, req.GetResourceLogs())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}

func TestClient_HTTP_JSON_Logs(t *testing.T) {
	mux := otlp.NewServerMux()
	var actual []byte
	mux.Logs().HandleFunc(func(ctx context.Context, request *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		var err error
		actual, err = protojson.Marshal(request)
		assert.NoError(t, err)
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
	err = protojson.Unmarshal(expected, &req)
	require.NoError(t, err)
	err = client.Start(ctx)
	require.NoError(t, err)
	defer client.Stop(ctx)
	err = client.UploadLogs(ctx, req.GetResourceLogs())
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(actual))
}
