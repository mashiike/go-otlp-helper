# go-otel-server

OpenTelemetry Collector Server Utils

## Overview

`go-otel-server` is a Go library that provides utilities for OpenTelemetry Collector servers. This library makes it easy to collect and process traces, metrics, and logs.

## Installation

You can install this library using the following command:

```sh
go get github.com/mashiike/go-otel-server/otlp 
```

## Usage

### simple grpc server example:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/mashiike/go-otel-server/otlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	slog.SetDefault(
		slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	)
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		slog.Error("API_KEY is required")
		os.Exit(1)
	}
	mux := otlp.NewServerMux()
	enc := func(ctx context.Context, msg proto.Message) {
		bs, err := protojson.Marshal(msg)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal proto message", "msg", err)
			return
		}
		fmt.Fprint(os.Stdout, string(bs))
	}
	mux.Trace().HandleFunc(func(ctx context.Context, req *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		enc(ctx, req)
		return &otlp.TraceResponse{}, nil
	})
	mux.Metrics().HandleFunc(func(ctx context.Context, req *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		enc(ctx, req)
		return &otlp.MetricsResponse{}, nil
	})
	mux.Logs().HandleFunc(func(ctx context.Context, req *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		enc(ctx, req)
		return &otlp.LogsResponse{}, nil
	})
	mux.Use(func(next otlp.ProtoHandlerFunc) otlp.ProtoHandlerFunc {
		return func(ctx context.Context, req proto.Message) (proto.Message, error) {
			headers, ok := otlp.HeadersFromContext(ctx)
			if !ok {
				return nil, status.Error(codes.Unauthenticated, "missing Api-Key")
			}
			if headers.Get("Api-Key") != apiKey {
				return nil, status.Error(codes.PermissionDenied, "invalid Api-Key")
			}
			return next(ctx, req)
		}
	})
	server := grpc.NewServer()
	mux.Register(server)
	lis, err := net.Listen("tcp", ":4317")
	if err != nil {
		slog.Error("failed to listen", "err", err)
		os.Exit(1)
	}
	defer lis.Close()
	if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		slog.Error("failed to serve", "err", err)
		os.Exit(1)
	}

}
```

### http server for Lambda Function example:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/fujiwara/ridge"
	"github.com/mashiike/go-otel-server/otlp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	slog.SetDefault(
		slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	)
	mux := otlp.NewServerMux()
	enc := func(ctx context.Context, msg proto.Message) {
		bs, err := protojson.Marshal(msg)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal proto message", "msg", err)
			return
		}
		fmt.Fprint(os.Stdout, string(bs))
	}
	mux.Trace().HandleFunc(func(ctx context.Context, req *otlp.TraceRequest) (*otlp.TraceResponse, error) {
		enc(ctx, req)
		return &otlp.TraceResponse{}, nil
	})
	mux.Metrics().HandleFunc(func(ctx context.Context, req *otlp.MetricsRequest) (*otlp.MetricsResponse, error) {
		enc(ctx, req)
		return &otlp.MetricsResponse{}, nil
	})
	mux.Logs().HandleFunc(func(ctx context.Context, req *otlp.LogsRequest) (*otlp.LogsResponse, error) {
		enc(ctx, req)
		return &otlp.LogsResponse{}, nil
	})
	ridge.Run(":4318", "/", mux)
}
```

### `otlptest` package: testhelper 

```go
package otlp_test

import (
    "context"
    "testing"
    "sync/atomic"

    "github.com/mashiike/go-otel-server/otlp"
    "github.com/stretchr/testify/assert"
)

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
	tracerProvider.ForceFlush(ctx)
	require.EqualValues(t, 1, atomic.LoadInt32(&traceCount))
	require.True(t, existsHeader.Load())
}
```

## License

This project is licensed under the [MIT License](LICENSE).
