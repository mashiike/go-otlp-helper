package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/mashiike/go-otlp-helper/otlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		bs, err := otlp.MarshalJSON(msg)
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
