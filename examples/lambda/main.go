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
