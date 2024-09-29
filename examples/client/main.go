package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/mashiike/go-otlp-helper/otlp"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	client, err := otlp.NewClient(
		"http://127.0.0.1:4317",
		otlp.DefaultClientOptions("OTEL_EXPORTER_"),
		otlp.WithLogger(slog.Default()),
	)
	if err != nil {
		slog.Error("failed to create client", "details", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := client.Start(ctx); err != nil {
		slog.Error("failed to start client", "details", err)
		os.Exit(1)
	}
	now := time.Now()
	randReader := rand.New(rand.NewSource(now.UnixNano()))
	traceID := make([]byte, 16)
	randReader.Read(traceID)
	spanID1 := make([]byte, 8)
	randReader.Read(spanID1)
	spanID2 := make([]byte, 8)
	randReader.Read(spanID2)
	resourceSpancs := []*trace.ResourceSpans{
		{
			Resource: &resource.Resource{
				Attributes: []*common.KeyValue{
					{
						Key: "service.name",
						Value: &common.AnyValue{
							Value: &common.AnyValue_StringValue{
								StringValue: "example-service",
							},
						},
					},
				},
			},
			ScopeSpans: []*trace.ScopeSpans{
				{
					Spans: []*trace.Span{
						{
							TraceId:           traceID,
							SpanId:            spanID1,
							Name:              "example-outer-span",
							Kind:              trace.Span_SPAN_KIND_INTERNAL,
							StartTimeUnixNano: uint64(now.Add(-1 * time.Second).UnixNano()),
							EndTimeUnixNano:   uint64(now.UnixNano()),
							Status: &trace.Status{
								Code: trace.Status_STATUS_CODE_OK,
							},
						},
						{
							TraceId:           traceID,
							SpanId:            spanID2,
							ParentSpanId:      spanID1,
							Name:              "example-inner-span",
							Kind:              trace.Span_SPAN_KIND_INTERNAL,
							StartTimeUnixNano: uint64(now.Add(-500 * time.Millisecond).UnixNano()),
							EndTimeUnixNano:   uint64(now.Add(-250 * time.Millisecond).UnixNano()),
							Status: &trace.Status{
								Code: trace.Status_STATUS_CODE_OK,
							},
						},
					},
				},
			},
		},
	}
	dumpResourceSpans(resourceSpancs)
	if err := client.UploadTraces(ctx, resourceSpancs); err != nil {
		slog.Error("failed to upload traces", "details", err)
		os.Exit(1)
	}
}

func dumpResourceSpans(resourceSpans []*trace.ResourceSpans) {
	req := &otlp.TraceRequest{
		ResourceSpans: resourceSpans,
	}
	bs, err := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}.Marshal(req)
	if err != nil {
		slog.Error("failed to marshal request", "details", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s\n", string(bs))
}
