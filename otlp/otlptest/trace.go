package otlptest

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/trace"
)

type TraceService struct {
	mu          sync.Mutex
	EndpointURL string
	Protocol    string
	exporter    *otlptrace.Exporter
	provider    *trace.TracerProvider
}

func (s *TraceService) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		if err := s.provider.Shutdown(context.Background()); err != nil {
			slog.Warn("failed to shutdown test trace provider", "details", err)
		}
	}
	if s.exporter != nil {
		if err := s.exporter.Shutdown(context.Background()); err != nil {
			slog.Warn("failed to shutdown test trace exporter", "details", err)
		}
	}
}

func (s *TraceService) Exporter(opts ...any) (*otlptrace.Exporter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exporter != nil {
		return s.exporter, nil
	}
	grpcOptions := []otlptracegrpc.Option{}
	httpOptions := []otlptracehttp.Option{}
	for _, opt := range opts {
		switch v := opt.(type) {
		case otlptracegrpc.Option:
			grpcOptions = append(grpcOptions, v)
		case otlptracehttp.Option:
			httpOptions = append(httpOptions, v)
		}
	}
	switch s.Protocol {
	case "grpc":
		return s.grpcExporter(grpcOptions...)
	case "http":
		return s.httpExporter(httpOptions...)
	default:
		return nil, errors.New("unsupported protocol")
	}
}

func (s *TraceService) grpcExporter(opts ...otlptracegrpc.Option) (*otlptrace.Exporter, error) {
	opts = append(opts, otlptracegrpc.WithEndpointURL(s.EndpointURL))
	exporter, err := otlptracegrpc.New(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	s.exporter = exporter
	return exporter, nil
}

func (s *TraceService) httpExporter(opts ...otlptracehttp.Option) (*otlptrace.Exporter, error) {
	opts = append(opts, otlptracehttp.WithEndpointURL(s.EndpointURL))
	exporter, err := otlptracehttp.New(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	s.exporter = exporter
	return exporter, nil
}

func (s *TraceService) Provider(opts ...any) (*trace.TracerProvider, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		return s.provider, nil
	}
	exporterOpts := []any{}
	providerOpts := []trace.TracerProviderOption{}
	batcherOpts := []trace.BatchSpanProcessorOption{}
	for _, opt := range opts {
		switch v := opt.(type) {
		case otlptracegrpc.Option, otlptracehttp.Option:
			exporterOpts = append(exporterOpts, v)
		case trace.TracerProviderOption:
			providerOpts = append(providerOpts, v)
		case trace.BatchSpanProcessorOption:
		}
	}
	s.mu.Unlock()
	exporter, err := s.Exporter(exporterOpts...)
	s.mu.Lock()
	if err != nil {
		return nil, err
	}
	providerOpts = append(providerOpts, trace.WithBatcher(
		exporter,
		batcherOpts...,
	))
	tp := trace.NewTracerProvider(providerOpts...)
	s.provider = tp
	return s.provider, nil
}
