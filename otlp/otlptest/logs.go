package otlptest

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/log"
)

type LogsService struct {
	mu          sync.Mutex
	EndpointURL string
	Protocol    string
	exporter    log.Exporter
	provider    *log.LoggerProvider
}

func (s *LogsService) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		s.provider.Shutdown(context.Background())
	}
	if s.exporter != nil {
		s.exporter.Shutdown(context.Background())
	}
}

func (s *LogsService) Exporter(opts ...any) (log.Exporter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exporter != nil {
		return s.exporter, nil
	}
	grpcOptions := []otlploggrpc.Option{}
	httpOptions := []otlploghttp.Option{}
	for _, opt := range opts {
		switch v := opt.(type) {
		case otlploggrpc.Option:
			grpcOptions = append(grpcOptions, v)
		case otlploghttp.Option:
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

func (s *LogsService) grpcExporter(opts ...otlploggrpc.Option) (log.Exporter, error) {
	opts = append(opts, otlploggrpc.WithEndpointURL(s.EndpointURL))
	return otlploggrpc.New(context.Background(), opts...)
}

func (s *LogsService) httpExporter(opts ...otlploghttp.Option) (log.Exporter, error) {
	opts = append(opts, otlploghttp.WithEndpointURL(s.EndpointURL))
	return otlploghttp.New(context.Background(), opts...)
}

func (s *LogsService) Provider(opts ...any) (*log.LoggerProvider, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		return s.provider, nil
	}
	exporterOpts := []any{}
	providerOpts := []log.LoggerProviderOption{}
	batcherOpts := []log.BatchProcessorOption{}
	for _, opt := range opts {
		switch v := opt.(type) {
		case otlploggrpc.Option, otlploghttp.Option:
			exporterOpts = append(exporterOpts, v)
		case log.LoggerProviderOption:
			providerOpts = append(providerOpts, v)
		case log.BatchProcessorOption:
			batcherOpts = append(batcherOpts, v)
		}
	}
	s.mu.Unlock()
	exporter, err := s.Exporter(exporterOpts...)
	s.mu.Lock()
	if err != nil {
		return nil, err
	}
	providerOpts = append(providerOpts,
		log.WithProcessor(log.NewBatchProcessor(exporter, batcherOpts...)),
	)
	s.provider = log.NewLoggerProvider(providerOpts...)
	return s.provider, nil
}
