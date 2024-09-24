package otlptest

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
)

type MetricsService struct {
	mu          sync.Mutex
	EndpointURL string
	Protocol    string
	exporter    metric.Exporter
	provider    *metric.MeterProvider
}

func (s *MetricsService) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		s.provider.Shutdown(context.Background())
	}
	if s.exporter != nil {
		s.exporter.Shutdown(context.Background())
	}
}

func (s *MetricsService) Exporter(opts ...any) (metric.Exporter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exporter != nil {
		return s.exporter, nil
	}
	grpcOptions := []otlpmetricgrpc.Option{}
	httpOptions := []otlpmetrichttp.Option{}
	for _, opt := range opts {
		switch v := opt.(type) {
		case otlpmetricgrpc.Option:
			grpcOptions = append(grpcOptions, v)
		case otlpmetrichttp.Option:
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

func (s *MetricsService) grpcExporter(opts ...otlpmetricgrpc.Option) (metric.Exporter, error) {
	opts = append(opts, otlpmetricgrpc.WithEndpointURL(s.EndpointURL))
	return otlpmetricgrpc.New(context.Background(), opts...)
}

func (s *MetricsService) httpExporter(opts ...otlpmetrichttp.Option) (metric.Exporter, error) {
	opts = append(opts, otlpmetrichttp.WithEndpointURL(s.EndpointURL))
	return otlpmetrichttp.New(context.Background(), opts...)
}

func (s *MetricsService) Provider(opts ...any) (*metric.MeterProvider, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		return s.provider, nil
	}
	exporterOpts := []any{}
	providerOpts := []metric.Option{}
	readerOpts := []metric.PeriodicReaderOption{}
	for _, opt := range opts {
		switch v := opt.(type) {
		case otlpmetricgrpc.Option, otlpmetrichttp.Option:
			exporterOpts = append(exporterOpts, v)
		case metric.PeriodicReaderOption:
			readerOpts = append(readerOpts, v)
		case metric.Option:
			providerOpts = append(providerOpts, v)
		}
	}
	s.mu.Unlock()
	exporter, err := s.Exporter(exporterOpts...)
	s.mu.Lock()
	if err != nil {
		return nil, err
	}
	providerOpts = append(providerOpts, metric.WithReader(
		metric.NewPeriodicReader(exporter, readerOpts...),
	))
	provider := metric.NewMeterProvider(
		providerOpts...,
	)
	s.provider = provider
	return provider, nil
}
