package otlp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	ResourceSpans   = tracepb.ResourceSpans
	ResourceMetrics = metricspb.ResourceMetrics
	ResourceLogs    = logspb.ResourceLogs
)

// Client is OTLP Low-Level Client
type Client struct {
	o  *clientOptions
	mu sync.RWMutex

	conns        map[string]*grpc.ClientConn
	stopContexts map[string]context.Context
	stopFuncs    map[string]context.CancelFunc
}

func NewClient(endpoint string, opts ...ClientOption) (*Client, error) {
	o := &clientOptions{}
	if endpoint != "" {
		u, err := parseEndpoint(endpoint)
		if err != nil {
			return nil, fmt.Errorf("endpoint parse error: %w", err)
		}
		o.endpoint = u
	}
	if err := o.apply(opts...); err != nil {
		return nil, err
	}
	o.logger.Debug(
		"initializing client",
		slog.Group("traces",
			"protocol", o.traces.protocol,
			"endpoint", o.traces.endpoint.String(),
			"address", o.traces.endpoint.Host,
			"insecure", o.traces.endpoint.Scheme != "https",
			"timeout", o.traces.exportTimeout,
		),
		slog.Group("metrics",
			"protocol", o.metrics.protocol,
			"endpoint", o.metrics.endpoint.String(),
			"address", o.metrics.endpoint.Host,
			"insecure", o.metrics.endpoint.Scheme != "https",
			"timeout", o.metrics.exportTimeout,
		),
		slog.Group("logs",
			"protocol", o.logs.protocol,
			"endpoint", o.logs.endpoint.String(),
			"address", o.logs.endpoint.Host,
			"insecure", o.logs.endpoint.Scheme != "https",
			"timeout", o.logs.exportTimeout,
		),
	)
	client := &Client{
		o:            o,
		conns:        make(map[string]*grpc.ClientConn, 3),
		stopContexts: make(map[string]context.Context, 3),
		stopFuncs:    make(map[string]context.CancelFunc, 3),
	}
	return client, nil
}

func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.o.traces.isGRPCProtocol() {
		if err := c.startGRPC(ctx, &c.o.traces); err != nil {
			return fmt.Errorf("start traces gRPC client: %w", err)
		}
	}
	if c.o.metrics.isGRPCProtocol() {
		if err := c.startGRPC(ctx, &c.o.metrics); err != nil {
			return fmt.Errorf("start metrics gRPC client: %w", err)
		}
	}
	if c.o.logs.isGRPCProtocol() {
		if err := c.startGRPC(ctx, &c.o.logs); err != nil {
			return fmt.Errorf("start logs gRPC client: %w", err)
		}
	}
	return nil
}

func (c *Client) startGRPC(ctx context.Context, so *clientSignalsOptions) error {
	target, dialOptions, connHash := so.grpcConnectionInfo()
	if _, ok := c.conns[connHash]; ok {
		return nil
	}
	c.o.logger.InfoContext(ctx, "connecting to gRPC server", "target", target, "conn_hash", connHash[0:8])
	conn, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		return err
	}
	c.stopContexts[connHash], c.stopFuncs[connHash] = context.WithCancel(ctx)
	c.conns[connHash] = conn
	return nil
}

func (c *Client) newGRPCContext(parent context.Context, so *clientSignalsOptions) (context.Context, context.CancelFunc) {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if so.exportTimeout > 0 {
		ctx, cancel = context.WithTimeout(parent, so.exportTimeout)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}

	if len(so.headers) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(so.headers))
	}
	_, _, connHash := so.grpcConnectionInfo()
	stopCtx, ok := c.stopContexts[connHash]
	if !ok {
		stopCtx = context.Background()
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-stopCtx.Done():
			cancel()
		}
	}()
	return ctx, cancel
}

var (
	ErrAlreadyClosed = errors.New("already closed")
	ErrNotStarted    = errors.New("not started")
)

func (c *Client) UploadTraces(ctx context.Context, protoSpans []*ResourceSpans) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.o.traces.isGRPCProtocol() {
		return c.uploadTracesWithGRPC(ctx, protoSpans)
	}
	return c.uploadTracesWithHTTP(ctx, protoSpans)
}

type UploadTracesPartialSuccessError struct {
	resp *coltracepb.ExportTraceServiceResponse
}

func (e *UploadTracesPartialSuccessError) Response() *coltracepb.ExportTraceServiceResponse {
	return e.resp
}

func (e *UploadTracesPartialSuccessError) Error() string {
	partialSuccess := e.resp.GetPartialSuccess()
	msg := partialSuccess.GetErrorMessage()
	n := partialSuccess.GetRejectedSpans()
	return fmt.Sprintf("failed to export %d spans: %s", n, msg)
}

func (c *Client) uploadTracesWithGRPC(ctx context.Context, protoSpans []*ResourceSpans) error {
	_, _, connHash := c.o.traces.grpcConnectionInfo()
	conn, ok := c.conns[connHash]
	if !ok || conn == nil {
		return ErrNotStarted
	}

	sericeClient := coltracepb.NewTraceServiceClient(conn)
	ctx, cancel := c.newGRPCContext(ctx, &c.o.traces)
	defer cancel()

	c.o.logger.InfoContext(ctx, "uploading traces with gRPC", "conn_hash", connHash[0:8], "num_resource_spans", len(protoSpans))
	resp, err := sericeClient.Export(ctx, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: protoSpans,
	})
	if err != nil && status.Code(err) != codes.OK {
		return err
	}
	if resp != nil && resp.PartialSuccess != nil {
		return &UploadTracesPartialSuccessError{resp: resp}
	}
	return nil
}

func (c *Client) uploadTracesWithHTTP(ctx context.Context, protoSpans []*ResourceSpans) error {
	data := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: protoSpans,
	}
	contentType := c.o.traces.httpContentType()
	var body []byte
	var err error
	if contentType == "application/x-protobuf" {
		body, err = proto.Marshal(data)
	} else {
		body, err = protojson.Marshal(data)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.o.traces.endpoint.String(),
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", c.o.traces.userAgent)
	if len(c.o.traces.headers) > 0 {
		for k, v := range c.o.traces.headers {
			req.Header.Set(k, v)
		}
	}
	client := c.o.traces.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	c.o.logger.InfoContext(ctx, "uploading traces with HTTP", "endpoint", c.o.traces.endpoint.String(), "num_resource_spans", len(protoSpans))
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	var respData coltracepb.ExportTraceServiceResponse
	switch resp.Header.Get("Content-Type") {
	case "application/x-protobuf":
		if err := proto.Unmarshal(respBody, &respData); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	case "application/json":
		if err := protojson.Unmarshal(respBody, &respData); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	default:
		return fmt.Errorf("unexpected content type: %s", resp.Header.Get("Content-Type"))
	}
	if respData.PartialSuccess != nil {
		return &UploadTracesPartialSuccessError{resp: &respData}
	}
	return nil
}

func (c *Client) UploadMetrics(ctx context.Context, protoMetrics []*ResourceMetrics) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.o.metrics.isGRPCProtocol() {
		return c.uploadMetricsWithGRPC(ctx, protoMetrics)
	}
	return c.uploadMetricsWithHTTP(ctx, protoMetrics)
}

type UploadMetricsPartialSuccessError struct {
	resp *colmetricpb.ExportMetricsServiceResponse
}

func (e *UploadMetricsPartialSuccessError) Response() *colmetricpb.ExportMetricsServiceResponse {
	return e.resp
}

func (e *UploadMetricsPartialSuccessError) Error() string {
	partialSuccess := e.resp.GetPartialSuccess()
	msg := partialSuccess.GetErrorMessage()
	n := partialSuccess.GetRejectedDataPoints()
	return fmt.Sprintf("failed to export %d metrics: %s", n, msg)
}

func (c *Client) uploadMetricsWithGRPC(ctx context.Context, protoMetrics []*ResourceMetrics) error {
	_, _, connHash := c.o.metrics.grpcConnectionInfo()
	conn, ok := c.conns[connHash]
	if !ok || conn == nil {
		return ErrNotStarted
	}

	serviceClient := colmetricpb.NewMetricsServiceClient(conn)
	ctx, cancel := c.newGRPCContext(ctx, &c.o.metrics)
	defer cancel()

	c.o.logger.InfoContext(ctx, "uploading metrics", "conn_hash", connHash[0:8], "num_resource_metrics", len(protoMetrics))
	resp, err := serviceClient.Export(ctx, &colmetricpb.ExportMetricsServiceRequest{
		ResourceMetrics: protoMetrics,
	})
	if err != nil && status.Code(err) != codes.OK {
		return err
	}
	if resp != nil && resp.PartialSuccess != nil {
		return &UploadMetricsPartialSuccessError{resp: resp}
	}
	return nil
}

func (c *Client) uploadMetricsWithHTTP(ctx context.Context, protoMetrics []*ResourceMetrics) error {
	data := &colmetricpb.ExportMetricsServiceRequest{
		ResourceMetrics: protoMetrics,
	}
	contentType := c.o.metrics.httpContentType()
	var body []byte
	var err error
	if contentType == "application/x-protobuf" {
		body, err = proto.Marshal(data)
	} else {
		body, err = protojson.Marshal(data)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.o.metrics.endpoint.String(),
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", c.o.metrics.userAgent)
	if len(c.o.metrics.headers) > 0 {
		for k, v := range c.o.metrics.headers {
			req.Header.Set(k, v)
		}
	}
	client := c.o.metrics.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	c.o.logger.InfoContext(ctx, "uploading metrics", "endpoint", c.o.metrics.endpoint.String(), "num_resource_metrics", len(protoMetrics))
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	var respData colmetricpb.ExportMetricsServiceResponse
	switch resp.Header.Get("Content-Type") {
	case "application/x-protobuf":
		if err := proto.Unmarshal(respBody, &respData); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	case "application/json":
		if err := protojson.Unmarshal(respBody, &respData); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	default:
		return fmt.Errorf("unexpected content type: %s", resp.Header.Get("Content-Type"))
	}
	if respData.PartialSuccess != nil {
		return &UploadMetricsPartialSuccessError{resp: &respData}
	}
	return nil
}

func (c *Client) UploadLogs(ctx context.Context, protoLogs []*ResourceLogs) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.o.logs.isGRPCProtocol() {
		return c.uploadLogsWithGRPC(ctx, protoLogs)
	}
	return c.uploadLogsWithHTTP(ctx, protoLogs)
}

type UploadLogsPartialSuccessError struct {
	resp *collogspb.ExportLogsServiceResponse
}

func (e *UploadLogsPartialSuccessError) Response() *collogspb.ExportLogsServiceResponse {
	return e.resp
}

func (e *UploadLogsPartialSuccessError) Error() string {
	partialSuccess := e.resp.GetPartialSuccess()
	msg := partialSuccess.GetErrorMessage()
	n := partialSuccess.GetRejectedLogRecords()
	return fmt.Sprintf("failed to export %d logs: %s", n, msg)
}

func (c *Client) uploadLogsWithGRPC(ctx context.Context, protoLogs []*ResourceLogs) error {
	_, _, connHash := c.o.logs.grpcConnectionInfo()
	conn, ok := c.conns[connHash]
	if !ok || conn == nil {
		return ErrNotStarted
	}

	serviceClient := collogspb.NewLogsServiceClient(conn)
	ctx, cancel := c.newGRPCContext(ctx, &c.o.logs)
	defer cancel()
	c.o.logger.InfoContext(ctx, "uploading logs with gRPC", "conn_hash", connHash[0:8], "num_resource_logs", len(protoLogs))
	resp, err := serviceClient.Export(ctx, &collogspb.ExportLogsServiceRequest{
		ResourceLogs: protoLogs,
	})
	if err != nil && status.Code(err) != codes.OK {
		return err
	}
	if resp != nil && resp.PartialSuccess != nil {
		return &UploadLogsPartialSuccessError{resp: resp}
	}
	return nil
}

func (c *Client) uploadLogsWithHTTP(ctx context.Context, protoLogs []*ResourceLogs) error {
	data := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: protoLogs,
	}
	contentType := c.o.logs.httpContentType()
	var body []byte
	var err error
	if contentType == "application/x-protobuf" {
		body, err = proto.Marshal(data)
	} else {
		body, err = protojson.Marshal(data)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.o.logs.endpoint.String(),
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", c.o.logs.userAgent)
	if len(c.o.logs.headers) > 0 {
		for k, v := range c.o.logs.headers {
			req.Header.Set(k, v)
		}
	}
	client := c.o.logs.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	c.o.logger.InfoContext(ctx, "uploading logs with HTTP", "endpoint", c.o.logs.endpoint.String(), "num_resource_logs", len(protoLogs))
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	var respData collogspb.ExportLogsServiceResponse
	switch resp.Header.Get("Content-Type") {
	case "application/x-protobuf":
		if err := proto.Unmarshal(respBody, &respData); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	case "application/json":
		if err := protojson.Unmarshal(respBody, &respData); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	default:
		return fmt.Errorf("unexpected content type: %s", resp.Header.Get("Content-Type"))
	}
	if respData.PartialSuccess != nil {
		return &UploadLogsPartialSuccessError{resp: &respData}
	}
	return nil
}

func (c *Client) Stop(ctx context.Context) error {
	err := ctx.Err()
	// wait trace uploads to finish
	acquired := make(chan struct{})
	go func() {
		c.mu.Lock()
		close(acquired)
	}()

	select {
	case <-ctx.Done():
		for _, stopFunc := range c.stopFuncs {
			stopFunc()
		}
		err = ctx.Err()

		<-acquired
	case <-acquired:
	}
	defer c.mu.Unlock()
	if len(c.conns) == 0 {
		return ErrAlreadyClosed
	}
	var colseErrs []error
	for connHash, conn := range c.conns {
		if conn == nil {
			continue
		}
		c.o.logger.InfoContext(ctx, "disconnecting from gRPC server", "conn_hash", connHash[0:8])
		if closeErr := conn.Close(); closeErr != nil {
			colseErrs = append(colseErrs, closeErr)
		}
	}
	if err == nil && len(colseErrs) > 0 {
		if len(colseErrs) == 1 {
			err = colseErrs[0]
		} else {
			err = errors.Join(colseErrs...)
		}
	}
	c.conns = make(map[string]*grpc.ClientConn, 3)
	c.stopFuncs = make(map[string]context.CancelFunc, 3)
	c.stopContexts = make(map[string]context.Context, 3)
	return err
}
