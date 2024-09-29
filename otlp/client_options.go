package otlp

import (
	"crypto/sha512"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type clientOptions struct {
	endpoint      *url.URL
	protocol      string
	userAgent     string
	headers       map[string]string
	gzip          *bool
	exportTimeout time.Duration
	httpClient    *http.Client

	traces  clientSignalsOptions
	metrics clientSignalsOptions
	logs    clientSignalsOptions
}

type clientSignalsOptions struct {
	gzip          *bool
	userAgent     string
	signalType    string
	endpoint      *url.URL
	protocol      string
	exportTimeout time.Duration
	headers       map[string]string
	httpClient    *http.Client

	mu          sync.Mutex
	target      string
	connHash    string
	dialOptions []grpc.DialOption
}

type ClientOption func(*clientOptions) error

func (o *clientOptions) apply(opts ...ClientOption) error {
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return err
		}
	}
	return o.build()
}

func ptr[T any](v T) *T {
	return &v
}

var allowedProtocols = []string{
	"grpc",
	"http/json",
	"http/protobuf",
}

// AllowedProtocols is the list of allowed protocol values.
var AllowedProtocols = allowedProtocols

func (so *clientSignalsOptions) fillDefaults(o *clientOptions) {
	if so.userAgent == "" {
		so.userAgent = o.userAgent
	}
	if so.protocol == "" {
		so.protocol = o.protocol
	}
	if so.gzip == nil {
		so.gzip = o.gzip
	}
	if so.exportTimeout == 0 {
		so.exportTimeout = o.exportTimeout
	}
	if so.httpClient == nil {
		so.httpClient = o.httpClient
	}
	if so.endpoint == nil {
		if strings.HasPrefix(so.protocol, "http/") {
			so.endpoint = o.endpoint.JoinPath("v1/" + so.signalType)
		} else {
			so.endpoint = o.endpoint
		}
	}
	if so.headers == nil {
		so.headers = make(map[string]string, len(o.headers))
	}
	for key, value := range o.headers {
		if _, ok := so.headers[key]; !ok {
			so.headers[key] = value
		}
	}
}

func (o *clientOptions) build() error {
	if o.userAgent == "" {
		o.userAgent = fmt.Sprintf(
			"go-otlp-helper/%s (github.com/mashiike/go-otlp-helper/otlp.Client) go/%s",
			version,
			runtime.Version(),
		)
	}
	if o.gzip == nil {
		o.gzip = ptr(false)
	}
	if o.protocol == "" {
		o.protocol = "grpc"
	}
	if o.httpClient == nil {
		o.httpClient = http.DefaultClient
	}
	o.traces.signalType = "traces"
	o.traces.fillDefaults(o)
	o.metrics.signalType = "metrics"
	o.metrics.fillDefaults(o)
	o.logs.signalType = "logs"
	o.logs.fillDefaults(o)
	return nil
}

func (so *clientSignalsOptions) isGRPCProtocol() bool {
	return so.protocol == "grpc"
}

func (so *clientSignalsOptions) isHTTPProtocol() bool {
	return strings.HasPrefix(so.protocol, "http/")
}

func (so *clientSignalsOptions) httpContentType() string {
	if !so.isHTTPProtocol() {
		return ""
	}
	switch so.protocol {
	case "http/json":
		return "application/json"
	case "http/protobuf":
		return "application/x-protobuf"
	default:
		return ""
	}
}

func (so *clientSignalsOptions) grpcConnectionInfo() (string, []grpc.DialOption, string) {
	so.mu.Lock()
	defer so.mu.Unlock()
	if so.connHash != "" {
		return so.target, so.dialOptions, so.connHash
	}
	so.target, so.dialOptions, so.connHash = so.buildGRPCConnectionInfo()
	return so.target, so.dialOptions, so.connHash
}

func (so *clientSignalsOptions) buildGRPCConnectionInfo() (string, []grpc.DialOption, string) {
	haser := sha512.New()
	haser.Write([]byte(so.endpoint.Host))
	opts := []grpc.DialOption{
		grpc.WithUserAgent(so.userAgent),
	}
	haser.Write([]byte(so.userAgent))
	if so.endpoint.Scheme != "https" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		haser.Write([]byte("insecure"))
	} else {
		cred := credentials.NewTLS(nil)
		opts = append(opts, grpc.WithTransportCredentials(cred))
		haser.Write([]byte("tls"))
	}
	if *so.gzip {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
		haser.Write([]byte("gzip"))
	}
	return so.endpoint.Host, opts, fmt.Sprintf("%x", haser.Sum(nil))
}

// WithUserAgent sets the user agent to be sent with the request.
func WithUserAgent(userAgent string) ClientOption {
	return func(o *clientOptions) error {
		o.userAgent = userAgent
		return nil
	}
}

// WithTracesUserAgent sets the user agent to be sent with the trace request. by default, the user agent is shared with all signals.
func WithTracesUserAgent(userAgent string) ClientOption {
	return func(o *clientOptions) error {
		o.traces.userAgent = userAgent
		return nil
	}
}

// WithMetricsUserAgent sets the user agent to be sent with the metrics request. by default, the user agent is shared with all signals.
func WithMetricsUserAgent(userAgent string) ClientOption {
	return func(o *clientOptions) error {
		o.metrics.userAgent = userAgent
		return nil
	}
}

// WithLogsUserAgent sets the user agent to be sent with the log request. by default, the user agent is shared with all signals.
func WithLogsUserAgent(userAgent string) ClientOption {
	return func(o *clientOptions) error {
		o.logs.userAgent = userAgent
		return nil
	}
}

// WithGzip sets the gzip compression to be used with the request.
func WithGzip(gzip bool) ClientOption {
	return func(o *clientOptions) error {
		o.gzip = ptr(gzip)
		return nil
	}
}

// WithTracesGzip sets the gzip compression to be used with the trace request. by default, the gzip compression is shared with all signals.
func WithTracesGzip(gzip bool) ClientOption {
	return func(o *clientOptions) error {
		o.traces.gzip = ptr(gzip)
		return nil
	}
}

// WithMetricsGzip sets the gzip compression to be used with the metrics request. by default, the gzip compression is shared with all signals.
func WithMetricsGzip(gzip bool) ClientOption {
	return func(o *clientOptions) error {
		o.metrics.gzip = ptr(gzip)
		return nil
	}
}

// WithLogsGzip sets the gzip compression to be used with the log request. by default, the gzip compression is shared with all signals.
func WithLogsGzip(gzip bool) ClientOption {
	return func(o *clientOptions) error {
		o.logs.gzip = ptr(gzip)
		return nil
	}
}

// WithHeaders sets the headers to be sent with the request.
func WithHeaders(headers map[string]string) ClientOption {
	return func(o *clientOptions) error {
		o.headers = headers
		return nil
	}
}

// WithTracesHeaders sets the headers to be sent with the trace request. by default, the headers are shared with all signals.
func WithTracesHeaders(headers map[string]string) ClientOption {
	return func(o *clientOptions) error {
		o.traces.headers = headers
		return nil
	}
}

// WithMetricsHeaders sets the headers to be sent with the metrics request. by default, the headers are shared with all signals.
func WithMetricsHeaders(headers map[string]string) ClientOption {
	return func(o *clientOptions) error {
		o.metrics.headers = headers
		return nil
	}
}

// WithLogsHeaders sets the headers to be sent with the log request. by default, the headers are shared with all signals.
func WithLogsHeaders(headers map[string]string) ClientOption {
	return func(o *clientOptions) error {
		o.logs.headers = headers
		return nil
	}
}

// WithProtocol sets the protocol to be used with the request.
func WithProtocol(protocol string) ClientOption {
	return func(o *clientOptions) error {
		if !slices.Contains(allowedProtocols, protocol) {
			return fmt.Errorf("protocol %q is not allowed", protocol)
		}
		o.protocol = protocol
		return nil
	}
}

// WithTracesProtocol sets the protocol to be used with the trace request. by default, the protocol is shared with all signals.
func WithTracesProtocol(protocol string) ClientOption {
	return func(o *clientOptions) error {
		if !slices.Contains(allowedProtocols, protocol) {
			return fmt.Errorf("traces protocol %q is not allowed", protocol)
		}
		o.traces.protocol = protocol
		return nil
	}
}

// WithMetricsProtocol sets the protocol to be used with the metrics request. by default, the protocol is shared with all signals.
func WithMetricsProtocol(protocol string) ClientOption {
	return func(o *clientOptions) error {
		if !slices.Contains(allowedProtocols, protocol) {
			return fmt.Errorf("metrics protocol %q is not allowed", protocol)
		}
		o.metrics.protocol = protocol
		return nil
	}
}

// WithLogsProtocol sets the protocol to be used with the log request. by default, the protocol is shared with all signals.
func WithLogsProtocol(protocol string) ClientOption {
	return func(o *clientOptions) error {
		if !slices.Contains(allowedProtocols, protocol) {
			return fmt.Errorf("logs protocol %q is not allowed", protocol)
		}
		o.logs.protocol = protocol
		return nil
	}
}

// WithExportTimeout sets the timeout to be used with the request.
func WithExportTimeout(exportTimeout time.Duration) ClientOption {
	return func(o *clientOptions) error {
		o.exportTimeout = exportTimeout
		return nil
	}
}

// WithTracesExportTimeout sets the timeout to be used with the trace request. by default, the timeout is shared with all signals.
func WithTracesExportTimeout(exportTimeout time.Duration) ClientOption {
	return func(o *clientOptions) error {
		o.traces.exportTimeout = exportTimeout
		return nil
	}
}

// WithMetricsExportTimeout sets the timeout to be used with the metrics request. by default, the timeout is shared with all signals.
func WithMetricsExportTimeout(exportTimeout time.Duration) ClientOption {
	return func(o *clientOptions) error {
		o.metrics.exportTimeout = exportTimeout
		return nil
	}
}

// WithLogsExportTimeout sets the timeout to be used with the log request. by default, the timeout is shared with all signals.
func WithLogsExportTimeout(exportTimeout time.Duration) ClientOption {
	return func(o *clientOptions) error {
		o.logs.exportTimeout = exportTimeout
		return nil
	}
}

// WithEndpoint sets the endpoint to be used with the request.
func WithEndpoint(endpoint string) ClientOption {
	return func(o *clientOptions) error {
		u, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("endpoint parse error: %w", err)
		}
		o.endpoint = u
		return nil
	}
}

// WithTracesEndpoint sets the endpoint to be used with the trace request. by default, the endpoint is shared with all signals.
func WithTracesEndpoint(endpoint string) ClientOption {
	return func(o *clientOptions) error {
		u, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("traces endpoint parse error: %w", err)
		}
		o.traces.endpoint = u
		return nil
	}
}

// WithMetricsEndpoint sets the endpoint to be used with the metrics request. by default, the endpoint is shared with all signals.
func WithMetricsEndpoint(endpoint string) ClientOption {
	return func(o *clientOptions) error {
		u, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("metrics endpoint parse error: %w", err)
		}
		o.metrics.endpoint = u
		return nil
	}
}

// WithLogsEndpoint sets the endpoint to be used with the log request. by default, the endpoint is shared with all signals.
func WithLogsEndpoint(endpoint string) ClientOption {
	return func(o *clientOptions) error {
		u, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("logs endpoint parse error: %w", err)
		}
		o.logs.endpoint = u
		return nil
	}
}

// WithHTTPClient sets the http client to be used with the request.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(o *clientOptions) error {
		o.httpClient = httpClient
		return nil
	}
}

// WithTracesHTTPClient sets the http client to be used with the trace request. by default, the http client is shared with all signals.
func WithTracesHTTPClient(httpClient *http.Client) ClientOption {
	return func(o *clientOptions) error {
		o.traces.httpClient = httpClient
		return nil
	}
}

// WithMetricsHTTPClient sets the http client to be used with the metrics request. by default, the http client is shared with all signals.
func WithMetricsHTTPClient(httpClient *http.Client) ClientOption {
	return func(o *clientOptions) error {
		o.metrics.httpClient = httpClient
		return nil
	}
}

// WithLogsHTTPClient sets the http client to be used with the log request. by default, the http client is shared with all signals.
func WithLogsHTTPClient(httpClient *http.Client) ClientOption {
	return func(o *clientOptions) error {
		o.logs.httpClient = httpClient
		return nil
	}
}

func lookupEnvValue(name string, envPrefixes []string, setter func(string) error) error {
	upperName := strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
	lowerName := strings.ToLower(strings.ReplaceAll(name, "-", "_"))
	envPrefixes = append(envPrefixes, "")
	for _, prefix := range envPrefixes {
		if s, ok := os.LookupEnv(strings.ToUpper(prefix) + upperName); ok {
			return setter(s)
		}
		if s, ok := os.LookupEnv(strings.ToUpper(prefix) + lowerName); ok {
			return setter(s)
		}
		if s, ok := os.LookupEnv(prefix + upperName); ok {
			return setter(s)
		}
		if s, ok := os.LookupEnv(prefix + lowerName); ok {
			return setter(s)
		}
	}
	return nil
}

// DefaultClientOptions returns the default client options from the environment variables.
// see https://opentelemetry.io/docs/specs/otel/protocol/exporter
// e.g. envPrefixes = []string{"OTEL_EXPORTER_"}
// OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
func DefaultClientOptions(envPrefixes ...string) ClientOption {
	return func(o *clientOptions) error {
		if err := lookupEnvValue("OTLP_PROTOCOL", envPrefixes, func(s string) error {
			o.protocol = s
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_TRACES_PROTOCOL", envPrefixes, func(s string) error {
			o.traces.protocol = s
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_METRICS_PROTOCOL", envPrefixes, func(s string) error {
			o.metrics.protocol = s
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_LOGS_PROTOCOL", envPrefixes, func(s string) error {
			o.logs.protocol = s
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_ENDPOINT", envPrefixes, func(s string) error {
			u, err := url.Parse(s)
			if err != nil {
				return fmt.Errorf("endpoint parse error: %w", err)
			}
			o.endpoint = u
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_TRACES_ENDPOINT", envPrefixes, func(s string) error {
			u, err := url.Parse(s)
			if err != nil {
				return fmt.Errorf("traces endpoint parse error: %w", err)
			}
			o.traces.endpoint = u
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_METRICS_ENDPOINT", envPrefixes, func(s string) error {
			u, err := url.Parse(s)
			if err != nil {
				return fmt.Errorf("metrics endpoint parse error: %w", err)
			}
			o.metrics.endpoint = u
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_LOGS_ENDPOINT", envPrefixes, func(s string) error {
			u, err := url.Parse(s)
			if err != nil {
				return fmt.Errorf("logs endpoint parse error: %w", err)
			}
			o.logs.endpoint = u
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_TIMEOUT", envPrefixes, func(s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return fmt.Errorf("export timeout parse error: %w", err)
			}
			o.exportTimeout = d
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_TRACES_TIMEOUT", envPrefixes, func(s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return fmt.Errorf("traces export timeout parse error: %w", err)
			}
			o.traces.exportTimeout = d
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_METRICS_TIMEOUT", envPrefixes, func(s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return fmt.Errorf("metrics export timeout parse error: %w", err)
			}
			o.metrics.exportTimeout = d
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_LOGS_TIMEOUT", envPrefixes, func(s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return fmt.Errorf("logs export timeout parse error: %w", err)
			}
			o.logs.exportTimeout = d
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_HEADERS", envPrefixes, func(s string) error {
			envParts := strings.Split(s, ",")
			headers := make(map[string]string, len(envParts))
			for _, envPart := range envParts {
				parts := strings.SplitN(envPart, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("header %q is invalid", envPart)
				}
				headers[parts[0]] = parts[1]
			}
			if o.headers == nil {
				o.headers = make(map[string]string)
			}
			for key, value := range headers {
				o.headers[key] = value
			}
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_TRACES_HEADERS", envPrefixes, func(s string) error {
			envParts := strings.Split(s, ",")
			headers := make(map[string]string, len(envParts))
			for _, envPart := range envParts {
				parts := strings.SplitN(envPart, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("traces header %q is invalid", envPart)
				}
				headers[parts[0]] = parts[1]
			}
			if o.traces.headers == nil {
				o.traces.headers = make(map[string]string)
			}
			for key, value := range headers {
				o.traces.headers[key] = value
			}
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_METRICS_HEADERS", envPrefixes, func(s string) error {
			envParts := strings.Split(s, ",")
			headers := make(map[string]string, len(envParts))
			for _, envPart := range envParts {
				parts := strings.SplitN(envPart, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("metrics header %q is invalid", envPart)
				}
				headers[parts[0]] = parts[1]
			}
			if o.metrics.headers == nil {
				o.metrics.headers = make(map[string]string)
			}
			for key, value := range headers {
				o.metrics.headers[key] = value
			}
			return nil
		}); err != nil {
			return err
		}
		if err := lookupEnvValue("OTLP_LOGS_HEADERS", envPrefixes, func(s string) error {
			envParts := strings.Split(s, ",")
			headers := make(map[string]string, len(envParts))
			for _, envPart := range envParts {
				parts := strings.SplitN(envPart, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("logs header %q is invalid", envPart)
				}
				headers[parts[0]] = parts[1]
			}
			if o.logs.headers == nil {
				o.logs.headers = make(map[string]string)
			}
			for key, value := range headers {
				o.logs.headers[key] = value
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}
}