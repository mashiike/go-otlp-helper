package otlptest

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"

	"github.com/mashiike/go-otlp-helper/otlp"
)

type HTTPServer struct {
	*httptest.Server
	Trace   *TraceService
	Metrics *MetricsService
	Logs    *LogsService
}

func NewHTTPServer(mux *otlp.ServerMux) *HTTPServer {
	server := NewUnstartedHTTPServer(mux)
	server.Start()
	return server
}

func NewUnstartedHTTPServer(mux *otlp.ServerMux) *HTTPServer {
	server := httptest.NewUnstartedServer(mux)
	return &HTTPServer{
		Server: server,
	}
}

func (s *HTTPServer) Start() {
	s.Server.Start()
	s.newTrace()
	s.newMetrics()
	s.newLogs()
	if httpServeFlag != "" {
		fmt.Fprintln(os.Stderr, "otlptest: serving on", s.URL)
		select {}
	}
}

func (s *HTTPServer) Close() {
	s.Trace.close()
	s.Metrics.close()
	s.Logs.close()
	s.Server.Close()
}

func (s *HTTPServer) newTrace() {
	u, _ := url.Parse(s.URL)
	u = u.JoinPath("/v1/traces")
	s.Trace = &TraceService{
		EndpointURL: u.String(),
		Protocol:    "http",
	}
}

func (s *HTTPServer) newMetrics() {
	u, _ := url.Parse(s.URL)
	u = u.JoinPath("/v1/metrics")
	s.Metrics = &MetricsService{
		EndpointURL: u.String(),
		Protocol:    "http",
	}
}

func (s *HTTPServer) newLogs() {
	u, _ := url.Parse(s.URL)
	u = u.JoinPath("/v1/logs")
	s.Logs = &LogsService{
		EndpointURL: u.String(),
		Protocol:    "http",
	}
}
