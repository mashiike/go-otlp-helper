package otlptest

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"

	"github.com/mashiike/go-otlp-helper/otlp"
	"google.golang.org/grpc"
)

type Server struct {
	URL      string
	Listener net.Listener
	Trace    *TraceService
	Metrics  *MetricsService
	Logs     *LogsService

	server *grpc.Server
	wg     sync.WaitGroup

	mu     sync.Mutex
	logger *slog.Logger
	closed bool
}

func NewServer(mux *otlp.ServerMux, opts ...grpc.ServerOption) *Server {
	server := NewUnstartedServer(mux, opts...)
	server.Start()
	return server
}

func NewUnstartedServer(mux *otlp.ServerMux, opts ...grpc.ServerOption) *Server {
	s := &Server{
		Listener: newLocalListener(grpcServeFlag),
		server:   grpc.NewServer(opts...),
	}
	s.SetLogger(nil)
	mux.Register(s.server)
	return s
}

func (s *Server) SetLogger(logger *slog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))
	}
	s.logger = logger
}

func (s *Server) Start() {
	if s.URL != "" {
		panic("Server already started")
	}

	s.URL = "http://" + s.Listener.Addr().String()
	s.goServe()
	s.newTrace()
	s.newMetrics()
	s.newLogs()
	if grpcServeFlag != "" {
		fmt.Fprintln(os.Stderr, "otlptest: serving on", s.URL)
		select {}
	}
}

func (s *Server) goServe() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.server.Serve(s.Listener); err != nil {
			s.logger.Error("server.Serve", "error", err)
		}
	}()
}

func (s *Server) Close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.Trace.close()
		s.Metrics.close()
		s.Logs.close()
		if err := s.Listener.Close(); err != nil {
			s.logger.Debug("Listener.Close", "error", err)
		}
		s.server.GracefulStop()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) newTrace() {
	s.Trace = &TraceService{
		EndpointURL: s.URL,
		Protocol:    "grpc",
	}
}

func (s *Server) newMetrics() {
	s.Metrics = &MetricsService{
		EndpointURL: s.URL,
		Protocol:    "grpc",
	}
}

func (s *Server) newLogs() {
	s.Logs = &LogsService{
		EndpointURL: s.URL,
		Protocol:    "grpc",
	}
}
