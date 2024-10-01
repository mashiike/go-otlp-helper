package otlptest

import (
	"flag"
	"fmt"
	"net"
	"os"
)

func newLocalListener(serveFlag string) net.Listener {
	if serveFlag != "" {
		l, err := net.Listen("tcp", serveFlag)
		if err != nil {
			panic(fmt.Sprintf("otlptest: failed to listen on %v: %v", serveFlag, err))
		}
		return l
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Sprintf("otlptest: failed to listen on a port: %v", err))
		}
	}
	return l
}

var (
	grpcServeFlag string
	httpServeFlag string
)

func slicesContains[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func init() {
	if slicesContains(os.Args, "-otlptest.grpc.serve=") || slicesContains(os.Args, "--otlptest.grpc.serve=") {
		flag.StringVar(&grpcServeFlag, "otlptest.grpc.serve", "", "if non-empty, otlptest.NewServer gRPC serves on this address and blocks.")
	}
}
