module github.com/mashiike/go-otlp-helper/examples/lambda

go 1.24.0

replace github.com/mashiike/go-otlp-helper => ../../

require (
	github.com/fujiwara/ridge v0.11.3
	github.com/mashiike/go-otlp-helper v0.4.1
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/aws/aws-lambda-go v1.47.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/pires/go-proxyproto v0.7.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251124214823-79d6a2a48846 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251124214823-79d6a2a48846 // indirect
	google.golang.org/grpc v1.77.0 // indirect
)
