# Repository Guidelines

## Project Structure & Module Organization
- Root module `go.mod` targets Go 1.22; core library code lives in `otlp/` (server mux, client helpers, JSON codecs, append/proxy utilities) with co-located `_test.go` files and fixtures in `otlp/testdata/`.
- Example apps in `examples/` (`grpc` server on :4317 requiring `API_KEY`, `client` sender, `lambda` HTTP entrypoint) show typical wiring; use them as references, not production artefacts.
- `my-local/` is a scratch module for ad-hoc experiments; avoid committing changes there unless intentionally shared.

## Build, Test, and Development Commands
- `go test ./...` — run the full suite; add `-race` locally for concurrency checks.
- `go vet ./...` — basic static analysis to catch obvious issues.
- `golangci-lint run` — uses `.golangci.yaml` (enables staticcheck, govet, errcheck, gocyclo; skips some rules for `_test.go`); run before opening a PR.
- `go run ./examples/grpc` to start a sample OTLP gRPC server (set `API_KEY`); `go run ./examples/client` to send demo spans; tune endpoints via `OTEL_EXPORTER_`-prefixed env vars.

## Coding Style & Naming Conventions
- Code must be `gofmt`/`goimports` clean; idiomatic tabs and one declaration per line. Keep functions focused to satisfy `gocyclo` thresholds.
- Prefer context-aware APIs; plumb `context.Context` through call chains and avoid swallowing errors. Use structured logging via `slog` when emitting server/client diagnostics.
- Comments follow Go style: skip “what” narration; only add brief “why” context where intent is non-obvious. Exported identifiers need short doc comments aligned with OTLP domain terms (`TraceRequest`, `MetricsRequest`, etc.).

## Testing Guidelines
- Use the standard library `testing` package with `testify/require` (already in use) for assertions; keep tests table-driven where it clarifies cases.
- Place fixtures under `otlp/testdata/` and keep them minimal; prefer golden JSON for OTLP payloads.
- When adding behaviors, co-locate new tests with the implementation file and exercise both happy-path and header/metadata edge cases.

## Commit & Pull Request Guidelines
- Commit messages should be concise and imperative (e.g., `Fix client retry backoff`); keep one logical change per commit.
- PRs should include: a short summary of behavior changes, any config or env vars touched, and test evidence (`go test ./...`, `golangci-lint run`). Link issues when relevant and note breaking changes or API surface adjustments explicitly.

## Security & Configuration Tips
- Do not commit secrets; pull keys like `API_KEY` or exporter credentials from the environment and document required variables in examples.
- When running sample servers, restrict exposed ports or bind to `127.0.0.1` during local development; verify TLS and auth when proxying OTLP traffic. 
