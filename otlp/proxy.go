package otlp

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func grpcCodeToHTTPStatus(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return http.StatusRequestTimeout
	case codes.Unknown:
		return http.StatusInternalServerError
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Aborted:
		return http.StatusConflict
	case codes.OutOfRange:
		return http.StatusBadRequest
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Internal:
		return http.StatusInternalServerError
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DataLoss:
		return http.StatusInternalServerError
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

func errorProto(w http.ResponseWriter, st *status.Status) {
	httpStatus := grpcCodeToHTTPStatus(st.Code())
	bs, err := proto.Marshal(st.Proto())
	if err != nil {
		http.Error(w, http.StatusText(httpStatus), httpStatus)
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(httpStatus)
	w.Write(bs)
}

func errorJSON(w http.ResponseWriter, st *status.Status) {
	httpStatus := grpcCodeToHTTPStatus(st.Code())
	bs, err := protojson.Marshal(st.Proto())
	if err != nil {
		http.Error(w, http.StatusText(httpStatus), httpStatus)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	w.Write(bs)
}

type proxyHandler[Req, Resp proto.Message] struct {
	newRequestFunc func(context.Context) Req
	handler        func(context.Context, Req) (Resp, error)
}

func newProxyHandler[Req, Resp proto.Message](newRequestFunc func(context.Context) Req, handler func(context.Context, Req) (Resp, error)) *proxyHandler[Req, Resp] {
	return &proxyHandler[Req, Resp]{
		newRequestFunc: newRequestFunc,
		handler:        handler,
	}
}

func (h *proxyHandler[Req, Resp]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		h.serveHTTPWithProto(w, r)
	case "application/json":
		h.serveHTTPWithJSON(w, r)
	default:
		http.Error(w, http.StatusText(http.StatusUnsupportedMediaType), http.StatusUnsupportedMediaType)
	}
}

func (h *proxyHandler[Req, Resp]) serveHTTPWithProto(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		st := status.New(codes.InvalidArgument, "Unable to read request body")
		st, _ = st.WithDetails(&errdetails.ErrorInfo{Reason: err.Error()})
		errorProto(w, st)
		return
	}
	defer r.Body.Close()
	req := h.newRequestFunc(ctx)
	if err := proto.Unmarshal(body, req); err != nil {
		errorProto(w, status.New(codes.InvalidArgument, "Unable to unmarshal request body"))
		return
	}
	resp, err := h.handler(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			errorProto(w, st)
			return
		}
		errorProto(w, status.New(codes.Internal, err.Error()))
		return
	}
	data, err := proto.Marshal(resp)
	if err != nil {
		st := status.New(codes.Internal, "Unable to marshal response")
		st, _ = st.WithDetails(&errdetails.ErrorInfo{Reason: err.Error()})
		errorProto(w, st)
		return
	}
	var buf bytes.Buffer
	if _, err := buf.Write(data); err != nil {
		st := status.New(codes.Internal, "Unable to write response")
		st, _ = st.WithDetails(&errdetails.ErrorInfo{Reason: err.Error()})
		errorProto(w, st)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, &buf)
}

func (h *proxyHandler[Req, Resp]) serveHTTPWithJSON(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer r.Body.Close()
	req := h.newRequestFunc(ctx)
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		st := status.New(codes.InvalidArgument, "Unable to read request body")
		st, _ = st.WithDetails(&errdetails.ErrorInfo{Reason: err.Error()})
		errorJSON(w, st)
		return
	}
	defer r.Body.Close()
	if err := protojson.Unmarshal(bs, req); err != nil {
		st := status.New(codes.InvalidArgument, "Unable to unmarshal request body")
		st, _ = st.WithDetails(&errdetails.ErrorInfo{Reason: err.Error()})
		errorJSON(w, st)
		return
	}
	resp, err := h.handler(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			errorJSON(w, st)
			return
		}
		errorJSON(w, status.New(codes.Internal, err.Error()))
		return
	}
	data, err := protojson.Marshal(resp)
	if err != nil {
		st := status.New(codes.Internal, "Unable to marshal response")
		st, _ = st.WithDetails(&errdetails.ErrorInfo{Reason: err.Error()})
		errorJSON(w, st)
		return
	}
	var buf bytes.Buffer
	if _, err := buf.Write(data); err != nil {
		st := status.New(codes.Internal, "Unable to write response")
		st, _ = st.WithDetails(&errdetails.ErrorInfo{Reason: err.Error()})
		errorJSON(w, st)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, &buf)
}

func HeadersFromContext(ctx context.Context) (http.Header, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return http.Header{}, false
	}
	headers := make(http.Header, len(md))
	for k, v := range md {
		for _, vv := range v {
			headers.Add(k, vv)
		}
	}
	return headers, true
}
