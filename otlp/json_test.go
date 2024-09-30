package otlp_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/mashiike/go-otlp-helper/otlp"
	"github.com/stretchr/testify/require"
)

func TestJSONEncoding_Trace(t *testing.T) {
	bs, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	dec := otlp.NewJSONDecoder(bytes.NewReader(bs))
	var req otlp.TraceRequest
	require.NoError(t, dec.Decode(&req))
	require.Len(t, req.GetResourceSpans(), 1)
	rs := req.GetResourceSpans()[0]
	require.NotNil(t, rs)
	r := rs.GetResource()
	require.NotNil(t, r)
	require.Len(t, r.GetAttributes(), 1)
	attr := r.GetAttributes()[0]
	require.NotNil(t, attr)
	require.Equal(t, "service.name", attr.GetKey())
	require.NotNil(t, attr.GetValue())
	require.Equal(t, "my.service", attr.GetValue().GetStringValue())
	ss := rs.GetScopeSpans()
	require.Len(t, ss, 1)
	spans := ss[0].GetSpans()
	require.Len(t, spans, 1)
	require.Len(t, spans[0].GetTraceId(), 16)
	require.Len(t, spans[0].GetSpanId(), 8)
	require.Len(t, spans[0].GetParentSpanId(), 8)

	var buf bytes.Buffer
	enc := otlp.NewJSONEncoder(&buf)
	require.NoError(t, enc.Encode(&req))
	require.JSONEq(t, string(bs), buf.String())
}
