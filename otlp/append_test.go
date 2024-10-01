package otlp_test

import (
	"os"
	"testing"

	"github.com/mashiike/go-otlp-helper/otlp"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestEqualAttributes(t *testing.T) {
	cases := []struct {
		name   string
		attrs1 []*commonpb.KeyValue
		attrs2 []*commonpb.KeyValue
		want   bool
	}{
		{
			name: "string same",
			attrs1: []*commonpb.KeyValue{
				{
					Key: "key1",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value1",
						},
					},
				},
			},
			attrs2: []*commonpb.KeyValue{
				{
					Key: "key1",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value1",
						},
					},
				},
			},
			want: true,
		},
		{
			name: "string different",
			attrs1: []*commonpb.KeyValue{
				{
					Key: "key1",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value1",
						},
					},
				},
			},
			attrs2: []*commonpb.KeyValue{
				{
					Key: "key1",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value2",
						},
					},
				},
			},
			want: false,
		},
		{
			name:   "missmatch element count",
			attrs1: []*commonpb.KeyValue{},
			attrs2: []*commonpb.KeyValue{
				{
					Key: "key1",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value1",
						},
					},
				},
			},
			want: false,
		},
		{
			name: "different key order",
			attrs1: []*commonpb.KeyValue{
				{
					Key: "key1",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value1",
						},
					},
				},
				{
					Key: "key2",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value2",
						},
					},
				},
			},
			attrs2: []*commonpb.KeyValue{
				{
					Key: "key2",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value2",
						},
					},
				},
				{
					Key: "key1",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "value1",
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := otlp.EqualAttributes(tc.attrs1, tc.attrs2)
			if got != tc.want {
				t.Errorf("EqualAttributes() = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestAppendResourceSpans(t *testing.T) {
	var trace1, trace2 tracepb.TracesData
	trace1json, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	trace2json, err := os.ReadFile("testdata/trace2.json")
	require.NoError(t, err)
	require.NoError(t, otlp.UnmarshalJSON(trace1json, &trace1))
	require.NoError(t, otlp.UnmarshalJSON(trace2json, &trace2))
	actual := otlp.AppendResourceSpans(trace1.GetResourceSpans(), trace2.GetResourceSpans()...)
	require.NotNil(t, actual)
	bs, err := otlp.MarshalIndentJSON(&tracepb.TracesData{ResourceSpans: actual}, "  ")
	require.NoError(t, err)
	t.Log("actual:", string(bs))
	expected, err := os.ReadFile("testdata/batched_trace.json")
	require.NoError(t, err)
	t.Log("expected:", string(expected))
	require.JSONEq(t, string(expected), string(bs))
}

func TestAppendResourceMetrics(t *testing.T) {
	var metrics1, metrics2 metricspb.MetricsData
	metrics1json, err := os.ReadFile("testdata/metrics.json")
	require.NoError(t, err)
	metrics2json, err := os.ReadFile("testdata/metrics2.json")
	require.NoError(t, err)
	require.NoError(t, otlp.UnmarshalJSON(metrics1json, &metrics1))
	require.NoError(t, otlp.UnmarshalJSON(metrics2json, &metrics2))
	actual := otlp.AppendResourceMetrics(metrics1.GetResourceMetrics(), metrics2.GetResourceMetrics()...)
	require.NotNil(t, actual)
	bs, err := otlp.MarshalIndentJSON(&metricspb.MetricsData{ResourceMetrics: actual}, "  ")
	require.NoError(t, err)
	t.Log("actual:", string(bs))
	expected, err := os.ReadFile("testdata/batched_metrics.json")
	require.NoError(t, err)
	t.Log("expected:", string(expected))
	require.JSONEq(t, string(expected), string(bs))
}

func TestAppendResourceLogs(t *testing.T) {
	var logs1, logs2 logspb.LogsData
	logs1json, err := os.ReadFile("testdata/logs.json")
	require.NoError(t, err)
	logs2json, err := os.ReadFile("testdata/logs2.json")
	require.NoError(t, err)
	require.NoError(t, otlp.UnmarshalJSON(logs1json, &logs1))
	require.NoError(t, otlp.UnmarshalJSON(logs2json, &logs2))
	actual := otlp.AppendResourceLogs(logs1.GetResourceLogs(), logs2.GetResourceLogs()...)
	require.NotNil(t, actual)
	bs, err := otlp.MarshalIndentJSON(&logspb.LogsData{ResourceLogs: actual}, "  ")
	require.NoError(t, err)
	t.Log("actual:", string(bs))
	expected, err := os.ReadFile("testdata/batched_logs.json")
	require.NoError(t, err)
	t.Log("expected:", string(expected))
	require.JSONEq(t, string(expected), string(bs))
}
