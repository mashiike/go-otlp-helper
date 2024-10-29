package otlp_test

import (
	"os"
	"testing"
	"time"

	"github.com/mashiike/go-otlp-helper/otlp"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestPartitionResourceSpans(t *testing.T) {
	bs, err := os.ReadFile("testdata/batched_trace.json")
	require.NoError(t, err)
	var data tracepb.TracesData
	require.NoError(t, otlp.UnmarshalJSON(bs, &data))

	require.NoError(t, err)
	require.Equal(t, 2, otlp.TotalSpans(data.GetResourceSpans()))
	partitionBy := otlp.PartitionResourceSpans(
		data.GetResourceSpans(),
		otlp.PartitionBySpanStartTime(otlp.Hourly, time.FixedZone("Asia/Tokyo", 9*60*60)),
	)
	require.Len(t, partitionBy, 2)
	require.ElementsMatch(
		t,
		[]string{
			"2018/12/13/23",
			"2018/12/14/00",
		},
		mapKeys(partitionBy),
	)

	trace1, err := os.ReadFile("testdata/trace.json")
	require.NoError(t, err)
	actual1, err := otlp.MarshalJSON(&tracepb.TracesData{
		ResourceSpans: partitionBy["2018/12/13/23"],
	})
	require.NoError(t, err)
	require.JSONEq(t, string(trace1), string(actual1))
	actual2, err := otlp.MarshalJSON(&tracepb.TracesData{
		ResourceSpans: partitionBy["2018/12/14/00"],
	})
	require.NoError(t, err)
	trace2, err := os.ReadFile("testdata/trace2.json")
	require.NoError(t, err)
	require.JSONEq(t, string(trace2), string(actual2))
}

func TestPartitionResourceMetrics(t *testing.T) {
	bs, err := os.ReadFile("testdata/batched_metrics.json")
	require.NoError(t, err)
	var data metricspb.MetricsData
	require.NoError(t, otlp.UnmarshalJSON(bs, &data))

	require.NoError(t, err)
	require.Equal(t, 7, otlp.TotalDataPoints(data.GetResourceMetrics()))
	partitionBy := otlp.PartitionResourceMetrics(
		data.GetResourceMetrics(),
		otlp.PartitionByMetricType(),
	)
	require.Len(t, partitionBy, 4)
	require.ElementsMatch(
		t,
		[]string{
			"Sum",
			"Gauge",
			"Histogram",
			"ExponentialHistogram",
		},
		mapKeys(partitionBy),
	)
	actual, err := otlp.MarshalJSON(&metricspb.MetricsData{
		ResourceMetrics: partitionBy["Sum"],
	})
	require.NoError(t, err)
	t.Log(string(actual))
	expected, err := os.ReadFile("testdata/sum_metrics.json")
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual))
}

func TestPartitionResourceLogs(t *testing.T) {
	bs, err := os.ReadFile("testdata/batched_logs.json")
	require.NoError(t, err)
	var data logspb.LogsData
	require.NoError(t, otlp.UnmarshalJSON(bs, &data))

	require.NoError(t, err)
	require.Equal(t, 2, otlp.TotalLogRecords(data.GetResourceLogs()))
	partitionBy := otlp.PartitionResourceLogs(
		data.GetResourceLogs(),
		otlp.PartitionByLogTime(otlp.Hourly, time.FixedZone("Asia/Tokyo", 9*60*60)),
	)
	require.Len(t, partitionBy, 2)

	require.ElementsMatch(
		t,
		[]string{
			"2018/12/13/23",
			"2018/12/14/00",
		},
		mapKeys(partitionBy),
	)

	log1, err := os.ReadFile("testdata/logs.json")
	require.NoError(t, err)
	actual1, err := otlp.MarshalJSON(&logspb.LogsData{
		ResourceLogs: partitionBy["2018/12/13/23"],
	})
	require.NoError(t, err)
	require.JSONEq(t, string(log1), string(actual1))
	actual2, err := otlp.MarshalJSON(&logspb.LogsData{
		ResourceLogs: partitionBy["2018/12/14/00"],
	})
	require.NoError(t, err)
	log2, err := os.ReadFile("testdata/logs2.json")
	require.NoError(t, err)
	require.JSONEq(t, string(log2), string(actual2))
}

func TestFilterResourceSpans(t *testing.T) {
	bs, err := os.ReadFile("testdata/batched_trace.json")
	require.NoError(t, err)
	var data tracepb.TracesData
	require.NoError(t, otlp.UnmarshalJSON(bs, &data))

	require.NoError(t, err)
	require.Equal(t, 2, otlp.TotalSpans(data.GetResourceSpans()))
	filterd := otlp.FilterResourceSpans(
		data.GetResourceSpans(),
		otlp.SpanInTimeRangeFilter(
			time.Date(2018, 12, 13, 23, 0, 0, 0, time.FixedZone("Asia/Tokyo", 9*60*60)),
			time.Date(2018, 12, 13, 23, 59, 59, 0, time.FixedZone("Asia/Tokyo", 9*60*60)),
		),
	)
	require.Equal(t, 1, otlp.TotalSpans(filterd))
	actual, err := otlp.MarshalJSON(&tracepb.TracesData{
		ResourceSpans: filterd,
	})
	require.NoError(t, err)
	expected, err := os.ReadFile("testdata/filterd_trace.json")
	require.NoError(t, err)
	t.Log("actual", string(actual))
	t.Log("expected", string(expected))
	require.JSONEq(t, string(expected), string(actual))
}

func TestFilterResourceMetrics(t *testing.T) {
	bs, err := os.ReadFile("testdata/batched_metrics.json")
	require.NoError(t, err)
	var data metricspb.MetricsData
	require.NoError(t, otlp.UnmarshalJSON(bs, &data))

	require.NoError(t, err)
	require.Equal(t, 7, otlp.TotalDataPoints(data.GetResourceMetrics()))
	filterd := otlp.FilterResourceMetrics(
		data.GetResourceMetrics(),
		otlp.MetricDataPointInTimeRangeFilter(
			time.Date(2018, 12, 13, 23, 51, 0, 0, time.FixedZone("Asia/Tokyo", 9*60*60)),
			time.Date(2018, 12, 13, 23, 51, 1, 0, time.FixedZone("Asia/Tokyo", 9*60*60)),
		),
		func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, m *metricspb.Metric) bool {
			return m.GetName() == "my.counter"
		},
	)
	require.Equal(t, 2, otlp.TotalDataPoints(filterd))
	actual, err := otlp.MarshalJSON(&metricspb.MetricsData{
		ResourceMetrics: filterd,
	})
	require.NoError(t, err)
	expected, err := os.ReadFile("testdata/filterd_metrics.json")
	require.NoError(t, err)
	t.Log("actual", string(actual))
	t.Log("expected", string(expected))
	require.JSONEq(t, string(expected), string(actual))
}

func TestFilterResourceLogs(t *testing.T) {
	bs, err := os.ReadFile("testdata/batched_logs.json")
	require.NoError(t, err)
	var data logspb.LogsData
	require.NoError(t, otlp.UnmarshalJSON(bs, &data))

	require.NoError(t, err)
	require.Equal(t, 2, otlp.TotalLogRecords(data.GetResourceLogs()))
	filterd := otlp.FilterResourceLogs(
		data.GetResourceLogs(),
		otlp.LogRecordInTimeRangeFilter(
			time.Date(2018, 12, 13, 23, 51, 0, 0, time.FixedZone("Asia/Tokyo", 9*60*60)),
			time.Date(2018, 12, 13, 23, 51, 1, 0, time.FixedZone("Asia/Tokyo", 9*60*60)),
		),
	)
	require.Equal(t, 1, otlp.TotalLogRecords(filterd))
	actual, err := otlp.MarshalJSON(&logspb.LogsData{
		ResourceLogs: filterd,
	})
	require.NoError(t, err)
	expected, err := os.ReadFile("testdata/filterd_logs.json")
	require.NoError(t, err)
	t.Log("actual", string(actual))
	t.Log("expected", string(expected))
	require.JSONEq(t, string(expected), string(actual))
}
