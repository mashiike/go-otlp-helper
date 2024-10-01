package otlp

import (
	"slices"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// AppendResourceSpans converts a list of ResourceSpans into a list of ResourceSpans with unique resources.
func AppendResourceSpans(dst []*tracepb.ResourceSpans, elems ...*tracepb.ResourceSpans) []*tracepb.ResourceSpans {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		targetResourceSpansIndex := slices.IndexFunc(dst, func(dstElem *tracepb.ResourceSpans) bool {
			if dstElem == nil {
				return false
			}
			return EqualResource(dstElem.GetResource(), elem.GetResource())
		})
		if targetResourceSpansIndex == -1 {
			dst = append(dst, elem)
			continue
		}
		dst[targetResourceSpansIndex].ScopeSpans = AppendScopeSpans(dst[targetResourceSpansIndex].GetScopeSpans(), elem.GetScopeSpans()...)
	}
	return dst
}

func AppendScopeSpans(dst []*tracepb.ScopeSpans, elems ...*tracepb.ScopeSpans) []*tracepb.ScopeSpans {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		targetScopeSpansIndex := slices.IndexFunc(dst, func(dstElem *tracepb.ScopeSpans) bool {
			if dstElem == nil {
				return false
			}
			return EqualScope(dstElem.GetScope(), elem.GetScope())
		})
		if targetScopeSpansIndex == -1 {
			dst = append(dst, elem)
			continue
		}
		dst[targetScopeSpansIndex].Spans = append(dst[targetScopeSpansIndex].GetSpans(), elem.GetSpans()...)
	}
	return dst
}

// AppendResourceMetrics converts a list of ResourceMetrics into a list of ResourceMetrics with unique resources.
func AppendResourceMetrics(dst []*metricspb.ResourceMetrics, elems ...*metricspb.ResourceMetrics) []*metricspb.ResourceMetrics {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		targetResourceMetricsIndex := slices.IndexFunc(dst, func(dstElem *metricspb.ResourceMetrics) bool {
			if dstElem == nil {
				return false
			}
			return EqualResource(dstElem.GetResource(), elem.GetResource())
		})
		if targetResourceMetricsIndex == -1 {
			dst = append(dst, elem)
			continue
		}
		dst[targetResourceMetricsIndex].ScopeMetrics = AppendScopeMetrics(dst[targetResourceMetricsIndex].GetScopeMetrics(), elem.GetScopeMetrics()...)
	}
	return dst
}

func AppendScopeMetrics(dst []*metricspb.ScopeMetrics, elems ...*metricspb.ScopeMetrics) []*metricspb.ScopeMetrics {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		targetScopeMetricsIndex := slices.IndexFunc(dst, func(dstElem *metricspb.ScopeMetrics) bool {
			if dstElem == nil {
				return false
			}
			return EqualScope(dstElem.GetScope(), elem.GetScope())
		})
		if targetScopeMetricsIndex == -1 {
			dst = append(dst, elem)
			continue
		}
		dst[targetScopeMetricsIndex].Metrics = AppendMetrics(dst[targetScopeMetricsIndex].GetMetrics(), elem.GetMetrics()...)
	}
	return dst
}

func AppendMetrics(dst []*metricspb.Metric, elems ...*metricspb.Metric) []*metricspb.Metric {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		targetMetricIndex := slices.IndexFunc(dst, func(dstElem *metricspb.Metric) bool {
			if dstElem == nil {
				return false
			}
			return EqualMetric(dstElem, elem)
		})
		if targetMetricIndex == -1 {
			dst = append(dst, elem)
			continue
		}
		dst[targetMetricIndex] = AppendMetricData(dst[targetMetricIndex], elem)
	}
	return dst
}

func AppendMetricData(dst *metricspb.Metric, elem *metricspb.Metric) *metricspb.Metric {
	if dst == nil {
		return elem
	}
	if elem == nil {
		return dst
	}
	switch data := dst.GetData().(type) {
	case *metricspb.Metric_Gauge:
		data.Gauge.DataPoints = append(data.Gauge.GetDataPoints(), elem.GetGauge().GetDataPoints()...)
		dst.Data = data
	case *metricspb.Metric_Sum:
		data.Sum.DataPoints = append(data.Sum.GetDataPoints(), elem.GetSum().GetDataPoints()...)
		dst.Data = data
	case *metricspb.Metric_Summary:
		data.Summary.DataPoints = append(data.Summary.GetDataPoints(), elem.GetSummary().GetDataPoints()...)
		dst.Data = data
	case *metricspb.Metric_Histogram:
		data.Histogram.DataPoints = append(data.Histogram.GetDataPoints(), elem.GetHistogram().GetDataPoints()...)
		dst.Data = data
	case *metricspb.Metric_ExponentialHistogram:
		data.ExponentialHistogram.DataPoints = append(data.ExponentialHistogram.GetDataPoints(), elem.GetExponentialHistogram().GetDataPoints()...)
		dst.Data = data
	}
	return dst
}

func AppendResourceLogs(dst []*logspb.ResourceLogs, elems ...*logspb.ResourceLogs) []*logspb.ResourceLogs {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		targetResourceLogsIndex := slices.IndexFunc(dst, func(dstElem *logspb.ResourceLogs) bool {
			if dstElem == nil {
				return false
			}
			return EqualResource(dstElem.GetResource(), elem.GetResource())
		})
		if targetResourceLogsIndex == -1 {
			dst = append(dst, elem)
			continue
		}
		dst[targetResourceLogsIndex].ScopeLogs = AppendScopeLogs(dst[targetResourceLogsIndex].GetScopeLogs(), elem.GetScopeLogs()...)
	}
	return dst
}

func AppendScopeLogs(dst []*logspb.ScopeLogs, elems ...*logspb.ScopeLogs) []*logspb.ScopeLogs {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		targetScopeLogsIndex := slices.IndexFunc(dst, func(dstElem *logspb.ScopeLogs) bool {
			if dstElem == nil {
				return false
			}
			return EqualScope(dstElem.GetScope(), elem.GetScope())
		})
		if targetScopeLogsIndex == -1 {
			dst = append(dst, elem)
			continue
		}
		dst[targetScopeLogsIndex].LogRecords = append(dst[targetScopeLogsIndex].GetLogRecords(), elem.GetLogRecords()...)
	}
	return dst
}

func EqualResource(resource1 *resourcepb.Resource, resource2 *resourcepb.Resource) bool {
	if resource1 == nil || resource2 == nil {
		return resource1 == resource2
	}
	if resource1.GetDroppedAttributesCount() != resource2.GetDroppedAttributesCount() {
		return false
	}
	return EqualAttributes(resource1.GetAttributes(), resource2.GetAttributes())
}

func EqualScope(scope1 *commonpb.InstrumentationScope, scope2 *commonpb.InstrumentationScope) bool {
	if scope1 == nil || scope2 == nil {
		return scope1 == scope2
	}
	if scope1.GetDroppedAttributesCount() != scope2.GetDroppedAttributesCount() {
		return false
	}
	if scope1.GetName() != scope2.GetName() {
		return false
	}
	if scope1.GetVersion() != scope2.GetVersion() {
		return false
	}
	return EqualAttributes(scope1.GetAttributes(), scope2.GetAttributes())
}

func EqualAttributes(attrs1 []*commonpb.KeyValue, attrs2 []*commonpb.KeyValue) bool {
	if len(attrs1) != len(attrs2) {
		return false
	}
	attr1Map := toAttributesMap(attrs1)
	attr2Map := toAttributesMap(attrs2)
	for key, value1 := range attr1Map {
		value2, ok := attr2Map[key]
		if !ok {
			return false
		}
		if value1.String() != value2.String() {
			return false
		}
	}
	return true
}

func toAttributesMap(attrs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	attrMap := make(map[string]*commonpb.AnyValue, len(attrs))
	for _, attr := range attrs {
		attrMap[attr.GetKey()] = attr.GetValue()
	}
	return attrMap
}

func EqualMetric(metric1 *metricspb.Metric, metric2 *metricspb.Metric) bool {
	if metric1 == nil || metric2 == nil {
		return metric1 == metric2
	}
	if metric1.GetName() != metric2.GetName() {
		return false
	}
	if metric1.GetDescription() != metric2.GetDescription() {
		return false
	}
	if metric1.GetUnit() != metric2.GetUnit() {
		return false
	}
	if metricTypeString(metric1) != metricTypeString(metric2) {
		return false
	}
	return true
}

func metricTypeString(metric *metricspb.Metric) string {
	switch data := metric.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return "Gauge"
	case *metricspb.Metric_Summary:
		return "Summary"
	case *metricspb.Metric_Sum:
		return metricSumString(data)
	case *metricspb.Metric_Histogram:
		return metricHistogramString(data)
	case *metricspb.Metric_ExponentialHistogram:
		return metricExponentialHistogramString(data)
	default:
		return "Unknown"
	}
}

func metricSumString(metric *metricspb.Metric_Sum) string {
	prefix := ""
	if metric.Sum.GetIsMonotonic() {
		prefix = "Monotonic"
	}
	switch metric.Sum.GetAggregationTemporality() {
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED:
		return prefix + "Sum"
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return prefix + "CumulativeSum"
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return prefix + "DeltaSum"
	}
	return prefix + "Sum"
}

func metricHistogramString(metric *metricspb.Metric_Histogram) string {
	prefix := ""
	switch metric.Histogram.GetAggregationTemporality() {
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED:
		return prefix + "Histogram"
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return prefix + "CumulativeHistogram"
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return prefix + "DeltaHistogram"
	}
	return prefix + "Histogram"
}

func metricExponentialHistogramString(metric *metricspb.Metric_ExponentialHistogram) string {
	prefix := ""
	switch metric.ExponentialHistogram.GetAggregationTemporality() {
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED:
		return prefix + "ExponentialHistogram"
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return prefix + "CumulativeExponentialHistogram"
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return prefix + "DeltaExponentialHistogram"
	}
	return prefix + "ExponentialHistogram"
}
