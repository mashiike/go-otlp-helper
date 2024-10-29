package otlp

import (
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// PartitionResourceSpans partitions the given ResourceSpans slice into multiple ResourceSpans slices, each containing only one Span, based on the given partition key.
func PartitionResourceSpans(src []*tracepb.ResourceSpans, getPartitionKey func(*tracepb.ResourceSpans) string) map[string][]*tracepb.ResourceSpans {
	m := make(map[string][]*tracepb.ResourceSpans)
	for _, elem := range SplitResourceSpans(src) {
		key := getPartitionKey(elem)
		m[key] = AppendResourceSpans(m[key], elem)
	}
	return m
}

// PartitionBySpanStartTime returns a function that partitions ResourceSpans by Span start time.
func PartitionBySpanStartTime(format string, tz *time.Location) func(*tracepb.ResourceSpans) string {
	return func(rspans *tracepb.ResourceSpans) string {
		if tz == nil {
			tz = time.UTC
		}
		scopeSpans := rspans.GetScopeSpans()
		if len(scopeSpans) == 0 {
			return ""
		}
		spans := scopeSpans[0].GetSpans()
		if len(spans) == 0 {
			return ""
		}
		return time.Unix(0, int64(spans[0].GetStartTimeUnixNano())).In(tz).Format(format)
	}
}

// PartitionBySpanEndTime returns a function that partitions ResourceSpans by Span end time.
func PartitionBySpanEndTime(format string, tz *time.Location) func(*tracepb.ResourceSpans) string {
	return func(rspans *tracepb.ResourceSpans) string {
		if tz == nil {
			tz = time.UTC
		}
		scopeSpans := rspans.GetScopeSpans()
		if len(scopeSpans) == 0 {
			return ""
		}
		spans := scopeSpans[0].GetSpans()
		if len(spans) == 0 {
			return ""
		}
		return time.Unix(0, int64(spans[0].GetEndTimeUnixNano())).In(tz).Format(format)
	}
}

const (
	Yearly  = "2006"
	Monthly = "2006/01"
	Daily   = "2006/01/02"
	Hourly  = "2006/01/02/15"
)

// TotalSpans returns the total number of spans in the given ResourceSpans slice.
func TotalSpans(src []*tracepb.ResourceSpans) int {
	total := 0
	for _, elem := range src {
		for _, elemScopeSpan := range elem.GetScopeSpans() {
			total += len(elemScopeSpan.GetSpans())
		}
	}
	return total
}

// SpanInTimeRangeFilter returns a filter function that filters spans based on the given time range.
func SpanInTimeRangeFilter(start, end time.Time) func(*resourcepb.Resource, *commonpb.InstrumentationScope, *tracepb.Span) bool {
	return func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, span *tracepb.Span) bool {
		spanStart := time.Unix(0, int64(span.GetStartTimeUnixNano()))
		spanEnd := time.Unix(0, int64(span.GetEndTimeUnixNano()))
		return spanStart.After(start) && spanEnd.Before(end)
	}
}

// FilterResourceSpans filters the given ResourceSpans slice based on the given filter function.
func FilterResourceSpans(src []*tracepb.ResourceSpans, filters ...func(*resourcepb.Resource, *commonpb.InstrumentationScope, *tracepb.Span) bool) []*tracepb.ResourceSpans {
	filter := andFilter(filters...)
	splited := SplitResourceSpans(src)
	filtered := make([]*tracepb.ResourceSpans, 0, len(splited))
	for _, elem := range splited {
		resource := elem.GetResource()
		for _, elemScopeSpan := range elem.GetScopeSpans() {
			scope := elemScopeSpan.GetScope()
			for _, elemSpan := range elemScopeSpan.GetSpans() {
				if filter(resource, scope, elemSpan) {
					filtered = append(filtered, elem)
				}
			}
		}
	}
	return filtered
}

// SplitResourceSpans splits the given ResourceSpans slice into multiple ResourceSpans slices, each containing only one Span.
func SplitResourceSpans(src []*tracepb.ResourceSpans) []*tracepb.ResourceSpans {
	dst := make([]*tracepb.ResourceSpans, 0, TotalSpans(src))
	for _, elem := range src {
		for _, elemScopeSpan := range splitScopeSpans(elem.GetScopeSpans()) {
			dst = append(dst, &tracepb.ResourceSpans{
				Resource:   elem.GetResource(),
				ScopeSpans: []*tracepb.ScopeSpans{elemScopeSpan},
				SchemaUrl:  elem.GetSchemaUrl(),
			})
		}
	}
	return dst
}

func splitScopeSpans(src []*tracepb.ScopeSpans) []*tracepb.ScopeSpans {
	dst := make([]*tracepb.ScopeSpans, 0, len(src))
	for _, elem := range src {
		for _, elemSpan := range elem.GetSpans() {
			dst = append(dst, &tracepb.ScopeSpans{
				Scope:     elem.GetScope(),
				Spans:     []*tracepb.Span{elemSpan},
				SchemaUrl: elem.GetSchemaUrl(),
			})
		}
	}
	return dst
}

// PartitionResourceMetrics partitions the given ResourceMetrics slice into multiple ResourceMetrics slices, each containing only one data point, based on the given partition key.
func PartitionResourceMetrics(src []*metricspb.ResourceMetrics, getPartitionKey func(*metricspb.ResourceMetrics) string) map[string][]*metricspb.ResourceMetrics {
	m := make(map[string][]*metricspb.ResourceMetrics)
	for _, elem := range SplitResourceMetrics(src) {
		key := getPartitionKey(elem)
		m[key] = AppendResourceMetrics(m[key], elem)
	}
	return m
}

func PartitionByMetricType() func(*metricspb.ResourceMetrics) string {
	return func(rmetrics *metricspb.ResourceMetrics) string {
		scopeMetrics := rmetrics.GetScopeMetrics()
		if len(scopeMetrics) == 0 {
			return ""
		}
		metrics := scopeMetrics[0].GetMetrics()
		if len(metrics) == 0 {
			return ""
		}
		switch metrics[0].GetData().(type) {
		case *metricspb.Metric_Gauge:
			return "Gauge"
		case *metricspb.Metric_Sum:
			return "Sum"
		case *metricspb.Metric_Summary:
			return "Summary"
		case *metricspb.Metric_Histogram:
			return "Histogram"
		case *metricspb.Metric_ExponentialHistogram:
			return "ExponentialHistogram"
		}
		return ""
	}
}

// PartitionByMetricStartTime returns a function that partitions ResourceMetrics by Metric start time.
func PartitionByMetricStartTime(format string, tz *time.Location) func(*metricspb.ResourceMetrics) string {
	return func(rmetrics *metricspb.ResourceMetrics) string {
		if tz == nil {
			tz = time.UTC
		}
		scopeMetrics := rmetrics.GetScopeMetrics()
		if len(scopeMetrics) == 0 {
			return ""
		}
		metrics := scopeMetrics[0].GetMetrics()
		if len(metrics) == 0 {
			return ""
		}
		switch data := metrics[0].GetData().(type) {
		case *metricspb.Metric_Gauge:
			dataPoints := data.Gauge.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}

			return time.Unix(0, int64(dataPoints[0].GetStartTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_Sum:
			dataPoints := data.Sum.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetStartTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_Summary:
			dataPoints := data.Summary.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetStartTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_Histogram:
			dataPoints := data.Histogram.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetStartTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_ExponentialHistogram:
			dataPoints := data.ExponentialHistogram.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetStartTimeUnixNano())).In(tz).Format(format)
		}
		return ""
	}
}

// PartitionByMetricTime returns a function that partitions ResourceMetrics by Metric time.
func PartitionByMetricTime(format string, tz *time.Location) func(*metricspb.ResourceMetrics) string {
	return func(rmetrics *metricspb.ResourceMetrics) string {
		if tz == nil {
			tz = time.UTC
		}
		scopeMetrics := rmetrics.GetScopeMetrics()
		if len(scopeMetrics) == 0 {
			return ""
		}
		metrics := scopeMetrics[0].GetMetrics()
		if len(metrics) == 0 {
			return ""
		}
		switch data := metrics[0].GetData().(type) {
		case *metricspb.Metric_Gauge:
			dataPoints := data.Gauge.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}

			return time.Unix(0, int64(dataPoints[0].GetTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_Sum:
			dataPoints := data.Sum.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_Summary:
			dataPoints := data.Summary.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_Histogram:
			dataPoints := data.Histogram.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetTimeUnixNano())).In(tz).Format(format)
		case *metricspb.Metric_ExponentialHistogram:
			dataPoints := data.ExponentialHistogram.GetDataPoints()
			if len(dataPoints) == 0 {
				return ""
			}
			return time.Unix(0, int64(dataPoints[0].GetTimeUnixNano())).In(tz).Format(format)
		}
		return ""
	}
}

// TotalDataPoints returns the total number of data points in the given ResourceMetrics slice.
func TotalDataPoints(src []*metricspb.ResourceMetrics) int {
	total := 0
	for _, elem := range src {
		for _, elemScopeMetric := range elem.GetScopeMetrics() {
			for _, elemMetric := range elemScopeMetric.GetMetrics() {
				switch data := elemMetric.GetData().(type) {
				case *metricspb.Metric_Gauge:
					total += len(data.Gauge.GetDataPoints())
				case *metricspb.Metric_Summary:
					total += len(data.Summary.GetDataPoints())
				case *metricspb.Metric_Sum:
					total += len(data.Sum.GetDataPoints())
				case *metricspb.Metric_Histogram:
					total += len(data.Histogram.GetDataPoints())
				case *metricspb.Metric_ExponentialHistogram:
					total += len(data.ExponentialHistogram.GetDataPoints())
				}
			}
		}
	}
	return total
}

// MetricInTimeRangeFilter returns a filter function that filters metrics based on the given time range.
//
//nolint:gocyclo
func MetricDataPointInTimeRangeFilter(start, end time.Time) func(*resourcepb.Resource, *commonpb.InstrumentationScope, *metricspb.Metric) bool {
	return func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, metric *metricspb.Metric) bool {
		switch data := metric.GetData().(type) {
		case *metricspb.Metric_Gauge:
			for _, elemDataPoint := range data.Gauge.GetDataPoints() {
				t := time.Unix(0, int64(elemDataPoint.GetTimeUnixNano()))
				if t.After(start) && t.Before(end) {
					return true
				}
			}
		case *metricspb.Metric_Sum:
			for _, elemDataPoint := range data.Sum.GetDataPoints() {
				t := time.Unix(0, int64(elemDataPoint.GetTimeUnixNano()))
				if t.After(start) && t.Before(end) {
					return true
				}
			}
		case *metricspb.Metric_Summary:
			for _, elemDataPoint := range data.Summary.GetDataPoints() {
				t := time.Unix(0, int64(elemDataPoint.GetTimeUnixNano()))
				if t.After(start) && t.Before(end) {
					return true
				}
			}
		case *metricspb.Metric_Histogram:
			for _, elemDataPoint := range data.Histogram.GetDataPoints() {
				t := time.Unix(0, int64(elemDataPoint.GetTimeUnixNano()))
				if t.After(start) && t.Before(end) {
					return true
				}
			}
		case *metricspb.Metric_ExponentialHistogram:
			for _, elemDataPoint := range data.ExponentialHistogram.GetDataPoints() {
				t := time.Unix(0, int64(elemDataPoint.GetTimeUnixNano()))
				if t.After(start) && t.Before(end) {
					return true
				}
			}
		}
		return false
	}
}

// FilterResourceMetrics filters the given ResourceMetrics slice based on the given filter function.
func FilterResourceMetrics(src []*metricspb.ResourceMetrics, filters ...func(*resourcepb.Resource, *commonpb.InstrumentationScope, *metricspb.Metric) bool) []*metricspb.ResourceMetrics {
	filter := andFilter(filters...)
	splited := SplitResourceMetrics(src)
	filtered := make([]*metricspb.ResourceMetrics, 0, len(splited))
	for _, elem := range splited {
		resource := elem.GetResource()
		for _, elemScopeMetric := range elem.GetScopeMetrics() {
			scope := elemScopeMetric.GetScope()
			for _, elemMetric := range elemScopeMetric.GetMetrics() {
				if filter(resource, scope, elemMetric) {
					filtered = append(filtered, elem)
				}
			}
		}
	}
	return filtered
}

// SplitResourceMetrics splits the given ResourceMetrics slice into multiple ResourceMetrics slices, each containing only one data point.
func SplitResourceMetrics(src []*metricspb.ResourceMetrics) []*metricspb.ResourceMetrics {
	dst := make([]*metricspb.ResourceMetrics, 0, TotalDataPoints(src))
	for _, elem := range src {
		for _, elemScopeMetric := range splitScopeMetrics(elem.GetScopeMetrics()) {
			dst = append(dst, &metricspb.ResourceMetrics{
				Resource:     elem.GetResource(),
				ScopeMetrics: []*metricspb.ScopeMetrics{elemScopeMetric},
				SchemaUrl:    elem.GetSchemaUrl(),
			})
		}
	}
	return dst
}

func splitScopeMetrics(src []*metricspb.ScopeMetrics) []*metricspb.ScopeMetrics {
	dst := make([]*metricspb.ScopeMetrics, 0, len(src))
	for _, elem := range src {
		for _, elemMetric := range splitMetrics(elem.GetMetrics()) {
			dst = append(dst, &metricspb.ScopeMetrics{
				Scope:     elem.GetScope(),
				Metrics:   []*metricspb.Metric{elemMetric},
				SchemaUrl: elem.GetSchemaUrl(),
			})
		}
	}
	return dst
}

func splitMetrics(src []*metricspb.Metric) []*metricspb.Metric {
	dst := make([]*metricspb.Metric, 0, len(src))
	for _, elem := range src {
		switch data := elem.GetData().(type) {
		case *metricspb.Metric_Gauge:
			for _, elemDataPoint := range data.Gauge.GetDataPoints() {
				dst = append(dst, &metricspb.Metric{
					Name:        elem.GetName(),
					Description: elem.GetDescription(),
					Unit:        elem.GetUnit(),
					Metadata:    elem.GetMetadata(),
					Data: &metricspb.Metric_Gauge{
						Gauge: &metricspb.Gauge{
							DataPoints: []*metricspb.NumberDataPoint{elemDataPoint},
						},
					},
				})
			}
		case *metricspb.Metric_Sum:
			for _, elemDataPoint := range data.Sum.GetDataPoints() {
				dst = append(dst, &metricspb.Metric{
					Name:        elem.GetName(),
					Description: elem.GetDescription(),
					Unit:        elem.GetUnit(),
					Metadata:    elem.GetMetadata(),
					Data: &metricspb.Metric_Sum{
						Sum: &metricspb.Sum{
							AggregationTemporality: data.Sum.GetAggregationTemporality(),
							IsMonotonic:            data.Sum.GetIsMonotonic(),
							DataPoints:             []*metricspb.NumberDataPoint{elemDataPoint},
						},
					},
				})
			}
		case *metricspb.Metric_Summary:
			for _, elemDataPoint := range data.Summary.GetDataPoints() {
				dst = append(dst, &metricspb.Metric{
					Name:        elem.GetName(),
					Description: elem.GetDescription(),
					Unit:        elem.GetUnit(),
					Metadata:    elem.GetMetadata(),
					Data: &metricspb.Metric_Summary{
						Summary: &metricspb.Summary{
							DataPoints: []*metricspb.SummaryDataPoint{elemDataPoint},
						},
					},
				})
			}
		case *metricspb.Metric_Histogram:
			for _, elemDataPoint := range data.Histogram.GetDataPoints() {
				dst = append(dst, &metricspb.Metric{
					Name:        elem.GetName(),
					Description: elem.GetDescription(),
					Unit:        elem.GetUnit(),
					Metadata:    elem.GetMetadata(),
					Data: &metricspb.Metric_Histogram{
						Histogram: &metricspb.Histogram{
							AggregationTemporality: data.Histogram.GetAggregationTemporality(),
							DataPoints:             []*metricspb.HistogramDataPoint{elemDataPoint},
						},
					},
				})
			}
		case *metricspb.Metric_ExponentialHistogram:
			for _, elemDataPoint := range data.ExponentialHistogram.GetDataPoints() {
				dst = append(dst, &metricspb.Metric{
					Name:        elem.GetName(),
					Description: elem.GetDescription(),
					Unit:        elem.GetUnit(),
					Metadata:    elem.GetMetadata(),
					Data: &metricspb.Metric_ExponentialHistogram{
						ExponentialHistogram: &metricspb.ExponentialHistogram{
							AggregationTemporality: data.ExponentialHistogram.GetAggregationTemporality(),
							DataPoints:             []*metricspb.ExponentialHistogramDataPoint{elemDataPoint},
						},
					},
				})
			}
		}
	}
	return dst
}

// PartitionResourceLogs partitions the given ResourceLogs slice into multiple ResourceLogs slices, each containing only one log record, based on the given partition key.
func PartitionResourceLogs(src []*logspb.ResourceLogs, getPartitionKey func(*logspb.ResourceLogs) string) map[string][]*logspb.ResourceLogs {
	m := make(map[string][]*logspb.ResourceLogs)
	for _, elem := range SplitResourceLogs(src) {
		key := getPartitionKey(elem)
		m[key] = AppendResourceLogs(m[key], elem)
	}
	return m
}

// PartitionByLogTime returns a function that partitions ResourceLogs by Log time.
func PartitionByLogTime(format string, tz *time.Location) func(*logspb.ResourceLogs) string {
	return func(rlogs *logspb.ResourceLogs) string {
		if tz == nil {
			tz = time.UTC
		}
		scopeLogs := rlogs.GetScopeLogs()
		if len(scopeLogs) == 0 {
			return ""
		}
		logRecords := scopeLogs[0].GetLogRecords()
		if len(logRecords) == 0 {
			return ""
		}
		logRecords[0].GetSeverityText()
		return time.Unix(0, int64(logRecords[0].GetTimeUnixNano())).In(tz).Format(format)
	}
}

// PartitionByLogSeverityNumber returns a function that partitions ResourceLogs by Log severity number.
func PartitionByLogSeverityNumber() func(*logspb.ResourceLogs) string {
	return func(rlogs *logspb.ResourceLogs) string {
		scopeLogs := rlogs.GetScopeLogs()
		if len(scopeLogs) == 0 {
			return ""
		}
		logRecords := scopeLogs[0].GetLogRecords()
		if len(logRecords) == 0 {
			return ""
		}
		return logRecords[0].GetSeverityText()
	}
}

// PartitionByLogSeverityText returns a function that partitions ResourceLogs by Log severity text.
func PartitionByLogSeverityText() func(*logspb.ResourceLogs) string {
	return func(rlogs *logspb.ResourceLogs) string {
		scopeLogs := rlogs.GetScopeLogs()
		if len(scopeLogs) == 0 {
			return ""
		}
		logRecords := scopeLogs[0].GetLogRecords()
		if len(logRecords) == 0 {
			return ""
		}
		return logRecords[0].GetSeverityText()
	}
}

// PartitionByLogObservedTime returns a function that partitions ResourceLogs by Log observation time.
func PartitionByLogObservedTime(format string, tz *time.Location) func(*logspb.ResourceLogs) string {
	return func(rlogs *logspb.ResourceLogs) string {
		if tz == nil {
			tz = time.UTC
		}
		scopeLogs := rlogs.GetScopeLogs()
		if len(scopeLogs) == 0 {
			return ""
		}
		logRecords := scopeLogs[0].GetLogRecords()
		if len(logRecords) == 0 {
			return ""
		}
		return time.Unix(0, int64(logRecords[0].GetObservedTimeUnixNano())).In(tz).Format(format)
	}
}

// TotalLogRecords returns the total number of log records in the given ResourceLogs slice.
func TotalLogRecords(src []*logspb.ResourceLogs) int {
	total := 0
	for _, elem := range src {
		for _, elemScopeLogs := range elem.GetScopeLogs() {
			total += len(elemScopeLogs.GetLogRecords())
		}
	}
	return total
}

// SplitResourceLogs splits the given ResourceLogs slice into multiple ResourceLogs slices, each containing only one log record.
func SplitResourceLogs(src []*logspb.ResourceLogs) []*logspb.ResourceLogs {
	dst := make([]*logspb.ResourceLogs, 0, TotalLogRecords(src))
	for _, elem := range src {
		for _, elemScopeLogs := range splitScopeLogs(elem.GetScopeLogs()) {
			dst = append(dst, &logspb.ResourceLogs{
				Resource:  elem.GetResource(),
				ScopeLogs: []*logspb.ScopeLogs{elemScopeLogs},
				SchemaUrl: elem.GetSchemaUrl(),
			})
		}
	}
	return dst
}

func LogRecordInTimeRangeFilter(start, end time.Time) func(*resourcepb.Resource, *commonpb.InstrumentationScope, *logspb.LogRecord) bool {
	return func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, logRecord *logspb.LogRecord) bool {
		t := time.Unix(0, int64(logRecord.GetTimeUnixNano()))
		return t.After(start) && t.Before(end)
	}
}

// FilterResourceLogs filters the given ResourceLogs slice based on the given filter function.
func FilterResourceLogs(src []*logspb.ResourceLogs, filters ...func(*resourcepb.Resource, *commonpb.InstrumentationScope, *logspb.LogRecord) bool) []*logspb.ResourceLogs {
	filter := andFilter(filters...)
	splited := SplitResourceLogs(src)
	filtered := make([]*logspb.ResourceLogs, 0, len(splited))
	for _, elem := range splited {
		resource := elem.GetResource()
		for _, elemScopeLogs := range elem.GetScopeLogs() {
			scope := elemScopeLogs.GetScope()
			for _, elemLogRecord := range elemScopeLogs.GetLogRecords() {
				if filter(resource, scope, elemLogRecord) {
					filtered = append(filtered, elem)
				}
			}
		}
	}
	return filtered
}

func splitScopeLogs(src []*logspb.ScopeLogs) []*logspb.ScopeLogs {
	dst := make([]*logspb.ScopeLogs, 0, len(src))
	for _, elem := range src {
		for _, elemLogRecord := range elem.GetLogRecords() {
			dst = append(dst, &logspb.ScopeLogs{
				Scope:      elem.GetScope(),
				LogRecords: []*logspb.LogRecord{elemLogRecord},
				SchemaUrl:  elem.GetSchemaUrl(),
			})
		}
	}
	return dst
}

func andFilter[T any](filters ...func(*resourcepb.Resource, *commonpb.InstrumentationScope, T) bool) func(*resourcepb.Resource, *commonpb.InstrumentationScope, T) bool {
	return func(r *resourcepb.Resource, s *commonpb.InstrumentationScope, t T) bool {
		for _, f := range filters {
			if !f(r, s, t) {
				return false
			}
		}
		return true
	}
}
