package raft

import "go.opentelemetry.io/otel/metric"

type RaftMetric struct {
	TermGauge   metric.Int64Gauge
	LogGauge    metric.Int64Gauge
	CommitGauge metric.Int64Gauge
	StateGauge  metric.Int64Gauge
}

func NewRaftMetric(meter metric.Meter) *RaftMetric {
	TermGauge, _ := meter.Int64Gauge("Raft Term")
	LogGauge, _ := meter.Int64Gauge("Raft Log Index")
	CommitGauge, _ := meter.Int64Gauge("Raft Commit Index")
	StateGauge, _ := meter.Int64Gauge("Raft State")

	return &RaftMetric{
		TermGauge,
		LogGauge,
		CommitGauge,
		StateGauge,
	}
}
