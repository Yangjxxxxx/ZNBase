package event

//AuditMetrics define the metric interface
type AuditMetrics interface {
	Metric(info interface{})
	RegistMetric() interface{}
}
