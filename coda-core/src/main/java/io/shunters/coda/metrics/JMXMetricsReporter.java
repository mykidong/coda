package io.shunters.coda.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

public class JMXMetricsReporter implements MetricsReporter {
	
	private MetricRegistry metricRegistry;

	public JMXMetricsReporter(MetricRegistry metricRegistry)
	{
		this.metricRegistry = metricRegistry;
	}

	@Override
	public void start() {
		JmxReporter reporter = JmxReporter.forRegistry(this.metricRegistry).build();
		reporter.start();
	}
}
