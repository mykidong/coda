package io.shunters.coda.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class SystemOutMetricsReporter implements MetricsReporter{
	
	private MetricRegistry metricRegistry;

	public SystemOutMetricsReporter(MetricRegistry metricRegistry)
	{
		this.metricRegistry = metricRegistry;
	}

	@Override
	public void start() {
		ConsoleReporter reporter = ConsoleReporter.forRegistry(this.metricRegistry)
			       .convertRatesTo(TimeUnit.SECONDS)
			       .convertDurationsTo(TimeUnit.MILLISECONDS)
			       .build();
		reporter.start(1, TimeUnit.SECONDS);
	}
}
