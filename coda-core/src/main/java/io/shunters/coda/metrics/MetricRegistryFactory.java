package io.shunters.coda.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricRegistryFactory {

	private static MetricRegistry metricRegistry;

	private static final Object lock = new Object();

	public static MetricRegistry getInstance()
	{
		if(metricRegistry == null)
		{
			synchronized (lock)
			{
				if(metricRegistry == null)
				{
					metricRegistry = new MetricRegistry();
				}
			}
		}

		return metricRegistry;
	}
}