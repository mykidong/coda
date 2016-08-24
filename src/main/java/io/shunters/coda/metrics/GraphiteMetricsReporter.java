package io.shunters.coda.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class GraphiteMetricsReporter implements MetricsReporter{

    private static Logger log = LoggerFactory.getLogger(GraphiteMetricsReporter.class);
	
	private MetricRegistry metricRegistry;	
	
	private String carbonHost;
	
	private int carbonPort;	
	
	private String prefix;

	public GraphiteMetricsReporter(MetricRegistry metricRegistry, String carbonHost, int carbonPort, String prefix)
	{
		this.metricRegistry = metricRegistry;
		this.carbonHost = carbonHost;
		this.carbonPort = carbonPort;
		this.prefix = prefix;
	}

	@Override
	public void start() {

        String localHostName = null;
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
            localHostName = localHostName.replaceAll("\\.", "_");
        }catch (UnknownHostException e)
        {
            log.error(e.getMessage());
        }
		
		Graphite graphite = new Graphite(new InetSocketAddress(this.carbonHost, this.carbonPort));
		GraphiteReporter reporter = GraphiteReporter.forRegistry(this.metricRegistry)
		                                                  .prefixedWith(this.prefix + "-" + localHostName)
		                                                  .convertRatesTo(TimeUnit.SECONDS)
		                                                  .convertDurationsTo(TimeUnit.MILLISECONDS)
		                                                  .filter(MetricFilter.ALL)
		                                                  .build(graphite);
		reporter.start(30, TimeUnit.SECONDS);	
	}
}
