package io.shunters.coda.server;

import com.codahale.metrics.MetricRegistry;
import io.shunters.coda.config.ConfigHandler;
import io.shunters.coda.config.YamlConfigHandler;
import io.shunters.coda.discovery.ConsulServiceDiscovery;
import io.shunters.coda.discovery.ConsulSessionHolder;
import io.shunters.coda.discovery.ServiceDiscovery;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.metrics.MetricsReporter;
import io.shunters.coda.metrics.SystemOutMetricsReporter;
import io.shunters.coda.processor.ChannelProcessor;
import io.shunters.coda.util.NetworkUtils;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 *  Request / Response Process Flow Example:
 *
 *
 *
 *        [Channel Processor] --> [Disruptor] --> [Request Processor] --> [Disruptor] --> [Store Processor]
 *                 ^                                       |                                        |
 *                 |                                       |                                        |
 *                 |                              [Fetch Request Handler]                  [Produce Request Handler]
 *                 |                                       |                                        |
 *                 |                                       |                                        |
 *                 |                                       + ---------------> [Disruptor] <-------- +
 *                 |                                                               |
 *                 |                                                               |
 *                 |          (wake up)                                            v
 *                 + ----------------------------------------------------- [Response Processor]
 *
 *
 *
 *
 * Created by mykidong on 2016-08-23.
 */
public class CodaServer implements Runnable {

    private static Logger log = LoggerFactory.getLogger(CodaServer.class);

    private Selector selector;

    private int port;

    private MetricRegistry metricRegistry;

    private MetricsReporter metricsReporter;

    private List<ChannelProcessor> channelProcessors;

    private int channelProcessorSize;

    private Random random = new Random();

    private ServiceDiscovery serviceDiscovery;

    private ConfigHandler configHandler;

    public CodaServer(int port, int channelProcessorSize) {

        configHandler = YamlConfigHandler.getConfigHandler();

        String log4jXmlPath = "/" + (String) configHandler.get(ConfigHandler.CONFIG_LOG4J_XML_PATH);

        // log4j init.
        DOMConfigurator.configure(this.getClass().getResource(log4jXmlPath));

        this.port = port;

        this.channelProcessorSize = channelProcessorSize;

        metricRegistry = MetricRegistryFactory.getInstance();

        // std out reporter for metrics.
        metricsReporter = new SystemOutMetricsReporter(metricRegistry);
        metricsReporter.start();

        channelProcessors = new ArrayList<>();
        for (int i = 0; i < channelProcessorSize; i++) {
            ChannelProcessor channelProcessor = new ChannelProcessor(this.metricRegistry);
            channelProcessor.start();

            channelProcessors.add(channelProcessor);
        }

        // register service onto consul.
        String hostName = NetworkUtils.getSimpleHostName();
        String hostPort = hostName + ":" + port;
        String serviceId = hostName + "-" + port;

        this.serviceDiscovery = ConsulServiceDiscovery.getConsulServiceDiscovery();
        serviceDiscovery.createService(ServiceDiscovery.SERVICE_NAME, serviceId, null, hostName, port, null, hostPort, "10s", "1s");
        log.info("consul service [" + ServiceDiscovery.SERVICE_NAME + ":" + serviceId + "] registered.");

        // run consul session holder to elect leader.
        new ConsulSessionHolder(ServiceDiscovery.SESSION_CODA_LOCK, ServiceDiscovery.LEADER_KEY, hostName, port, 10);
    }

    private ChannelProcessor getNextChannelProcessor() {
        int randomIndex = random.nextInt(this.channelProcessorSize);

        return this.channelProcessors.get(randomIndex);
    }


    @Override
    public void run() {
        try {
            this.selector = Selector.open();

            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(this.port));
            serverSocketChannel.configureBlocking(false);

            // server socket registered for accept.
            serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);

            log.info("coda server is listening on [{}]...", this.port);

            while (true) {
                int readyChannels = this.selector.select();
                if (readyChannels == 0) {
                    continue;
                }

                Iterator<SelectionKey> iter = this.selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();

                    iter.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        this.accept(key);
                    }
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }


    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setKeepAlive(true);

        log.info("socket channel accepted: [{}]", socketChannel.socket().getRemoteSocketAddress());

        // put socket channel to read channel processor.
        this.getNextChannelProcessor().put(socketChannel);
    }
}
