package io.shunters.coda;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.metrics.MetricsReporter;
import io.shunters.coda.metrics.SystemOutMetricsReporter;
import io.shunters.coda.selector.DisruptorSingleton;
import io.shunters.coda.selector.SelectorEvent;
import io.shunters.coda.selector.SelectorEventTranslator;
import io.shunters.coda.selector.SelectorHandler;
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
 * Created by mykidong on 2016-08-23.
 */
public class Broker implements Runnable{

    private static Logger log = LoggerFactory.getLogger(Broker.class);

    private Selector selector;

    private int port;

    private Disruptor<SelectorEvent> selectorEventDisruptor;

    private SelectorEventTranslator selectorEventTranslator;

    private MetricRegistry metricRegistry;

    private MetricsReporter metricsReporter;

    private List<ChannelProcessor> channelProcessors;

    private int channelProcessorSize;

    private Random random = new Random();

    public Broker(int port, int channelProcessorSize) {

        // TODO: log4j init. should be configurable.
        // log4j init.
        java.net.URL url = this.getClass().getResource("/log4j.xml");
        System.out.println("log4j url: " + url.toString());
        DOMConfigurator.configure(url);

        this.port = port;

        this.channelProcessorSize = channelProcessorSize;

        metricRegistry = MetricRegistryFactory.getInstance();

        // std out reporter for metrics.
        metricsReporter = new SystemOutMetricsReporter(metricRegistry);
        metricsReporter.start();

        channelProcessors = new ArrayList<>();
        for(int i = 0; i < channelProcessorSize; i++)
        {
            ChannelProcessor channelProcessor = new ChannelProcessor(this.metricRegistry);
            channelProcessor.start();
            channelProcessors.add(channelProcessor);
        }
    }

    private ChannelProcessor getNextChannelProcessor()
    {
        int randomIndex = random.nextInt(this.channelProcessorSize);

        return this.channelProcessors.get(randomIndex);
    }

    @Override
    public void run()  {
        try {
            this.selector = Selector.open();

            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(this.port));
            serverSocketChannel.configureBlocking(false);

            // server socket registered for accept.
            serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);

            log.info("broker is listening on [{}]...", this.port);

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

//                    else if (key.isReadable()) {
//                        this.publishEvent(key);
//                    } else if (key.isWritable()) {
//                        this.publishEvent(key);
//                    }
                }
            }
        }catch (IOException e)
        {
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

        this.getNextChannelProcessor().put(socketChannel);
    }

    private void publishEvent(SelectionKey key) {
        this.publishEvent(key, (SocketChannel) key.channel());
    }

    private void publishEvent(SelectionKey key, SocketChannel socketChannel) {
        this.selectorEventTranslator.setKey(key);
        this.selectorEventTranslator.setSocketChannel(socketChannel);
        this.selectorEventDisruptor.publishEvent(this.selectorEventTranslator);
    }

}
