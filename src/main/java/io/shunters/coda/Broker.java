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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

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

    public Broker(int port) {
        this.port = port;

        metricRegistry = MetricRegistryFactory.getInstance();

        // std out reporter for metrics.
        metricsReporter = new SystemOutMetricsReporter(metricRegistry);
        metricsReporter.start();

        selectorEventDisruptor = DisruptorSingleton.getInstance(SelectorEvent.FACTORY, 1024, new SelectorHandler(metricRegistry));
        selectorEventTranslator = new SelectorEventTranslator();
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
                    } else if (key.isReadable()) {
                        this.publishEvent(key);
                    } else if (key.isWritable()) {
                        this.publishEvent(key);
                    }
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

        socketChannel.register(this.selector, SelectionKey.OP_READ);

        log.info("socket channel accepted: [{}]", socketChannel.socket().getRemoteSocketAddress());

        publishEvent(key, socketChannel);
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
