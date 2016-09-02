package io.shunters.coda.processor;

import com.codahale.metrics.MetricRegistry;
import io.shunters.coda.command.RequestByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class WriteChannelProcessor extends Thread {

    private static Logger log = LoggerFactory.getLogger(WriteChannelProcessor.class);

    private BlockingQueue<SocketChannel> queue;
    private NioSelector nioSelector;
    private MetricRegistry metricRegistry;

    public WriteChannelProcessor(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        this.queue = new LinkedBlockingQueue<>();
        this.nioSelector = NioSelector.open();
    }

    public void put(SocketChannel socketChannel) {
        this.queue.add(socketChannel);
        this.nioSelector.wakeup();
    }

    public NioSelector getNioSelector()
    {
        return this.nioSelector;
    }

    @Override
    public void run() {

        try {
            while (true) {
                SocketChannel socketChannel = this.queue.poll();

                // if new connection is added, register it to selector.
                if (socketChannel != null) {
                    String channelId = NioSelector.makeChannelId(socketChannel);
                    nioSelector.register(channelId, socketChannel, SelectionKey.OP_WRITE);
                }

                int ready = this.nioSelector.select();
                if (ready == 0) {
                    continue;
                }

                Iterator<SelectionKey> iter = this.nioSelector.selectedKeys().iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();

                    iter.remove();

                    if(key.isWritable())
                    {
                        this.response(key);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void response(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer buffer = (ByteBuffer) key.attachment();

        if(buffer == null)
        {
            return;
        }

        buffer.rewind();

        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }

        buffer.clear();

        this.metricRegistry.meter("WriteChannelProcessor.write").mark();
    }
}
