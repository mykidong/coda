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


public class ChannelProcessor extends Thread {

    private static Logger log = LoggerFactory.getLogger(ChannelProcessor.class);

    private BlockingQueue<SocketChannel> queue;

    private NioSelector nioSelector;

    private MetricRegistry metricRegistry;

    private ToRequestProcessor toRequestProcessor;

    private WriteChannelProcessor writeChannelProcessor;

    public ChannelProcessor(MetricRegistry metricRegistry, WriteChannelProcessor writeChannelProcessor) {
        this.metricRegistry = metricRegistry;
        this.writeChannelProcessor = writeChannelProcessor;

        this.queue = new LinkedBlockingQueue<>();
        this.nioSelector = NioSelector.open();

        this.toRequestProcessor = new ToRequestProcessor();
        this.toRequestProcessor.start();
    }

    public void put(SocketChannel socketChannel) {
        this.queue.add(socketChannel);
        this.nioSelector.wakeup();

        this.writeChannelProcessor.put(socketChannel);
    }


    @Override
    public void run() {

        try {
            while (true) {
                SocketChannel socketChannel = this.queue.poll();

                // if new connection is added, register it to selector.
                if (socketChannel != null) {
                    String channelId = NioSelector.makeChannelId(socketChannel);
                    nioSelector.register(channelId, socketChannel, SelectionKey.OP_READ);
                }

                int ready = this.nioSelector.select();
                if (ready == 0) {
                    continue;
                }

                Iterator<SelectionKey> iter = this.nioSelector.selectedKeys().iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();

                    iter.remove();

                    if (key.isReadable()) {
                        this.request(key);
                    }
                }
            }
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void request(SelectionKey key) throws IOException {

        SocketChannel socketChannel = (SocketChannel) key.channel();

        // channel id.
        String channelId = NioSelector.makeChannelId(socketChannel);

        // to get total size.
        ByteBuffer totalSizeBuffer = ByteBuffer.allocate(4);
        socketChannel.read(totalSizeBuffer);

        totalSizeBuffer.rewind();

        // total size.
        int totalSize = totalSizeBuffer.getInt();

        // subsequent bytes buffer.
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        socketChannel.read(buffer);


        buffer.rewind();

        // command id.
        short commandId = buffer.getShort();

        buffer.rewind();

        NioSelector writeNioSelector = this.writeChannelProcessor.getNioSelector();
        RequestByteBuffer requestByteBuffer = new RequestByteBuffer(writeNioSelector, channelId, commandId, buffer);

        // send to ToRequestProcessor.
        this.toRequestProcessor.put(requestByteBuffer);

        this.metricRegistry.meter("ChannelProcessor.read").mark();
    }
}
