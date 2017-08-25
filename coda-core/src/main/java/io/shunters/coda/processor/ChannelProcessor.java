package io.shunters.coda.processor;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.util.DisruptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ChannelProcessor extends Thread {

    private static Logger log = LoggerFactory.getLogger(ChannelProcessor.class);

    private BlockingQueue<SocketChannel> queue;

    private NioSelector nioSelector;

    private MetricRegistry metricRegistry;

    /**
     * base message bytes event disruptor.
     */
    private Disruptor<BaseMessage.BaseMessageBytesEvent> baseMessageBytesEventDisruptor;

    /**
     * base message bytes event translator.
     */
    private BaseMessage.BaseMessageBytesEventTranslator baseMessageBytesEventTranslator;


    public ChannelProcessor(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;

        this.queue = new LinkedBlockingQueue<>();
        this.nioSelector = NioSelector.open();

        baseMessageBytesEventDisruptor = DisruptorBuilder.singleton("ToRequest", BaseMessage.BaseMessageBytesEvent.FACTORY, 1024, ToRequestProcessor.singleton());
        this.baseMessageBytesEventTranslator = new BaseMessage.BaseMessageBytesEventTranslator();
    }

    public void put(SocketChannel socketChannel) {
        this.queue.add(socketChannel);
        this.nioSelector.wakeup();
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
                    else if(key.isWritable())
                    {
                        this.response(key);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void request(SelectionKey key) throws IOException {

        SocketChannel socketChannel = (SocketChannel) key.channel();

        // channel id.
        String channelId = NioSelector.makeChannelId(socketChannel);

        // total size.
        ByteBuffer totalSizeBuffer = ByteBuffer.allocate(4);
        socketChannel.read(totalSizeBuffer);
        totalSizeBuffer.rewind();

        int totalSize = totalSizeBuffer.getInt();

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        socketChannel.read(buffer);
        buffer.rewind();

        // api key
        short apiKey = buffer.getShort();

        // api version.
        short apiVersion = buffer.getShort();

        // messsage format.
        byte messageFormat = buffer.get();

        // TODO: just avro message format is allowed.
        //       another formats like protocol buffers, etc. should be supported in future.
        if(messageFormat != ClientServerSpec.MESSAGE_FORMAT_AVRO)
        {
            log.error("Not Avro Message Format!");

            return;
        }

        // message bytes.
        byte[] messsageBytes = new byte[totalSize - (2 + 2 + 1)];
        buffer.get(messsageBytes);

        // construct disruptor translator.
        this.baseMessageBytesEventTranslator.setChannelId(channelId);
        this.baseMessageBytesEventTranslator.setNioSelector(this.nioSelector);
        this.baseMessageBytesEventTranslator.setApiKey(apiKey);
        this.baseMessageBytesEventTranslator.setApiVersion(apiVersion);
        this.baseMessageBytesEventTranslator.setMessageFormat(messageFormat);
        this.baseMessageBytesEventTranslator.setMessageBytes(messsageBytes);

        // produce base message bytes event to disruptor.
        this.baseMessageBytesEventDisruptor.publishEvent(this.baseMessageBytesEventTranslator);

        this.metricRegistry.meter("ChannelProcessor.read").mark();
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

        this.nioSelector.interestOps(socketChannel, SelectionKey.OP_READ);

        this.metricRegistry.meter("ChannelProcessor.write").mark();
    }
}
