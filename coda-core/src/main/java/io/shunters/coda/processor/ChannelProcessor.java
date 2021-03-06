package io.shunters.coda.processor;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.util.DisruptorCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

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
     * request bytes event disruptor.
     */
    private Disruptor<BaseMessage.RequestBytesEvent> requestBytesEventDisruptor;

    /**
     * request bytes event translator.
     */
    private BaseMessage.RequestBytesEventTranslator requestBytesEventTranslator;


    public ChannelProcessor(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;

        this.queue = new LinkedBlockingQueue<>();
        this.nioSelector = NioSelector.open();

        requestBytesEventDisruptor = DisruptorCreator.singleton(DisruptorCreator.DISRUPTOR_NAME_REQUEST_PROCESSOR, BaseMessage.RequestBytesEvent.FACTORY, 1024, RequestProcessor.singleton());
        this.requestBytesEventTranslator = new BaseMessage.RequestBytesEventTranslator();
    }

    public void put(SocketChannel socketChannel) {
        this.queue.add(socketChannel);
        this.nioSelector.wakeup();
    }


    @Override
    public void run() {
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
                } else if (key.isWritable()) {
                    this.response(key);
                }
            }
        }
    }


    private void request(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // channel id.
        String channelId = NioSelector.makeChannelId(socketChannel);

        try {
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
            if (messageFormat != ClientServerSpec.MESSAGE_FORMAT_AVRO) {
                log.error("Not Avro Message Format!");

                return;
            }

            // compression codec.
            byte compressionCodec = buffer.get();

            // message bytes.
            byte[] messsageBytes = new byte[totalSize - (2 + 2 + 1 + 1)];
            buffer.get(messsageBytes);

            // if avro bytes is coompressed by snappy, uncompress them.
            if (compressionCodec == ClientServerSpec.COMPRESSION_CODEC_SNAPPY) {
                messsageBytes = Snappy.uncompress(messsageBytes);
            }

            // construct disruptor translator.
            this.requestBytesEventTranslator.setChannelId(channelId);
            this.requestBytesEventTranslator.setNioSelector(this.nioSelector);
            this.requestBytesEventTranslator.setApiKey(apiKey);
            this.requestBytesEventTranslator.setApiVersion(apiVersion);
            this.requestBytesEventTranslator.setMessageFormat(messageFormat);
            this.requestBytesEventTranslator.setMessageBytes(messsageBytes);

            // produce request bytes event to disruptor.
            this.requestBytesEventDisruptor.publishEvent(this.requestBytesEventTranslator);

            this.metricRegistry.meter("ChannelProcessor.read").mark();
        } catch (Exception e) {
            nioSelector.removeSocketChannel(channelId);
            key.cancel();
        }
    }

    private void response(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // channel id.
        String channelId = NioSelector.makeChannelId(socketChannel);

        try {
            ByteBuffer buffer = (ByteBuffer) key.attachment();

            if (buffer == null) {
                return;
            }

            buffer.rewind();

            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }

            buffer.clear();

            this.nioSelector.interestOps(socketChannel, SelectionKey.OP_READ);

            this.metricRegistry.meter("ChannelProcessor.write").mark();
        } catch (IOException e) {
            nioSelector.removeSocketChannel(channelId);
            key.cancel();
        }
    }
}
