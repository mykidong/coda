package io.shunters.coda;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.command.RequestByteBuffer;
import io.shunters.coda.disruptor.DisruptorSingleton;
import io.shunters.coda.disruptor.ToRequestEvent;
import io.shunters.coda.disruptor.ToRequestHandler;
import io.shunters.coda.disruptor.ToRequestTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ChannelProcessor extends Thread{

    private static Logger log = LoggerFactory.getLogger(Broker.class);

    private BlockingQueue<SocketChannel> queue;

    private NioSelector nioSelector;

    private MetricRegistry metricRegistry;

    private Disruptor<ToRequestEvent> toRequestEventDisruptor;

    private ToRequestTranslator toRequestTranslator;


    public ChannelProcessor(MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;

        this.queue = new LinkedBlockingQueue<>();

        this.nioSelector = NioSelector.open();

        // ToRequest Disruptor.
        toRequestEventDisruptor = DisruptorSingleton.getInstance("ToRequest", ToRequestEvent.FACTORY, 1024, new ToRequestHandler());
        toRequestTranslator = new ToRequestTranslator();
    }

    public void put(SocketChannel socketChannel)
    {
        this.queue.add(socketChannel);

        this.nioSelector.wakeup();
    }


    @Override
    public void run() {

        try {
            while (true) {
                SocketChannel socketChannel = this.queue.poll();

                // if new connection is added, register it to selector.
                if(socketChannel != null) {
                    String channelId = NioSelector.makeChannelId(socketChannel);
                    nioSelector.register(channelId, socketChannel, SelectionKey.OP_READ);
                }

                int ready = this.nioSelector.select();
                if(ready == 0)
                {
                    continue;
                }

                Iterator<SelectionKey> iter = this.nioSelector.selectedKeys().iterator();

                while(iter.hasNext())
                {
                    SelectionKey key = iter.next();

                    iter.remove();

                    if(key.isReadable())
                    {
                        this.request(key);
                    }
                    else if(key.isWritable())
                    {
                        this.response(key);
                    }
                }
            }
        }
        catch (IOException e)
        {
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


        RequestByteBuffer requestByteBuffer = new RequestByteBuffer(this.nioSelector, channelId, commandId, buffer);

        CommandProcessor commandProcessor = new CommandProcessor(requestByteBuffer);
        commandProcessor.process();

        // send to ToRequest handler.
//        toRequestTranslator.setRequestByteBuffer(requestByteBuffer);
//        toRequestEventDisruptor.publishEvent(toRequestTranslator);

        this.metricRegistry.meter("ChannelProcessor.read").mark();
    }

    private void response(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer buffer = (ByteBuffer) key.attachment();

        buffer.rewind();

        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }

        buffer.clear();

        // switch to read.
        this.nioSelector.interestOps(socketChannel, SelectionKey.OP_READ);

        this.metricRegistry.meter("ChannelProcessor.write").mark();
    }
}
