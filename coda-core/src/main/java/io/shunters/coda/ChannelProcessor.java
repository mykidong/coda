package io.shunters.coda;

import com.codahale.metrics.MetricRegistry;
import io.shunters.coda.metrics.MetricsReporter;
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

    public ChannelProcessor(MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;

        this.queue = new LinkedBlockingQueue<>();

        this.nioSelector = NioSelector.open();
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
                SocketChannel socketChannel = queue.poll();
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
                        this.read(key);
                    }
                    else if(key.isWritable())
                    {
                        this.write(key);
                    }
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }


    private void read(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer readBuf = ByteBuffer.allocate(1024);

        int bytesRead;
        try {
            bytesRead = socketChannel.read(readBuf);
        } catch (IOException e) {
            key.cancel();
            socketChannel.close();

            return;
        }

        if (bytesRead == -1) {
            log.info("Connection closed by client [{}]", socketChannel.socket().getRemoteSocketAddress());

            socketChannel.close();
            key.cancel();

            return;
        }

        if(bytesRead == 0)
        {
            //log.info("bytesRead: [{}]", bytesRead);

            return;
        }

        readBuf.flip();

        byte[] dest = new byte[bytesRead];
        readBuf.get(dest);

        //log.info("incoming message: [{}]", new String(dest));


        String echoResponse = new String(dest) + " : from server " + Thread.currentThread().getId();
        ByteBuffer writeBuf = ByteBuffer.wrap(echoResponse.getBytes());

        // NOTE: DO NOT writeBuf.flip();

        nioSelector.attach(socketChannel, SelectionKey.OP_WRITE, writeBuf);

        this.metricRegistry.meter("ChannelProcessor.read").mark();
    }

    private void write(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer buf = (ByteBuffer) key.attachment();

        // NOTE: DO NOT buf.flip();

        //log.info("ready to write buffer [{}] to connnected [{}]", buf.capacity(), socketChannel.socket().getRemoteSocketAddress());

        while (buf.hasRemaining()) {
            socketChannel.write(buf);
        }

        buf.clear();

        // switch to read.
        this.nioSelector.interestOps(socketChannel, SelectionKey.OP_READ);

        this.metricRegistry.meter("ChannelProcessor.write").mark();
    }

}
