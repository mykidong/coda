package io.shunters.coda.selector;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by mykidong on 2016-08-23.
 */
public class SelectorHandler implements EventHandler<SelectorEvent> {

    private static Logger log = LoggerFactory.getLogger(SelectorHandler.class);

    private MetricRegistry metricRegistry;

    private BlockingQueue<SocketChannel> queue;

    final CountDownLatch latch = new CountDownLatch(1);

    public SelectorHandler(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;

        queue = new LinkedBlockingQueue<>();

        Thread t = new Thread(new Task(this.queue, this.latch));
        t.start();

        try {
            latch.await();
        }catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }


    private static class Task implements Runnable
    {
        private Selector selector;
        private BlockingQueue<SocketChannel> queue;

        private final CountDownLatch latch;

        public Task(BlockingQueue<SocketChannel> queue, CountDownLatch latch)
        {
            this.queue = queue;
            this.latch = latch;

            try {
                selector = Selector.open();
            }catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

            latch.countDown();

            try {
                while (true) {
                    SocketChannel socketChannel = queue.poll();

                    if(socketChannel == null)
                    {
                        continue;
                    }

                    log.info("polled socket: [{}]", socketChannel.socket().getRemoteSocketAddress());

                    socketChannel.register(this.selector, SelectionKey.OP_READ);

                    log.info("after register...");

                    int ready = this.selector.select();

                    log.info("after select: [{}]", ready);

                    if(ready == 0)
                    {
                        continue;
                    }

                    Iterator<SelectionKey> iter = this.selector.selectedKeys().iterator();

                    while(iter.hasNext())
                    {
                        SelectionKey key = iter.next();

                        SocketChannel channel = (SocketChannel) key.channel();

                        iter.remove();

                        log.info("selected channel: [{}]", socketChannel.socket().getRemoteSocketAddress());

                        if(key.isReadable())
                        {
                            log.info("is readable...");
                            this.read(key, channel);
                        }
                        else if (key.isWritable())
                        {
                            log.info("is writable...");
                            this.write(key, channel);
                        }
                    }
                }
            }catch (IOException e)
            {
                e.printStackTrace();
            }

        }

        private void read(SelectionKey key, SocketChannel socketChannel) throws IOException {
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

            log.info("incoming message: [{}]", new String(dest));


            String echoResponse = new String(dest) + " : from server";
            ByteBuffer writeBuf = ByteBuffer.wrap(echoResponse.getBytes());

            // NOTE: DO NOT writeBuf.flip();

            // socket registered for write.
            socketChannel.register(this.selector, SelectionKey.OP_WRITE, writeBuf);

            //this.metricRegistry.meter("selector.SelectorHandler.read").mark();
        }

        private void write(SelectionKey key, SocketChannel socketChannel) throws IOException {
            ByteBuffer buf = (ByteBuffer) key.attachment();

            // NOTE: DO NOT buf.flip();

            //log.info("ready to write buffer [{}] to connnected [{}]", buf.capacity(), socketChannel.socket().getRemoteSocketAddress());

            while (buf.hasRemaining()) {
                socketChannel.write(buf);
            }

            buf.clear();

            // switch to read.
            key.interestOps(SelectionKey.OP_READ);

            //this.metricRegistry.meter("selector.SelectorHandler.write").mark();
        }
    }


    @Override
    public void onEvent(SelectorEvent selectorEvent, long l, boolean b) throws Exception {
        SelectionKey key = selectorEvent.getKey();
        SocketChannel socketChannel = selectorEvent.getSocketChannel();

        this.queue.add(socketChannel);
    }
}
