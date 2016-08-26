package io.shunters.coda.selector;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Created by mykidong on 2016-08-23.
 */
public class SelectorHandler implements EventHandler<SelectorEvent> {

    private static Logger log = LoggerFactory.getLogger(SelectorHandler.class);

    private MetricRegistry metricRegistry;

    public SelectorHandler(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }


    @Override
    public void onEvent(SelectorEvent selectorEvent, long l, boolean b) throws Exception {
        SelectionKey key = selectorEvent.getKey();
        SocketChannel socketChannel = selectorEvent.getSocketChannel();

        try {
            int interestOps = key.interestOps();
            if (interestOps == SelectionKey.OP_READ) {
                this.read(key, socketChannel);
            } else if (interestOps == SelectionKey.OP_WRITE) {
                this.write(key, socketChannel);
            }

            // wake up selector.
            key.selector().wakeup();

        }catch (Exception e)
        {
            //e.printStackTrace();
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

        //log.info("incoming message: [{}]", new String(dest));


        String echoResponse = new String(dest) + " : from server";
        ByteBuffer writeBuf = ByteBuffer.wrap(echoResponse.getBytes());

        // NOTE: DO NOT writeBuf.flip();

        // socket registered for write.
        socketChannel.register(key.selector(), SelectionKey.OP_WRITE, writeBuf);

        this.metricRegistry.meter("selector.SelectorHandler.read").mark();
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

        this.metricRegistry.meter("selector.SelectorHandler.write").mark();
    }

}
