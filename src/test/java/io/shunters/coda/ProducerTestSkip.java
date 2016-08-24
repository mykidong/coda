package io.shunters.coda;

import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Created by mykidong on 2016-08-23.
 */
public class ProducerTestSkip {

    private static Logger log = LoggerFactory.getLogger(ProducerTestSkip.class);

    private Selector selector;

    @Before
    public void init() throws Exception {
        java.net.URL url = new ProducerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);
    }

    @Test
    public void run() throws Exception {
        selector = Selector.open();

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
        socketChannel.connect(new InetSocketAddress("localhost", 9911));

        while(true)
        {
            int readyChannels = selector.select();
            if(readyChannels == 0)
            {
                continue;
            }

            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();

                iter.remove();

                if (!key.isValid()) {
                    continue;
                }

                if (key.isConnectable()) {
                    this.connect(key);
                } else if (key.isReadable()) {
                    this.readBytes(key);
                }
                else if(key.isWritable())
                {
                    this.writeBytes(key);
                }
            }
        }
    }


    private void connect(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        socketChannel.configureBlocking(false);

        if(socketChannel.isConnectionPending())
        {
            socketChannel.finishConnect();

            log.info("connected...");
        }

        log.info("connected to remote host [{}]: ", socketChannel.socket().getRemoteSocketAddress());

        // switch to write.
        socketChannel.register(this.selector, SelectionKey.OP_WRITE);
    }

    private void readBytes(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        log.info("read bytes...");

        ByteBuffer readBuf = ByteBuffer.allocate(1024);

        int bytesRead = socketChannel.read(readBuf);

        if (bytesRead == -1) {
            log.info("read bytes == -1");

            socketChannel.close();
            key.cancel();

            return;
        }

        readBuf.flip();

        byte[] dest = new byte[bytesRead];
        readBuf.get(dest);

        log.info("response message: [{}]", new String(dest));


        // DO NOT SEND ANY MORE!
        // switch to write.
        //socketChannel.register(this.selector, SelectionKey.OP_WRITE);
    }

    private void writeBytes(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        String newData = "New String to write to file..." + System.currentTimeMillis();

        ByteBuffer buf = ByteBuffer.allocate(48);
        buf.clear();
        buf.put(newData.getBytes());

        buf.flip();

        socketChannel.write(buf);

        log.info("bytes written ...");

        // switch to read.
        socketChannel.register(this.selector, SelectionKey.OP_READ);
    }
}
