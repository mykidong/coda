package io.shunters.coda;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutRequestTest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.message.BaseRequestHeader;
import io.shunters.coda.message.BaseRequestHeaderTest;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageListTest;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

        // PutResponse.

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

        PutResponse putResponse = PutResponse.fromByteBuffer(buffer);

        log.info("response message: [{}]", putResponse.toString());


        // DO NOT SEND ANY MORE!
        // switch to write.
        //socketChannel.register(this.selector, SelectionKey.OP_WRITE);
    }

    private void writeBytes(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // PutRequest.

        PutRequest putRequest = PutRequestTest.buildPutRequest();

        ByteBuffer buffer = putRequest.write();

        buffer.rewind();

        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }

        buffer.clear();

        log.info("bytes written ...");

        // switch to read.
        socketChannel.register(this.selector, SelectionKey.OP_READ);
    }
}
