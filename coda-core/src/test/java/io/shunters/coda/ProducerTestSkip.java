package io.shunters.coda;

import io.shunters.coda.api.ProduceRequestTestSkip;
import io.shunters.coda.deser.AvroDeSer;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.util.SingletonUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

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

    private byte[] produceRequestAvroBytes;

    @Before
    public void init() throws Exception {
        java.net.URL url = new ProducerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        GenericRecord produceRequest = new ProduceRequestTestSkip().buildProduceRequest();
        AvroDeSer avroDeSer = SingletonUtils.getAvroDeSerSingleton();
        produceRequestAvroBytes = avroDeSer.serialize(produceRequest);
    }

    @Test
    public void run() throws Exception {
        selector = Selector.open();

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
        socketChannel.connect(new InetSocketAddress("localhost", 9911));

        while (true) {
            int readyChannels = selector.select();
            if (readyChannels == 0) {
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
                } else if (key.isWritable()) {
                    this.writeBytes(key);
                }
            }
        }
    }


    private void connect(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        socketChannel.configureBlocking(false);

        if (socketChannel.isConnectionPending()) {
            socketChannel.finishConnect();

            log.info("connected...");
        }

        log.info("connected to remote host [{}]: ", socketChannel.socket().getRemoteSocketAddress());

        // switch to write.
        socketChannel.register(this.selector, SelectionKey.OP_WRITE);
    }

    private void readBytes(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
//
//        // to get total size.
//        ByteBuffer totalSizeBuffer = ByteBuffer.allocate(4);
//        socketChannel.read(totalSizeBuffer);
//
//        totalSizeBuffer.rewind();
//
//        // total size.
//        int totalSize = totalSizeBuffer.getInt();
//
//        // subsequent bytes buffer.
//        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
//        socketChannel.read(buffer);
//
//        buffer.rewind();
//
//        // TODO: ProduceRequest.
//
//
        // DO NOT SEND ANY MORE!
        // switch to write.
        socketChannel.register(this.selector, SelectionKey.OP_WRITE);
    }

    private void writeBytes(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // snappy compressed avro bytes.
        byte[] snappyCompressedAvro = Snappy.compress(produceRequestAvroBytes);

        // ProduceRequest message.
        int totalSize = (2 + 2 + 1 + 1) + snappyCompressedAvro.length;

        ByteBuffer buffer = ByteBuffer.allocate(4 + totalSize);
        buffer.putInt(totalSize); // total size.
        buffer.putShort(ClientServerSpec.API_KEY_PRODUCE_REQUEST); // api key.
        buffer.putShort((short) 1); // api version.
        buffer.put(ClientServerSpec.MESSAGE_FORMAT_AVRO); // message format.
        buffer.put(ClientServerSpec.COMPRESSION_CODEC_SNAPPY);
        buffer.put(snappyCompressedAvro); // produce request avro bytes.

        buffer.rewind();

        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }

        buffer.clear();

        // switch to read.
        socketChannel.register(this.selector, SelectionKey.OP_READ);
    }
}
