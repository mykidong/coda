package io.shunters.coda;

import com.cedarsoftware.util.io.JsonWriter;
import io.shunters.coda.api.ProduceRequestTestSkip;
import io.shunters.coda.deser.MessageDeSer;
import io.shunters.coda.protocol.ApiKeyAvroSchemaMap;
import io.shunters.coda.protocol.ClientServerSpec;
import org.apache.avro.generic.GenericRecord;
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

    private GenericRecord produceRequest;

    private MessageDeSer messageDeSer;

    private ApiKeyAvroSchemaMap apiKeyAvroSchemaMap;

    @Before
    public void init() throws Exception {
        java.net.URL url = new ProducerTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        produceRequest = new ProduceRequestTestSkip().buildProduceRequest();
        messageDeSer = MessageDeSer.singleton();
        apiKeyAvroSchemaMap = ApiKeyAvroSchemaMap.getApiKeyAvroSchemaMapSingleton();
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

        // ProduceResponse.
        GenericRecord responseRecord =
                messageDeSer.deserializeResponse(apiKeyAvroSchemaMap.getSchemaName(ClientServerSpec.API_KEY_PRODUCE_RESPONSE), totalSize, buffer);

        log.info("records json: \n" + JsonWriter.formatJson(responseRecord.toString()));



        // DO NOT SEND ANY MORE!
        // switch to write.
        socketChannel.register(this.selector, SelectionKey.OP_WRITE);
    }

    private void writeBytes(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        MessageDeSer.ByteBufferAndSize produceRequestBuffer = messageDeSer.serializeRequestToByteBuffer(ClientServerSpec.API_KEY_PRODUCE_REQUEST,
                ClientServerSpec.API_VERSION_1,
                ClientServerSpec.COMPRESSION_CODEC_SNAPPY,
                produceRequest);

        ByteBuffer buffer = produceRequestBuffer.getByteBuffer();

        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }

        buffer.clear();

        // switch to read.
        socketChannel.register(this.selector, SelectionKey.OP_READ);
    }
}
