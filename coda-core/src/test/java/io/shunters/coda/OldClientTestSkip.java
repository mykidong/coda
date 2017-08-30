package io.shunters.coda;

import io.shunters.coda.api.ProduceRequestTestSkip;
import io.shunters.coda.api.service.AvroDeSerService;
import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutRequestTest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.util.SingletonUtils;
import io.shunters.coda.util.TimeUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by mykidong on 2016-08-23.
 */
public class OldClientTestSkip {

    @Test
    public void run() throws Exception {
        String host = System.getProperty("host", "localhost");
        int port = Integer.parseInt(System.getProperty("port", "9911"));
        int MAX_THREAD = Integer.parseInt(System.getProperty("threadSize", "20"));
        long pause = Long.parseLong(System.getProperty("pause", "100"));

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREAD);

        for (int i = 0; i < MAX_THREAD; i++) {
            executor.execute(new ClientTask(host, port, pause));
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    private static class ClientTask implements Runnable {

        private Socket clientSocket;
        private OutputStream out;
        private InputStream in;

        private long pause;
        private String host;
        private int port;

        private byte[] produceRequestAvroBytes;

        public ClientTask(String host, int port, long pause) {
            this.host = host;
            this.port = port;
            this.pause = pause;
            try {
                clientSocket = new Socket(this.host, this.port);

                out = clientSocket.getOutputStream();
                in = clientSocket.getInputStream();
            } catch (Exception e) {
                e.printStackTrace();
            }

            GenericRecord produceRequest = new ProduceRequestTestSkip().buildProduceRequest();
            AvroDeSerService avroDeSerService = SingletonUtils.getAvroDeSerServiceSingleton();
            produceRequestAvroBytes = avroDeSerService.serialize(produceRequest);
        }


        @Override
        public void run() {
            while (true) {
                try {
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

                    byte[] produceRequestBytes = new byte[4 + totalSize];
                    buffer.get(produceRequestBytes);

                    out.write(produceRequestBytes);
                    out.flush();


                    // Response.
                    byte[] responseBytes = new byte["hello, I'm response!".length()];

                    in.read(responseBytes);

                    //System.out.println("response: [" + new String(responseBytes) + "]");

                    TimeUtils.pause(this.pause);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
