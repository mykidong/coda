package io.shunters.coda;

import io.shunters.coda.api.BaseRequestTest;
import io.shunters.coda.api.ProduceRequestTestSkip;
import io.shunters.coda.deser.AvroDeSer;
import io.shunters.coda.deser.MessageDeSer;
import io.shunters.coda.protocol.ApiKeyAvroSchemaMap;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.util.TimeUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by mykidong on 2016-08-23.
 */
public class OldClientTestSkip extends BaseRequestTest {

    private static Logger log = LoggerFactory.getLogger(OldClientTestSkip.class);

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

        private GenericRecord produceRequest;

        private MessageDeSer messageDeSer;

        private AvroDeSer avroDeSer;

        private ApiKeyAvroSchemaMap apiKeyAvroSchemaMap;

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

            produceRequest = new ProduceRequestTestSkip().buildProduceRequest();

            this.messageDeSer = MessageDeSer.singleton();

            this.avroDeSer = AvroDeSer.getAvroDeSerSingleton();

            this.apiKeyAvroSchemaMap = ApiKeyAvroSchemaMap.getApiKeyAvroSchemaMapSingleton();
        }


        @Override
        public void run() {
            while (true) {
                try {
                    byte[] produceRequestBytes = messageDeSer.serializeRequest(ClientServerSpec.API_KEY_PRODUCE_REQUEST,
                            ClientServerSpec.API_VERSION_1,
                            ClientServerSpec.COMPRESSION_CODEC_SNAPPY,
                            produceRequest);

                    out.write(produceRequestBytes);
                    out.flush();


                    // Response.
                    byte[] totalSizeBytes = new byte[4];
                    in.read(totalSizeBytes);
                    int totalSize = ByteBuffer.wrap(totalSizeBytes).getInt();

                    byte[] responseMessageBytes = new byte[totalSize];
                    in.read(responseMessageBytes);

                    GenericRecord responseRecord =
                            messageDeSer.deserializeResponse(apiKeyAvroSchemaMap.getSchemaName(ClientServerSpec.API_KEY_PRODUCE_RESPONSE), totalSize, ByteBuffer.wrap(responseMessageBytes));

                    //log.info("records json: \n" + JsonWriter.formatJson(responseRecord.toString()));

                    TimeUtils.pause(this.pause);

                } catch (Exception e) {
                    e.printStackTrace();

                    break;
                }
            }
        }

    }
}
