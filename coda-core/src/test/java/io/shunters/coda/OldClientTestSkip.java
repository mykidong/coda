package io.shunters.coda;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutRequestTest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.util.TimeUtils;
import org.junit.Test;

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
        int MAX_THREAD = Integer.parseInt(System.getProperty("threadSize", "5"));
        long pause = Long.parseLong(System.getProperty("pause", "10000000"));

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
        }


        @Override
        public void run() {
            while (true) {
                try {
                    // PutRequest.

                    PutRequest putRequest = PutRequestTest.buildPutRequest();

                    int length = putRequest.length();

                    ByteBuffer buffer = putRequest.write();

                    buffer.rewind();

                    int totalSize = length + 4;
                    byte[] putRequestBytes = new byte[totalSize];
                    buffer.get(putRequestBytes);


                    out.write(putRequestBytes);
                    out.flush();


                    // PutResponse.

                    byte[] responseTotalSizeBytes = new byte[4];

                    int readNum = in.read(responseTotalSizeBytes);

                    int responseTotalSize = ByteBuffer.wrap(responseTotalSizeBytes).getInt();

                    byte[] responseBytes = new byte[responseTotalSize];

                    in.read(responseBytes);


                    ByteBuffer responseBuffer = ByteBuffer.wrap(responseBytes);

                    PutResponse putResponse = PutResponse.fromByteBuffer(responseBuffer);

                    System.out.println("response: [" + putResponse.toString() + "]");

                    TimeUtils.pause(this.pause);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
