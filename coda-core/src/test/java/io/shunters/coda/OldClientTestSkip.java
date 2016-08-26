package io.shunters.coda;

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
        int MAX_THREAD = Integer.parseInt(System.getProperty("threadSize", "1"));
        long pause = Long.parseLong(System.getProperty("pause", "100000"));

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
                    String newData = "New String to write to file..." + System.currentTimeMillis();
                    out.write(newData.getBytes());
                    out.flush();

                    byte[] readBytes = new byte[1024];
                    int readNum = in.read(readBytes);

                    ByteBuffer buf = ByteBuffer.allocate(readNum);
                    buf.put(readBytes, 0, readNum);
                    buf.flip();

                    byte[] dest = new byte[readNum];
                    buf.get(dest);

                    //System.out.println("response: [" + new String(dest) + "]");

                    TimeUtils.pause(this.pause);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
