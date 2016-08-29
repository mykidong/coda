package io.shunters.coda;

import io.shunters.coda.command.RequestByteBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by mykidong on 2016-08-29.
 */
public class RequestProcessor extends Thread {

    private BlockingQueue<RequestByteBuffer> queue;

    public RequestProcessor()
    {
        this.queue = new LinkedBlockingQueue<>();
    }

    public void put(RequestByteBuffer requestByteBuffer)
    {
        this.queue.add(requestByteBuffer);
    }


    @Override
    public void run()
    {
        while (true) {
            RequestByteBuffer requestByteBuffer = this.queue.poll();
            if(requestByteBuffer == null)
            {
                continue;
            }

            CommandProcessor commandProcessor = new CommandProcessor(requestByteBuffer);
            commandProcessor.process();
        }
    }
}
