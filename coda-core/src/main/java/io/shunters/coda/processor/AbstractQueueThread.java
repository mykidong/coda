package io.shunters.coda.processor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by mykidong on 2016-09-01.
 */
public abstract class AbstractQueueThread<T> extends Thread {

    private BlockingQueue<T> queue = new LinkedBlockingQueue<>();

    public void put(T event)
    {
        this.queue.add(event);
    }

    @Override
    public void run()
    {
        try {
            while (true) {
                Object obj = this.queue.take();

                T event = (T) obj;
                process(event);
            }
        }catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    public abstract void process(T event);
}
