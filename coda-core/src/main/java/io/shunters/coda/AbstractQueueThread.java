package io.shunters.coda;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by mykidong on 2016-09-01.
 */
public abstract class AbstractQueueThread extends Thread {

    protected BlockingQueue queue = new LinkedBlockingQueue();

    public void put(Object obj)
    {
        this.queue.add(obj);
    }

    public abstract void process(Object obj);
}
