package io.shunters.coda.queue;

/**
 * Created by mykidong on 2016-10-21.
 */
public interface CodaQueue {

    public void add(Object object);

    public Object get();

}
