package io.shunters.coda.disruptor;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

/**
 * Created by mykidong on 2016-09-01.
 */
public class DisruptorSingleton {

    private static Logger log = LoggerFactory.getLogger(DisruptorSingleton.class);

    private static ConcurrentMap<String, Disruptor> disruptorMap;

    private static final Object lock = new Object();


    public static <T> Disruptor getInstance(String disruptorName, EventFactory<T> factory, int bufferSize, EventHandler<T>... handlers)
    {
        if(disruptorMap == null) {
            synchronized(lock) {
                if(disruptorMap == null) {

                    disruptorMap = new ConcurrentHashMap<>();

                    Disruptor disruptor = getDisruptor(factory, bufferSize, handlers);

                    disruptorMap.put(disruptorName, disruptor);

                    log.info("disruptorMap is initialized, disruptorName: [" + disruptorName + "]");
                }
            }
        }
        else
        {
            synchronized(lock) {
                if (!disruptorMap.containsKey(disruptorName)) {
                    Disruptor disruptor = getDisruptor(factory, bufferSize, handlers);

                    disruptorMap.put(disruptorName, disruptor);

                    log.info("disruptorName: [" + disruptorName + "] not exists, new disruptor put to map.");
                }
            }
        }

        return disruptorMap.get(disruptorName);
    }

    private static <T> Disruptor getDisruptor(EventFactory<T> factory, int bufferSize, EventHandler<T>[] handlers) {
        Disruptor disruptor = new Disruptor(factory,
                bufferSize,
                Executors.newCachedThreadPool(),
                ProducerType.SINGLE, // Single producer
                new BlockingWaitStrategy());

        disruptor.handleEventsWith(handlers);

        disruptor.start();

        return disruptor;
    }
}
