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
public class DisruptorBuilder {

    private static Logger log = LoggerFactory.getLogger(DisruptorBuilder.class);

    private static ConcurrentMap<String, Disruptor> disruptorMap;

    private static final Object lock = new Object();


    public static <T> Disruptor singleton(String disruptorName, EventFactory<T> factory, int bufferSize, EventHandler<T>... handlers)
    {
        if(disruptorMap == null) {
            synchronized(lock) {
                if(disruptorMap == null) {

                    disruptorMap = new ConcurrentHashMap<>();

                    Disruptor disruptor = newInstance(disruptorName, factory, bufferSize, handlers);

                    disruptorMap.put(disruptorName, disruptor);

                    log.info("disruptor map is initialized, and new disruptor [" + disruptorName + "] added...");
                }
            }
        }
        else
        {
            synchronized(lock) {
                if (!disruptorMap.containsKey(disruptorName)) {
                    Disruptor disruptor = newInstance(disruptorName, factory, bufferSize, handlers);

                    disruptorMap.put(disruptorName, disruptor);

                    log.info("disruptor [" + disruptorName + "] does not exist, new disruptor put to map...");
                }
                else {
                    log.info("disruptor [{}] called from map...", disruptorName);
                }
            }
        }

        return disruptorMap.get(disruptorName);
    }

    public static <T> Disruptor newInstance(String disruptorName, EventFactory<T> factory, int bufferSize, EventHandler<T>... handlers) {
        Disruptor disruptor = new Disruptor(factory,
                bufferSize,
                Executors.newCachedThreadPool(),
                ProducerType.SINGLE, // Single producer
                new BlockingWaitStrategy());

        disruptor.handleEventsWith(handlers);

        disruptor.start();

        log.info("disruptor [{}] returned...", disruptorName);

        return disruptor;
    }
}
