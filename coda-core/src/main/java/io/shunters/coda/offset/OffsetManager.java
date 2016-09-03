package io.shunters.coda.offset;

import io.shunters.coda.processor.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mykidong on 2016-09-01.
 */
public class OffsetManager implements OffsetHandler{

    private static Logger log = LoggerFactory.getLogger(OffsetManager.class);

    private static OffsetHandler offsetHandler;

    private final static Object lock = new Object();

    private ConcurrentMap<QueueShard, AtomicLong> queueShardOffsetMap;

    public static OffsetHandler singleton()
    {
        if(offsetHandler == null)
        {
            synchronized (lock)
            {
                if(offsetHandler == null)
                {
                    offsetHandler = new OffsetManager();
                }
            }
        }

        return offsetHandler;
    }

    private OffsetManager()
    {
        this.queueShardOffsetMap = new ConcurrentHashMap<>();

        loadOffset();
    }


    @Override
    public long getCurrentOffsetAndIncrease(QueueShard queueShard, long size) {
        long currentOffset = 0;
        if(this.queueShardOffsetMap.containsKey(queueShard))
        {
            currentOffset = this.queueShardOffsetMap.get(queueShard).getAndAdd(size);
        }
        else {
            this.queueShardOffsetMap.put(queueShard, new AtomicLong(size));
        }

        return currentOffset;
    }

    @Override
    public void updateOffset(QueueShard queueShard, long offset) {
        this.queueShardOffsetMap.put(queueShard, new AtomicLong(offset));
    }

    @Override
    public void loadOffset() {
        synchronized (lock)
        {
            // TODO: load offset from segment and index files.
        }

    }
}
