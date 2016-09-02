package io.shunters.coda.offset;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mykidong on 2016-09-01.
 */
public class OffsetManager implements OffsetHandler{

    private static OffsetManager offsetManager;

    private final static Object lock = new Object();

    private ConcurrentMap<QueueShard, AtomicLong> queueShardOffsetMap;

    public static OffsetManager singleton()
    {
        if(offsetManager == null)
        {
            synchronized (lock)
            {
                if(offsetManager == null)
                {
                    offsetManager = new OffsetManager();
                }
            }
        }

        return offsetManager;
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
