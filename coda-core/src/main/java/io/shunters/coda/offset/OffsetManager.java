package io.shunters.coda.offset;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2016-09-01.
 */
public class OffsetManager implements OffsetHandler{

    private static OffsetManager offsetManager;

    private final static Object lock = new Object();

    private Map<QueueShard, Long> queueShardOffsetMap;

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
        this.queueShardOffsetMap = new HashMap<>();

        loadOffset();
    }


    @Override
    public long getCurrentOffsetAndIncrease(QueueShard queueShard, long size) {
        synchronized (lock)
        {
            long currentOffset = 0;
            if(this.queueShardOffsetMap.containsKey(queueShard))
            {
                currentOffset = this.queueShardOffsetMap.get(queueShard);
            }

            long newOffset = currentOffset + size;
            this.queueShardOffsetMap.put(queueShard, newOffset);

            return currentOffset;
        }
    }

    @Override
    public void updateOffset(QueueShard queueShard, long offset) {
        synchronized (lock)
        {
            this.queueShardOffsetMap.put(queueShard, offset);
        }
    }

    @Override
    public void loadOffset() {
        synchronized (lock)
        {
            // TODO: load offset from segment and index files.
        }

    }
}
