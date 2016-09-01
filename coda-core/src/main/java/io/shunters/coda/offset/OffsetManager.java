package io.shunters.coda.offset;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2016-09-01.
 */
public class OffsetManager implements OffsetHandler{

    private static OffsetManager offsetManager;

    private final static Object lock = new Object();

    private ConcurrentMap<QueueShard, Long> queueShardOffsetMap;

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
    }


    @Override
    public long getCurrentOffset(QueueShard queueShard) {
        synchronized (lock) {
            if(this.queueShardOffsetMap.containsKey(queueShard))
            {
                return this.queueShardOffsetMap.get(queueShard);
            }
            else {
                return 0;
            }
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
