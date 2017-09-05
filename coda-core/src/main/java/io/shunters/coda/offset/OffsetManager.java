package io.shunters.coda.offset;

import io.shunters.coda.store.OffsetIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mykidong on 2016-09-01.
 */
public class OffsetManager implements OffsetHandler {

    private static Logger log = LoggerFactory.getLogger(OffsetManager.class);

    private static OffsetHandler offsetHandler;

    private final static Object lock = new Object();

    private ConcurrentMap<TopicPartition, AtomicLong> topicPartitionOffsetMap;

    public static OffsetHandler singleton() {
        if (offsetHandler == null) {
            synchronized (lock) {
                if (offsetHandler == null) {
                    offsetHandler = new OffsetManager();
                }
            }
        }

        return offsetHandler;
    }

    private OffsetManager() {
        this.topicPartitionOffsetMap = new ConcurrentHashMap<>();

        loadOffset();
    }


    @Override
    public long getCurrentOffsetAndIncrease(TopicPartition topicPartition, long size) {
        long currentOffset = 1;
        if (this.topicPartitionOffsetMap.containsKey(topicPartition)) {
            currentOffset = this.topicPartitionOffsetMap.get(topicPartition).getAndAdd(size);
        } else {
            this.topicPartitionOffsetMap.put(topicPartition, new AtomicLong(size + 1));
        }

        return currentOffset;
    }

    @Override
    public void updateOffset(TopicPartition topicPartition, long offset) {
        this.topicPartitionOffsetMap.put(topicPartition, new AtomicLong(offset));
    }

    private void loadOffset() {
        // TODO: load last offset from offset index files.
        long lastOffset = new OffsetIndex(new File("C:\\tmp\\1.index"), 1).getLastOffset();
        this.topicPartitionOffsetMap.put(new TopicPartition("any-topic", 0), new AtomicLong(lastOffset + 1));
    }
}
