package io.shunters.coda.offset;

import io.shunters.coda.store.LogHandler;
import io.shunters.coda.store.PartitionLog;
import io.shunters.coda.store.PartitionLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mykidong on 2016-09-01.
 */
public class PartitionOffsetHandler implements OffsetHandler {

    private static Logger log = LoggerFactory.getLogger(PartitionOffsetHandler.class);

    private LogHandler logHandler;

    private static OffsetHandler offsetHandler;

    private final static Object lock = new Object();

    private ConcurrentMap<TopicPartition, AtomicLong> topicPartitionOffsetMap;

    public static OffsetHandler singleton() {
        if (offsetHandler == null) {
            synchronized (lock) {
                if (offsetHandler == null) {
                    offsetHandler = new PartitionOffsetHandler();
                }
            }
        }

        return offsetHandler;
    }

    private PartitionOffsetHandler() {
        this.topicPartitionOffsetMap = new ConcurrentHashMap<>();
        this.logHandler = PartitionLogHandler.singleton();

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


    private void loadOffset() {
        ConcurrentMap<TopicPartition, List<PartitionLog>> partitionLogMap = logHandler.getPartitionLogMap();
        for (TopicPartition topicPartition : partitionLogMap.keySet()) {
            List<PartitionLog> partitionLogs = partitionLogMap.get(topicPartition);

            long maxLastOffset = 0;
            for (PartitionLog partitionLog : partitionLogs) {
                long lastOffset = partitionLog.getOffsetIndex().getLastOffset();
                if (maxLastOffset < lastOffset) {
                    maxLastOffset = lastOffset;
                }
            }

            this.topicPartitionOffsetMap.put(topicPartition, new AtomicLong(maxLastOffset + 1));
        }
    }
}
