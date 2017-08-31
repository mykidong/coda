package io.shunters.coda.store;

import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageOffset;
import io.shunters.coda.offset.QueueShard;
import io.shunters.coda.offset.TopicPartition;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mykidong on 2017-08-30.
 */
public class LogHandler {

    private static Logger log = LoggerFactory.getLogger(LogHandler.class);

    private static LogHandler logHandler;

    private static final Object lock = new Object();

    private ConcurrentMap<TopicPartition, List<PartitionLog>> partitionLogMap;

    private final ReentrantLock reentrantLock = new ReentrantLock();

    public static LogHandler singleton()
    {
        if(logHandler == null)
        {
            synchronized (lock)
            {
                if(logHandler == null)
                {
                    logHandler = new LogHandler();
                }
            }
        }
        return logHandler;
    }

    private LogHandler()
    {
        partitionLogMap = new ConcurrentHashMap<>();

        // TODO: load partition log and offset index files.
    }

    private int getPartitionLogIndex(TopicPartition topicPartition, long offset)
    {
        List<PartitionLog> partitionLogs = this.partitionLogMap.get(topicPartition);

        int first = 0;
        int last = partitionLogs.size() -1;
        while(first <= last)
        {
            int middle = (first + last) / 2;
            long retOffset = partitionLogs.get(middle).getBaseOffset();
            if(retOffset < offset)
            {
                first = middle + 1;
            }
            else if(retOffset > offset)
            {
                last = middle -1;
            }
            else
            {
                return middle;
            }
        }

        return last;
    }


    public void add(TopicPartition topicPartition, long firstOffset, GenericRecord records) {
        PartitionLog partitionLog = null;
        if(partitionLogMap.containsKey(topicPartition))
        {
            partitionLog = partitionLogMap.get(topicPartition).get(this.getPartitionLogIndex(topicPartition, firstOffset));

            reentrantLock.lock();
            try {
                partitionLog.add(firstOffset, records);
            }finally {
                reentrantLock.unlock();
            }
        }
        else
        {
            // TODO: base directory for segment and index to be configurable.
            long baseOffset = 1;
            OffsetIndex offsetIndex = new OffsetIndex(new File("/tmp/" + baseOffset + ".index"), baseOffset);
            partitionLog = new PartitionLog(new File("/tmp/" + baseOffset + ".log"), baseOffset, offsetIndex);
            partitionLog.add(firstOffset, records);

            List<PartitionLog> partitionLogs = new ArrayList<>();
            partitionLogs.add(partitionLog);

            partitionLogMap.put(topicPartition, partitionLogs);
        }
    }
}
