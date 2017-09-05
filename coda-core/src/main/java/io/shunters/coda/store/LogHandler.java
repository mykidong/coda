package io.shunters.coda.store;

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


    public int add(TopicPartition topicPartition, long firstOffset, GenericRecord records) {
        int errorCode = 0;

        PartitionLog partitionLog = null;
        if(partitionLogMap.containsKey(topicPartition))
        {
            partitionLog = partitionLogMap.get(topicPartition).get(this.getPartitionLogIndex(topicPartition, firstOffset));

            errorCode = partitionLog.add(firstOffset, records);
        }
        else
        {
            // TODO: base directory for segment and index to be configurable.
            long baseOffset = 1;
            OffsetIndex offsetIndex = new OffsetIndex(new File("/tmp/" + baseOffset + ".index"), baseOffset);
            partitionLog = new PartitionLog(new File("/tmp/" + baseOffset + ".log"), baseOffset, offsetIndex);

            errorCode = partitionLog.add(firstOffset, records);

            List<PartitionLog> partitionLogs = new ArrayList<>();
            partitionLogs.add(partitionLog);

            partitionLogMap.put(topicPartition, partitionLogs);
        }

        return errorCode;
    }

    public PartitionLog.FetchRecord fetch(TopicPartition topicPartition, long fetchOffset, int maxBytes)
    {
        PartitionLog partitionLog = null;

        if(!partitionLogMap.containsKey(topicPartition))
        {
            log.error("topic [" + topicPartition.getTopic() + "] partition [" + topicPartition.getPartition() + "] not found!");

            return null;
        }
        else {
            int index = this.getPartitionLogIndex(topicPartition, fetchOffset);

            partitionLog = partitionLogMap.get(topicPartition).get(this.getPartitionLogIndex(topicPartition, fetchOffset));

            return partitionLog.fetch(fetchOffset, maxBytes);
        }
    }
}
