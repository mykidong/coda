package io.shunters.coda.store;

import io.shunters.coda.config.ConfigHandler;
import io.shunters.coda.config.YamlConfigHandler;
import io.shunters.coda.offset.TopicPartition;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * data directory structures look like this:
 * <p>
 * index file path: [data-dir]/[topic]/[partition]/[offset].index
 * log file path: [data-dir]/[topic]/[partition]/[offset].log
 */
public class LogHandler {

    private static Logger log = LoggerFactory.getLogger(LogHandler.class);

    public static final String INDEX_FILE_EXTENSION = ".index";
    public static final String LOG_FILE_EXTENSION = ".log";

    private static LogHandler logHandler;

    private static final Object lock = new Object();

    private ConcurrentMap<TopicPartition, List<PartitionLog>> partitionLogMap;

    private ConfigHandler configHandler;

    private List<String> dataDirs;

    private long segmentMaxBytes;

    private Random random;

    private final ReentrantLock reentrantLock = new ReentrantLock();

    public static LogHandler singleton() {
        if (logHandler == null) {
            synchronized (lock) {
                if (logHandler == null) {
                    logHandler = new LogHandler();
                }
            }
        }
        return logHandler;
    }

    private LogHandler() {
        partitionLogMap = new ConcurrentHashMap<>();
        random = new Random();

        configHandler = YamlConfigHandler.getConfigHandler();

        // get max. log segment file size.
        Object segmentMaxBytesObj = configHandler.get(ConfigHandler.CONFIG_DATA_SEGMENT_MAX_BYTES);
        if (segmentMaxBytesObj instanceof Long) {
            segmentMaxBytes = (Long) segmentMaxBytesObj;
        } else if (segmentMaxBytesObj instanceof Integer) {
            segmentMaxBytes = ((Integer) segmentMaxBytesObj).longValue();
        }


        // load log and index files from data directories.
        dataDirs = (List<String>) configHandler.get(ConfigHandler.CONFIG_DATA_DIRS);
        for (String dataDir : dataDirs) {
            File dataDirFile = new File(dataDir);

            File[] topicFiles = dataDirFile.listFiles();
            if (topicFiles == null) {
                break;
            }

            for (File topicFile : topicFiles) {
                if (topicFile.isFile()) {
                    continue;
                }

                // topic.
                String topicName = topicFile.getName();


                File[] partitionFiles = topicFile.listFiles();
                for (File partitionFile : partitionFiles) {
                    if (partitionFile.isFile()) {
                        continue;
                    }

                    // partition.
                    int partition = Integer.valueOf(partitionFile.getName());

                    File[] logFiles = partitionFile.listFiles();

                    Set<String> offsetSet = new HashSet<>();
                    for (File logFile : logFiles) {
                        if (logFile.isDirectory()) {
                            continue;
                        }

                        // log or index file.
                        String logFileName = logFile.getName();

                        String offsetString = logFileName.substring(0, logFileName.lastIndexOf("."));

                        offsetSet.add(offsetString);
                    }

                    TopicPartition topicPartition = new TopicPartition(topicName, partition);
                    List<PartitionLog> partitionLogs = null;
                    if (partitionLogMap.containsKey(topicPartition)) {
                        partitionLogs = partitionLogMap.get(topicPartition);
                    } else {
                        partitionLogs = new ArrayList<>();
                    }


                    for (String offsetString : offsetSet) {
                        long offset = Long.valueOf(offsetString);

                        String indexFilePath = dataDir + File.separator + topicName + File.separator + partition + File.separator + offset + LogHandler.INDEX_FILE_EXTENSION;
                        String logFilePath = dataDir + File.separator + topicName + File.separator + partition + File.separator + offset + LogHandler.LOG_FILE_EXTENSION;

                        OffsetIndex offsetIndex = new OffsetIndex(new File(indexFilePath), offset);
                        PartitionLog partitionLog = new PartitionLog(new File(logFilePath), offset, offsetIndex);

                        partitionLogs.add(partitionLog);

                        partitionLogMap.put(topicPartition, partitionLogs);
                    }
                }
            }
        }
    }

    public ConcurrentMap<TopicPartition, List<PartitionLog>> getPartitionLogMap() {
        return partitionLogMap;
    }


    private int getPartitionLogIndex(TopicPartition topicPartition, long offset) {
        List<PartitionLog> partitionLogs = this.partitionLogMap.get(topicPartition);

        int first = 0;
        int last = partitionLogs.size() - 1;
        while (first <= last) {
            int middle = (first + last) / 2;
            long retOffset = partitionLogs.get(middle).getBaseOffset();
            if (retOffset < offset) {
                first = middle + 1;
            } else if (retOffset > offset) {
                last = middle - 1;
            } else {
                return middle;
            }
        }

        return last;
    }


    public int add(TopicPartition topicPartition, long firstOffset, GenericRecord records, int recordSize) {
        int errorCode = 0;

        PartitionLog partitionLog = null;
        if (partitionLogMap.containsKey(topicPartition)) {
            int partitionLogIndex = this.getPartitionLogIndex(topicPartition, firstOffset);

            List<PartitionLog> partitionLogs = partitionLogMap.get(topicPartition);

            partitionLog = partitionLogs.get(partitionLogIndex);

            long partitionLogFileSize = partitionLog.getSize();

            // if segment file size is reached to the maximum size of segment bytes, create new one.
            if (segmentMaxBytes < partitionLogFileSize) {
                // random selected data dir in which segment file will be created.
                String selectedDataDir = dataDirs.get(random.nextInt(dataDirs.size()));

                errorCode = createNewPartitionLogFileAndSaveRecords(topicPartition, firstOffset, records, recordSize, partitionLogs, partitionLogMap, selectedDataDir);
            } else {
                // save records.
                errorCode = partitionLog.add(firstOffset, records, recordSize);
            }
        }
        // if there is no partition log file for the topic and partition.
        else {
            String firstDataDir = dataDirs.get(0);
            List<PartitionLog> partitionLogs = new ArrayList<>();

            errorCode = createNewPartitionLogFileAndSaveRecords(topicPartition, firstOffset, records, recordSize, partitionLogs, partitionLogMap, firstDataDir);
        }

        return errorCode;
    }

    private int createNewPartitionLogFileAndSaveRecords(TopicPartition topicPartition,
                                          long firstOffset,
                                          GenericRecord records,
                                          int recordSize,
                                          List<PartitionLog> partitionLogs,
                                          ConcurrentMap<TopicPartition, List<PartitionLog>> partitionLogMap,
                                          String dataDir) {

        String topicName = topicPartition.getTopic();
        int partition = topicPartition.getPartition();

        int errorCode = 0;

        String indexFilePath = dataDir + File.separator + topicName + File.separator + partition + File.separator + firstOffset + LogHandler.INDEX_FILE_EXTENSION;
        String logFilePath = dataDir + File.separator + topicName + File.separator + partition + File.separator + firstOffset + LogHandler.LOG_FILE_EXTENSION;

        OffsetIndex offsetIndex = new OffsetIndex(new File(indexFilePath), firstOffset);
        PartitionLog partitionLog = new PartitionLog(new File(logFilePath), firstOffset, offsetIndex);

        // save records.
        errorCode = partitionLog.add(firstOffset, records, recordSize);

        partitionLogs.add(partitionLog);
        partitionLogMap.put(topicPartition, partitionLogs);

        return errorCode;
    }

    public PartitionLog.FetchRecord fetch(TopicPartition topicPartition, long fetchOffset, int maxBytes) {

        if (!partitionLogMap.containsKey(topicPartition)) {
            log.error("topic [" + topicPartition.getTopic() + "] partition [" + topicPartition.getPartition() + "] not found!");

            return null;
        } else {
            PartitionLog partitionLog = partitionLogMap.get(topicPartition).get(this.getPartitionLogIndex(topicPartition, fetchOffset));

            return partitionLog.fetch(fetchOffset, maxBytes);
        }
    }
}
