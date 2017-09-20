package io.shunters.coda.store;

import io.shunters.coda.config.ConfigHandler;
import io.shunters.coda.config.YamlConfigHandler;
import io.shunters.coda.offset.TopicPartition;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2017-09-07.
 */
public class LogHandlerTestSkip {

    @Test
    public void readLogFiles()
    {
        ConcurrentMap<TopicPartition, List<PartitionLog>> partitionLogMap = new ConcurrentHashMap<>();

        ConfigHandler configHandler = YamlConfigHandler.singleton("/config/coda-test.yml");


        List<String> dataDirs = (List<String>) configHandler.get(ConfigHandler.CONFIG_DATA_DIRS);
        for(String dataDir : dataDirs)
        {
            dataDir = "target/test-classes/" + dataDir;

            File dataDirFile = new File(dataDir);

            File[] topicFiles = dataDirFile.listFiles();
            if(topicFiles == null)
            {
                break;
            }

            for(File topicFile : topicFiles)
            {
                if(topicFile.isFile())
                {
                    continue;
                }

                // topic.
                String topicName = topicFile.getName();


                File[] partitionFiles = topicFile.listFiles();
                for(File partitionFile : partitionFiles)
                {
                    if(partitionFile.isFile())
                    {
                        continue;
                    }

                    // partition.
                    int partition = Integer.valueOf(partitionFile.getName());

                    File[] logFiles = partitionFile.listFiles();

                    Set<String> offsetSet = new HashSet<>();
                    for(File logFile : logFiles)
                    {
                        if(logFile.isDirectory())
                        {
                            continue;
                        }

                        // log or index file.
                        String logFileName = logFile.getName();

                        String offsetString = logFileName.substring(0, logFileName.lastIndexOf("."));

                        offsetSet.add(offsetString);
                    }

                    TopicPartition topicPartition = new TopicPartition(topicName, partition);
                    List<PartitionLog> partitionLogs = null;
                    if(partitionLogMap.containsKey(topicPartition))
                    {
                        partitionLogs = partitionLogMap.get(topicPartition);
                    }
                    else
                    {
                        partitionLogs = new ArrayList<>();
                    }


                    for(String offsetString : offsetSet)
                    {
                        long offset = Long.valueOf(offsetString);

                        String indexFilePath = dataDir + File.separator + topicName + File.separator + partition + File.separator + offset + PartitionLogHandler.INDEX_FILE_EXTENSION;
                        String logFilePath = dataDir + File.separator + topicName + File.separator + partition + File.separator + offset + PartitionLogHandler.LOG_FILE_EXTENSION;

                        OffsetIndex offsetIndex = new OffsetIndex(new File(indexFilePath), offset);
                        PartitionLog partitionLog = new PartitionLog(new File(logFilePath), offset, offsetIndex);

                        partitionLogs.add(partitionLog);

                        partitionLogMap.put(topicPartition, partitionLogs);
                    }
                }
            }
        }

        for(TopicPartition topicPartition : partitionLogMap.keySet())
        {
            System.out.println("topic partition: " + topicPartition.toString() + ", " + partitionLogMap.get(topicPartition));
        }
    }
}
