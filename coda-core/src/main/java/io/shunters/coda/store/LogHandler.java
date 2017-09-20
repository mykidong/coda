package io.shunters.coda.store;

import io.shunters.coda.offset.TopicPartition;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2017-09-20.
 */
public interface LogHandler {

    ConcurrentMap<TopicPartition, List<PartitionLog>> getPartitionLogMap();

    int add(TopicPartition topicPartition, long firstOffset, GenericRecord records, int recordSize);

    FetchRecord fetch(TopicPartition topicPartition, long fetchOffset, int maxBytes);

    public static class FetchRecord {
        private int errorCode;

        private long highwaterMarkOffset;

        private List<GenericRecord> recordsList;

        public FetchRecord(int errorCode, long highwaterMarkOffset, List<GenericRecord> recordsList) {
            this.errorCode = errorCode;
            this.highwaterMarkOffset = highwaterMarkOffset;
            this.recordsList = recordsList;
        }

        public int getErrorCode() {
            return errorCode;
        }

        public long getHighwaterMarkOffset() {
            return highwaterMarkOffset;
        }

        public List<GenericRecord> getRecordsList() {
            return recordsList;
        }
    }
}
