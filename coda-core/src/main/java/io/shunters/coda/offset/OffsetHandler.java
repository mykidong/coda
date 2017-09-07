package io.shunters.coda.offset;

/**
 * Created by mykidong on 2016-09-01.
 */
public interface OffsetHandler {

    public long getCurrentOffsetAndIncrease(TopicPartition topicPartition, long size);

}
