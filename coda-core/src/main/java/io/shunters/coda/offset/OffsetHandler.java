package io.shunters.coda.offset;

/**
 * Created by mykidong on 2016-09-01.
 */
public interface OffsetHandler {

    public long getCurrentOffsetAndIncrease(QueueShard queueShard, long size);

    public void updateOffset(QueueShard queueShard, long offset);

    public void loadOffset();
}
