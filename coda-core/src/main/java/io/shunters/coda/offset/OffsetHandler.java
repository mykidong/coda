package io.shunters.coda.offset;

/**
 * Created by mykidong on 2016-09-01.
 */
public interface OffsetHandler {

    public long getCurrentOffset(QueueShard queueShard);

    public void updateOffset(QueueShard queueShard, long offset);

    public void loadOffset();
}
