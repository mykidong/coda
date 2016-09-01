package io.shunters.coda.offset;

/**
 * Created by mykidong on 2016-09-01.
 */
public class QueueShard {

    private String queue;

    private int shardId;

    public QueueShard(String queue, int shardId)
    {
        this.queue = queue;
        this.shardId = shardId;
    }

    public String getQueue() {
        return queue;
    }

    public int getShardId() {
        return shardId;
    }
}
