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

    @Override
    public int hashCode()
    {
        return queue.hashCode() + this.shardId;
    }

    @Override
    public boolean equals(Object o)
    {
        QueueShard queueShard = (QueueShard) o;

        return (this.queue.equals(queueShard.getQueue())) && (this.shardId == queueShard.getShardId());
    }
}
