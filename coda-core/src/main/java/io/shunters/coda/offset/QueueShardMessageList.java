package io.shunters.coda.offset;

import io.shunters.coda.message.MessageList;

/**
 * Created by mykidong on 2016-09-06.
 */
public class QueueShardMessageList {
    private QueueShard queueShard;
    private long firstOffset;
    private MessageList messageList;

    public QueueShardMessageList(QueueShard queueShard, long firstOffset, MessageList messageList)
    {
        this.queueShard = queueShard;
        this.firstOffset = firstOffset;
        this.messageList = messageList;
    }

    public QueueShard getQueueShard() {
        return queueShard;
    }

    public void setQueueShard(QueueShard queueShard) {
        this.queueShard = queueShard;
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(long firstOffset) {
        this.firstOffset = firstOffset;
    }

    public MessageList getMessageList() {
        return messageList;
    }

    public void setMessageList(MessageList messageList) {
        this.messageList = messageList;
    }
}
