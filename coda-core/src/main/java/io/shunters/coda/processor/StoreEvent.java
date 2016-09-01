package io.shunters.coda.processor;

import io.shunters.coda.message.MessageList;
import io.shunters.coda.offset.QueueShard;

/**
 * Created by mykidong on 2016-09-01.
 */
public class StoreEvent {

    private BaseEvent baseEvent;

    private int messageId;

    private QueueShard queueShard;

    private MessageList messageList;


    public StoreEvent(BaseEvent baseEvent, int messageId, QueueShard queueShard, MessageList messageList)
    {
        this.baseEvent = baseEvent;
        this.messageId = messageId;
        this.queueShard = queueShard;
        this.messageList = messageList;
    }

    public BaseEvent getBaseEvent() {
        return baseEvent;
    }

    public int getMessageId() {
        return messageId;
    }

    public QueueShard getQueueShard() {
        return queueShard;
    }

    public MessageList getMessageList() {
        return messageList;
    }
}
