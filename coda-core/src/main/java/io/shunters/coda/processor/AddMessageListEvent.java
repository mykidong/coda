package io.shunters.coda.processor;

import io.shunters.coda.offset.QueueShardMessageList;

import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddMessageListEvent {

    private BaseEvent baseEvent;
    private int messageId;
    private List<QueueShardMessageList> queueShardMessageLists;

    public AddMessageListEvent(BaseEvent baseEvent, int messageId, List<QueueShardMessageList> queueShardMessageLists)
    {
        this.baseEvent = baseEvent;
        this.messageId = messageId;
        this.queueShardMessageLists = queueShardMessageLists;
    }

    public BaseEvent getBaseEvent() {
        return baseEvent;
    }

    public int getMessageId() {
        return messageId;
    }

    public List<QueueShardMessageList> getQueueShardMessageLists() {
        return queueShardMessageLists;
    }
}
