package io.shunters.coda.processor;

import io.shunters.coda.message.Message;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.offset.QueueShard;

import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class StoreEvent {

    private BaseEvent baseEvent;
    private int messageId;
    private List<QueueShardMessageList> queueShardMessageLists;

    public StoreEvent(BaseEvent baseEvent, int messageId, List<QueueShardMessageList> queueShardMessageLists)
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

    public static class  QueueShardMessageList
    {
        private QueueShard queueShard;
        private MessageList messageList;

        public QueueShardMessageList(QueueShard queueShard, MessageList messageList)
        {
            this.queueShard = queueShard;
            this.messageList = messageList;
        }

        public QueueShard getQueueShard() {
            return queueShard;
        }

        public MessageList getMessageList() {
            return messageList;
        }
    }
}
