package io.shunters.coda.processor;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreEvent {

    private List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists;

    public StoreEvent(List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists)
    {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    public List<AddMessageListEvent.QueueShardMessageList> getQueueShardMessageLists() {
        return queueShardMessageLists;
    }
}
