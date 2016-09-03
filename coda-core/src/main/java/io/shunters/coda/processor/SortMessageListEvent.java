package io.shunters.coda.processor;

import com.lmax.disruptor.EventFactory;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class SortMessageListEvent {
    private List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists;

    public List<AddMessageListEvent.QueueShardMessageList> getQueueShardMessageLists() {
        return queueShardMessageLists;
    }

    public void setQueueShardMessageLists(List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    public static final EventFactory<SortMessageListEvent> FACTORY = new EventFactory<SortMessageListEvent>() {
        @Override
        public SortMessageListEvent newInstance() {
            return new SortMessageListEvent();
        }
    };
}
