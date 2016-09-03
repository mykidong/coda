package io.shunters.coda.processor;

import com.lmax.disruptor.EventFactory;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class MemStoreEvent {
    private List<StoreEvent.QueueShardMessageList> queueShardMessageLists;

    public List<StoreEvent.QueueShardMessageList> getQueueShardMessageLists() {
        return queueShardMessageLists;
    }

    public void setQueueShardMessageLists(List<StoreEvent.QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    public static final EventFactory<MemStoreEvent> FACTORY = new EventFactory<MemStoreEvent>() {
        @Override
        public MemStoreEvent newInstance() {
            return new MemStoreEvent();
        }
    };
}
