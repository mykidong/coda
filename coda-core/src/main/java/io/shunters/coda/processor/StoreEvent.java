package io.shunters.coda.processor;

import com.lmax.disruptor.EventFactory;
import io.shunters.coda.offset.QueueShardMessageList;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreEvent {

    private List<QueueShardMessageList> queueShardMessageLists;


    public List<QueueShardMessageList> getQueueShardMessageLists() {
        return queueShardMessageLists;
    }

    public void setQueueShardMessageLists(List<QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    public static final EventFactory<StoreEvent> FACTORY = new EventFactory<StoreEvent>() {
        @Override
        public StoreEvent newInstance() {
            return new StoreEvent();
        }
    };
}
