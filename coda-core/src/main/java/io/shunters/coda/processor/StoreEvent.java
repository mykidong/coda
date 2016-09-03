package io.shunters.coda.processor;

import com.lmax.disruptor.EventFactory;
import io.shunters.coda.offset.QueueShard;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreEvent {

    private QueueShard queueShard;
    private List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists;

    public QueueShard getQueueShard() {
        return queueShard;
    }

    public void setQueueShard(QueueShard queueShard) {
        this.queueShard = queueShard;
    }

    public List<AddMessageListEvent.QueueShardMessageList> getQueueShardMessageLists() {
        return queueShardMessageLists;
    }

    public void setQueueShardMessageLists(List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    public static final EventFactory<StoreEvent> FACTORY = new EventFactory<StoreEvent>() {
        @Override
        public StoreEvent newInstance() {
            return new StoreEvent();
        }
    };
}
