package io.shunters.coda.processor;

import com.lmax.disruptor.EventTranslator;
import io.shunters.coda.offset.QueueShard;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreEventTranslator implements EventTranslator<StoreEvent>{

    private QueueShard queueShard;
    private List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists;

    public void setQueueShard(QueueShard queueShard) {
        this.queueShard = queueShard;
    }

    public void setQueueShardMessageLists(List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    @Override
    public void translateTo(StoreEvent storeEvent, long l) {
        storeEvent.setQueueShard(this.queueShard);
        storeEvent.setQueueShardMessageLists(this.queueShardMessageLists);
    }
}
