package io.shunters.coda.processor;

import com.lmax.disruptor.EventTranslator;
import io.shunters.coda.offset.QueueShardMessageList;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreEventTranslator implements EventTranslator<StoreEvent>{

    private List<QueueShardMessageList> queueShardMessageLists;

    public void setQueueShardMessageLists(List<QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    @Override
    public void translateTo(StoreEvent storeEvent, long l) {
        storeEvent.setQueueShardMessageLists(this.queueShardMessageLists);
    }
}
