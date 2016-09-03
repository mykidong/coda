package io.shunters.coda.processor;

import com.lmax.disruptor.EventTranslator;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class MemStoreTranslator implements EventTranslator<MemStoreEvent> {

    private List<StoreEvent.QueueShardMessageList> queueShardMessageLists;

    public void setQueueShardMessageLists(List<StoreEvent.QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    @Override
    public void translateTo(MemStoreEvent memStoreEvent, long l) {
        memStoreEvent.setQueueShardMessageLists(this.queueShardMessageLists);
    }
}
