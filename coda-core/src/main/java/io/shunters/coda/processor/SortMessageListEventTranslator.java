package io.shunters.coda.processor;

import com.lmax.disruptor.EventTranslator;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class SortMessageListEventTranslator implements EventTranslator<SortMessageListEvent> {

    private List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists;

    public void setQueueShardMessageLists(List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists) {
        this.queueShardMessageLists = queueShardMessageLists;
    }

    @Override
    public void translateTo(SortMessageListEvent memStoreEvent, long l) {
        memStoreEvent.setQueueShardMessageLists(this.queueShardMessageLists);
    }
}
