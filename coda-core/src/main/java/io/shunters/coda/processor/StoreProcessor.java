package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;
import io.shunters.coda.offset.QueueShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreProcessor extends AbstractQueueThread<StoreEvent> implements EventHandler<StoreEvent> {

    private static Logger log = LoggerFactory.getLogger(StoreProcessor.class);

    @Override
    public void onEvent(StoreEvent storeEvent, long l, boolean b) throws Exception {
        this.put(storeEvent);
    }

    @Override
    public void process(StoreEvent event) {
        QueueShard queueShard = event.getQueueShard();
        List<AddMessageListEvent.QueueShardMessageList> messageList = event.getQueueShardMessageLists();

        // TODO: Save MessageList to File!!!
    }
}
