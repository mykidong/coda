package io.shunters.coda.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreProcessor extends AbstractQueueThread<StoreEvent> {

    private static Logger log = LoggerFactory.getLogger(StoreProcessor.class);

    @Override
    public void process(StoreEvent event) {
        List<AddMessageListEvent.QueueShardMessageList> messageList = event.getQueueShardMessageLists();
        // TODO: Save MessageList to Segment File!!!
    }
}
