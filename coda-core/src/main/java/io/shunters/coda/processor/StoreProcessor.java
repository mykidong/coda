package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.offset.QueueShard;
import io.shunters.coda.offset.QueueShardMessageList;
import io.shunters.coda.store.StoreHandler;
import io.shunters.coda.store.StoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreProcessor extends AbstractQueueThread<StoreEvent> implements EventHandler<StoreEvent> {

    private static Logger log = LoggerFactory.getLogger(StoreProcessor.class);

    private StoreHandler storeHandler;

    public StoreProcessor()
    {
        storeHandler = StoreManager.getInstance();
    }

    @Override
    public void onEvent(StoreEvent storeEvent, long l, boolean b) throws Exception {
        this.put(storeEvent);
    }

    @Override
    public void process(StoreEvent event) {
        List<QueueShardMessageList> queueShardMessageLists = event.getQueueShardMessageLists();

        for(QueueShardMessageList queueShardMessageList : queueShardMessageLists)
        {
            QueueShard queueShard = queueShardMessageList.getQueueShard();
            long firstOffset = queueShardMessageList.getFirstOffset();
            MessageList messageList = queueShardMessageList.getMessageList();

           storeHandler.add(queueShard, firstOffset, messageList);
        }
    }
}
