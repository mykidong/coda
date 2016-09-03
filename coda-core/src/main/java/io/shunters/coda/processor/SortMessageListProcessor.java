package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.offset.QueueShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by mykidong on 2016-09-03.
 */
public class SortMessageListProcessor extends AbstractQueueThread<SortMessageListEvent> implements EventHandler<SortMessageListEvent> {

    private static Logger log = LoggerFactory.getLogger(SortMessageListProcessor.class);

    private ConcurrentMap<QueueShard, List<MessageList>> messageListMap;

    public SortMessageListProcessor()
    {
        this.messageListMap = new ConcurrentHashMap<>();
    }



    @Override
    public void onEvent(SortMessageListEvent memStoreEvent, long l, boolean b) throws Exception {
        this.process(memStoreEvent);
    }

    public void process(SortMessageListEvent event) {

        // TODO:

        List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists = event.getQueueShardMessageLists();
        for(AddMessageListEvent.QueueShardMessageList queueShardMessageList : queueShardMessageLists)
        {
            QueueShard queueShard = queueShardMessageList.getQueueShard();

            MessageList messageList = queueShardMessageList.getMessageList();

            if(this.messageListMap.containsKey(queueShard))
            {
                this.messageListMap.get(queueShard).add(messageList);
            }
            else
            {
                List<MessageList> messageLists = new ArrayList<>();
                messageLists.add(messageList);
                this.messageListMap.put(queueShard, messageLists);
            }
        }

    }
}
