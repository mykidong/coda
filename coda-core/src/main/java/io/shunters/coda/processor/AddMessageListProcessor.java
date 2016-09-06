package io.shunters.coda.processor;

import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.message.BaseResponseHeader;
import io.shunters.coda.offset.QueueShard;
import io.shunters.coda.offset.QueueShardMessageList;
import io.shunters.coda.util.DisruptorBuilder;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddMessageListProcessor extends AbstractQueueThread<AddMessageListEvent>{

    private Disruptor<StoreEvent> storeDisruptor;
    private StoreEventTranslator storeEventTranslator;

    public AddMessageListProcessor()
    {
        StoreProcessor storeProcessor = new StoreProcessor();
        storeProcessor.start();

        storeDisruptor = DisruptorBuilder.singleton("Store", StoreEvent.FACTORY, 1024, storeProcessor);
        this.storeEventTranslator = new StoreEventTranslator();
    }


    @Override
    public void process(AddMessageListEvent storeEvent)
    {
        BaseEvent baseEvent = storeEvent.getBaseEvent();
        int messageId = storeEvent.getMessageId();
        List<QueueShardMessageList> queueShardMessageLists = storeEvent.getQueueShardMessageLists();

        // put MessageList to StoreProcessor for the shard of the queue.
        this.storeEventTranslator.setQueueShardMessageLists(queueShardMessageLists);
        this.storeDisruptor.publishEvent(this.storeEventTranslator);

        // prepare response.
        BaseResponseHeader baseResponseHeader = new BaseResponseHeader(messageId);

        Map<String, List<PutResponse.QueuePutResult.ShardPutResult>> queueShardPutResultMap = new HashMap<>();

        for(QueueShardMessageList queueShardMessageList : queueShardMessageLists)
        {
            QueueShard queueShard = queueShardMessageList.getQueueShard();

            String queue = queueShard.getQueue();
            int shardId = queueShard.getShardId();

            short shardErrorCode = 0;

            long firstOffset = queueShardMessageList.getFirstOffset();

            // TODO: set timestamp with respect to timestampType.
            long timestamp = -1;

            PutResponse.QueuePutResult.ShardPutResult shardPutResult = new PutResponse.QueuePutResult.ShardPutResult(shardId, shardErrorCode, firstOffset, timestamp);

            if(queueShardPutResultMap.containsKey(queue))
            {
                queueShardPutResultMap.get(queue).add(shardPutResult);
            }
            else
            {
                List<PutResponse.QueuePutResult.ShardPutResult> shardPutResults = new ArrayList<>();
                shardPutResults.add(shardPutResult);
                queueShardPutResultMap.put(queue, shardPutResults);
            }
        }

        List<PutResponse.QueuePutResult> queuePutResults = new ArrayList<>();
        for(String queue : queueShardPutResultMap.keySet())
        {
            PutResponse.QueuePutResult queuePutResult = new PutResponse.QueuePutResult(queue, queueShardPutResultMap.get(queue));
            queuePutResults.add(queuePutResult);
        }

        // build PutResponse.
        PutResponse putResponse = new PutResponse(baseResponseHeader, queuePutResults);
        ByteBuffer responseBuffer = putResponse.write();

        String channelId = baseEvent.getChannelId();
        NioSelector nioSelector = baseEvent.getNioSelector();

        // attache response to channel with SelectionKey.OP_WRITE, which causes channel processor to send response to the client.
        nioSelector.attach(channelId, SelectionKey.OP_WRITE, responseBuffer);

        // wakeup must be called.
        nioSelector.wakeup();
    }
}
