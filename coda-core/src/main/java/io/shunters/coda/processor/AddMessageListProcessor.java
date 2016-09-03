package io.shunters.coda.processor;

import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.message.BaseResponseHeader;
import io.shunters.coda.offset.QueueShard;
import io.shunters.coda.util.DisruptorBuilder;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.*;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddMessageListProcessor extends AbstractQueueThread<AddMessageListEvent>{

    private Disruptor<SortMessageListEvent> sortMessageListDisruptor;
    private SortMessageListEventTranslator sortMessageListEventTranslator;

    public AddMessageListProcessor()
    {
        SortMessageListProcessor sortMessageListProcessor = new SortMessageListProcessor();
        sortMessageListProcessor.start();
        sortMessageListDisruptor = DisruptorBuilder.singleton("SortMessageList", SortMessageListEvent.FACTORY, 1024, sortMessageListProcessor);
        this.sortMessageListEventTranslator = new SortMessageListEventTranslator();
    }


    @Override
    public void process(AddMessageListEvent storeEvent)
    {
        BaseEvent baseEvent = storeEvent.getBaseEvent();
        int messageId = storeEvent.getMessageId();
        List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists = storeEvent.getQueueShardMessageLists();

        // put MessageList to SortMessageListProcessor for the shard of the queue.
        this.sortMessageListEventTranslator.setQueueShardMessageLists(queueShardMessageLists);
        this.sortMessageListDisruptor.publishEvent(this.sortMessageListEventTranslator);

        // prepare response.
        BaseResponseHeader baseResponseHeader = new BaseResponseHeader(messageId);

        Map<String, List<PutResponse.QueuePutResult.ShardPutResult>> queueShardPutResultMap = new HashMap<>();

        for(AddMessageListEvent.QueueShardMessageList queueShardMessageList : queueShardMessageLists)
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
