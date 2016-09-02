package io.shunters.coda.processor;

import com.codahale.metrics.MetricRegistry;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.message.BaseResponseHeader;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.offset.QueueShard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class StoreProcessor extends AbstractQueueThread<StoreEvent>{

    private MetricRegistry metricRegistry;

    public StoreProcessor()
    {
        metricRegistry = MetricRegistryFactory.getInstance();
    }


    @Override
    public void process(StoreEvent storeEvent)
    {
        // TODO: sort StoreEvent by shard of the queue.

        QueueShard queueShard = storeEvent.getQueueShard();
        MessageList messageList = storeEvent.getMessageList();

        // TODO: Save MessageList to Memstore for the shard of the queue.

        BaseEvent baseEvent = storeEvent.getBaseEvent();

        // IT IS JUST TEST PURPOSE.
        sendResponse(baseEvent);
    }

    private void sendResponse(BaseEvent baseEvent) {
        // IT IS JUST TEST PURPOSE.
        PutResponse putResponse = buildPutResponse();

        ByteBuffer responseBuffer = putResponse.write();
        responseBuffer.rewind();

        SocketChannel socketChannel = baseEvent.getNioSelector().getSocketChannel(baseEvent.getChannelId());
        try {
            while (responseBuffer.hasRemaining()) {
                socketChannel.write(responseBuffer);
            }

            responseBuffer.clear();

            this.metricRegistry.meter("StoreProcessor.write").mark();
        }catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static BaseResponseHeader buildInstance()
    {
        int messageId = 234584;

        BaseResponseHeader baseResponseHeader = new BaseResponseHeader(messageId);

        return baseResponseHeader;
    }

    public static PutResponse.QueuePutResult.ShardPutResult buildShardPutResult()
    {
        int shardId = 1;
        short shardErrorCode = 0;
        long offset = 33424;
        long timestamp = new Date().getTime();

        return new PutResponse.QueuePutResult.ShardPutResult(shardId, shardErrorCode, offset, timestamp);
    }

    public static PutResponse.QueuePutResult buildQueuePutResult()
    {
        String queue = "some-queue-name";

        List<PutResponse.QueuePutResult.ShardPutResult> shardPutResults = new ArrayList<>();
        for(int i = 0; i < 3; i++)
        {
            PutResponse.QueuePutResult.ShardPutResult shardPutResult = buildShardPutResult();
            shardPutResults.add(shardPutResult);
        }

        return new PutResponse.QueuePutResult(queue, shardPutResults);
    }

    public static PutResponse buildPutResponse()
    {
        BaseResponseHeader baseResponseHeader = buildInstance();

        List<PutResponse.QueuePutResult> queuePutResults = new ArrayList<>();

        for(int i = 0; i < 2; i++)
        {
            PutResponse.QueuePutResult queuePutResult = buildQueuePutResult();
            queuePutResults.add(queuePutResult);
        }

        return new PutResponse(baseResponseHeader, queuePutResults);
    }
}
