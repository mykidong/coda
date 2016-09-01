package io.shunters.coda.pipeline;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.message.BaseResponseHeader;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ShardPutRequestProcessor extends AbstractQueueThread {

    @Override
    public void run()
    {
        try {
            while (true) {
                Object obj = this.queue.take();

                ShardPutRequestEvent shardPutRequestEvent = (ShardPutRequestEvent) obj;
                process(shardPutRequestEvent);
            }
        }catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void process(ShardPutRequestEvent shardPutRequestEvent)
    {
        BaseEvent baseEvent = shardPutRequestEvent.getBaseEvent();

        PutRequest putRequest = shardPutRequestEvent.getPutRequest();

        // IT IS JUST TEST PURPOSE.
        PutResponse putResponse = buildPutResponse();

        ByteBuffer responseBuffer = putResponse.write();

        String channelId = baseEvent.getChannelId();
        NioSelector nioSelector = baseEvent.getNioSelector();

        // attache response to channel with SelectionKey.OP_WRITE, which causes channel processor to send response to the client.
        nioSelector.attach(channelId, SelectionKey.OP_WRITE, responseBuffer);

        // wakeup must be called.
        nioSelector.wakeup();
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
