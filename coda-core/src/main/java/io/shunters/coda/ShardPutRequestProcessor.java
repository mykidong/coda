package io.shunters.coda;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.pipeline.BaseEvent;
import io.shunters.coda.pipeline.ShardPutRequestEvent;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.TimeUnit;

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
        PutResponse putResponse = CommandProcessor.buildPutResponse();

        ByteBuffer responseBuffer = putResponse.write();

        String channelId = baseEvent.getChannelId();
        NioSelector nioSelector = baseEvent.getNioSelector();

        // attache response to channel with SelectionKey.OP_WRITE, which causes channel processor to send response to the client.
        nioSelector.attach(channelId, SelectionKey.OP_WRITE, responseBuffer);
        nioSelector.wakeup();
    }
}
