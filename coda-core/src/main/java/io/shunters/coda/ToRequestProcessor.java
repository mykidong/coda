package io.shunters.coda;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.RequestByteBuffer;
import io.shunters.coda.pipeline.BaseEvent;
import io.shunters.coda.pipeline.ShardPutRequestEvent;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.TimeUnit;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ToRequestProcessor extends AbstractQueueThread {

    private ShardPutRequestProcessor shardPutRequestProcessor;

    public ToRequestProcessor()
    {
        this.shardPutRequestProcessor = new ShardPutRequestProcessor();
        this.shardPutRequestProcessor.start();
    }


    @Override
    public void run()
    {
        try {
            while (true) {
                Object obj = this.queue.poll(500, TimeUnit.MILLISECONDS);
                if(obj == null)
                {
                    continue;
                }

                RequestByteBuffer requestByteBuffer = (RequestByteBuffer) obj;
                process(requestByteBuffer);
            }
        }catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void process(Object obj)
    {
        RequestByteBuffer requestByteBuffer = (RequestByteBuffer) obj;

        String channelId = requestByteBuffer.getChannelId();
        short commandId = requestByteBuffer.getCommandId();
        ByteBuffer buffer = requestByteBuffer.getBuffer();
        NioSelector nioSelector = requestByteBuffer.getNioSelector();

        ByteBuffer responseBuffer = null;

        if(commandId == CommandProcessor.PUT_REQUEST)
        {
            PutRequest putRequest = PutRequest.fromByteBuffer(buffer);

            ShardPutRequestEvent shardPutRequestEvent = new ShardPutRequestEvent(new BaseEvent(channelId, nioSelector), putRequest);

            shardPutRequestProcessor.put(shardPutRequestEvent);
        }
        // TODO: add another commands.
        else
        {
            // TODO:
        }


        if(responseBuffer != null) {
            // attache response to channel with SelectionKey.OP_WRITE, which causes channel processor to send response to the client.
            nioSelector.attach(channelId, SelectionKey.OP_WRITE, responseBuffer);
            nioSelector.wakeup();
        }
    }
}
