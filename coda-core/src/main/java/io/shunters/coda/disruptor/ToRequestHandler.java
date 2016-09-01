package io.shunters.coda.disruptor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.CommandProcessor;
import io.shunters.coda.NioSelector;
import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.command.RequestByteBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ToRequestHandler implements EventHandler<ToRequestEvent> {

    private Disruptor<ShardPutRequestEvent> shardPutRequestEventDisruptor;
    private ShardPutRequestTranslator shardPutRequestTranslator;

    public ToRequestHandler()
    {
        String disruptorName = "ShardPutRequest-" + Thread.currentThread().getId();
        shardPutRequestEventDisruptor = DisruptorBuilder.newInstance(disruptorName, ShardPutRequestEvent.FACTORY, 1024, new ShardPutRequestHandler());
        shardPutRequestTranslator = new ShardPutRequestTranslator();
    }


    @Override
    public void onEvent(ToRequestEvent toRequestEvent, long l, boolean b) throws Exception {

        RequestByteBuffer requestByteBuffer = toRequestEvent.getRequestByteBuffer();

        String channelId = requestByteBuffer.getChannelId();
        short commandId = requestByteBuffer.getCommandId();
        ByteBuffer buffer = requestByteBuffer.getBuffer();
        NioSelector nioSelector = requestByteBuffer.getNioSelector();

        ByteBuffer responseBuffer = null;

        if(commandId == CommandProcessor.PUT_REQUEST)
        {
            PutRequest putRequest = PutRequest.fromByteBuffer(buffer);

            // send to ShardPutRequest disruptor.
            shardPutRequestTranslator.setBaseEvent(new BaseEvent(channelId, nioSelector));
            shardPutRequestTranslator.setPutRequest(putRequest);
            shardPutRequestEventDisruptor.publishEvent(shardPutRequestTranslator);
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
