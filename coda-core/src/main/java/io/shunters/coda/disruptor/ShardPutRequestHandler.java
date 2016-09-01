package io.shunters.coda.disruptor;

import com.lmax.disruptor.EventHandler;
import io.shunters.coda.CommandProcessor;
import io.shunters.coda.NioSelector;
import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutResponse;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ShardPutRequestHandler implements EventHandler<ShardPutRequestEvent> {

    @Override
    public void onEvent(ShardPutRequestEvent shardPutRequestEvent, long l, boolean b) throws Exception {

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
