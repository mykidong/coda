package io.shunters.coda.disruptor;

import com.lmax.disruptor.EventHandler;
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

            // TODO:
            // 1. put it to memstore.
            // 2. build response object.
            // 3. convert response to bytebuffer.
            // 4. attache response to channel with interestOps WRITE, which causes channel processor to send response to the client.


            // IT IS JUST TEST PURPOSE.
            PutResponse putResponse = CommandProcessor.buildPutResponse();


            responseBuffer = putResponse.write();
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
