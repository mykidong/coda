package io.shunters.coda.pipeline;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.RequestByteBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ToRequestProcessor extends AbstractQueueThread {

    public static final short META_REQUEST = 0;

    public static final short PUT_REQUEST = 100;
    public static final short GET_REQUEST = 101;

    public static final short OFFSET_REQUEST = 200;
    public static final short OFFSET_PUT_REQUEST = 201;
    public static final short OFFSET_GET_REQUEST = 202;

    public static final short CONSUMER_GROUP_COORDINATOR_REQUEST = 300;
    public static final short JOIN_GROUP_REQUEST = 301;
    public static final short HEARTBEAT_GROUP_REQUEST = 302;
    public static final short LEAVE_GROUP_REQUEST = 303;
    public static final short SYNC_GROUP_REQUEST = 304;
    public static final short DESCRIBE_GROUPS_REQUEST = 305;
    public static final short LIST_GROUPS_REQUEST = 306;

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
                Object obj = this.queue.take();

                RequestByteBuffer requestByteBuffer = (RequestByteBuffer) obj;
                process(requestByteBuffer);
            }
        }catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void process(RequestByteBuffer requestByteBuffer)
    {
        String channelId = requestByteBuffer.getChannelId();
        short commandId = requestByteBuffer.getCommandId();
        ByteBuffer buffer = requestByteBuffer.getBuffer();
        NioSelector nioSelector = requestByteBuffer.getNioSelector();

        ByteBuffer responseBuffer = null;

        if(commandId == PUT_REQUEST)
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
