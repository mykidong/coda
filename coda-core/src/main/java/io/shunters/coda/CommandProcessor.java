package io.shunters.coda;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.command.RequestByteBuffer;
import io.shunters.coda.message.BaseResponseHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CommandProcessor {

    private static Logger log = LoggerFactory.getLogger(CommandProcessor.class);

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


    private RequestByteBuffer requestByteBuffer;

    public CommandProcessor(RequestByteBuffer requestByteBuffer)
    {
        this.requestByteBuffer = requestByteBuffer;
    }

    public void process()
    {
        String channelId = this.requestByteBuffer.getChannelId();

        short commandId = this.requestByteBuffer.getCommandId();

        ByteBuffer buffer = this.requestByteBuffer.getBuffer();

        NioSelector nioSelector = this.requestByteBuffer.getNioSelector();

        ByteBuffer responseBuffer = null;

        if(commandId == PUT_REQUEST)
        {
            PutRequest putRequest = PutRequest.fromByteBuffer(buffer);

            // TODO:
            // 1. put it to memstore.
            // 2. build response object.
            // 3. convert response to bytebuffer.
            // 4. attache response to channel with interestOps WRITE, which causes channel processor to send response to the client.


            // IT IS JUST TEST PURPOSE.
            PutResponse putResponse = buildPutResponse();


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
        for(int i = 0; i < 5; i++)
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

        for(int i = 0; i < 7; i++)
        {
            PutResponse.QueuePutResult queuePutResult = buildQueuePutResult();
            queuePutResults.add(queuePutResult);
        }

        return new PutResponse(baseResponseHeader, queuePutResults);
    }

}
