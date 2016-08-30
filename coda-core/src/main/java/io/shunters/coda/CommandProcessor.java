package io.shunters.coda;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.RequestByteBuffer;

import java.nio.ByteBuffer;

public class CommandProcessor {

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
        int totalSize = this.requestByteBuffer.getTotalSize();

        short commandId = this.requestByteBuffer.getCommandId();

        ByteBuffer buffer = this.requestByteBuffer.getBuffer();


        if(commandId == PUT_REQUEST)
        {
            PutRequest putRequest = PutRequest.fromByteBuffer(buffer, totalSize);

            // TODO:
            // 1. put it to memstore.
            // 2. build response object.
            // 3. convert response to bytebuffer.
            // 4. attache response to channel with interestOps WRITE, which causes channel processor to send response to the client.
        }
        // TODO: add another commands.
        else
        {
            // TODO:
        }
    }

}
