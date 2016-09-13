package io.shunters.coda.command;

import io.shunters.coda.message.BaseResponseHeader;
import io.shunters.coda.message.BaseResponseHeaderTest;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageListTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2016-09-13.
 */
public class GetResponseTest {

    @Test
    public void serializeShardGetResult()
    {
        GetResponse.QueueGetResult.ShardGetResult shardGetResult = buildShardGetResult();
        int length = shardGetResult.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);
        shardGetResult.writeToBuffer(buffer);

        buffer.rewind();

        GetResponse.QueueGetResult.ShardGetResult ret = GetResponse.QueueGetResult.ShardGetResult.fromByteBuffer(buffer);
        Assert.assertEquals(shardGetResult.getShardId(), ret.getShardId());
        Assert.assertEquals(shardGetResult.getShardErrorCode(), ret.getShardErrorCode());
        Assert.assertEquals(shardGetResult.getMessageList().getMessageOffsets().size(), ret.getMessageList().getMessageOffsets().size());
    }

    public static GetResponse.QueueGetResult.ShardGetResult buildShardGetResult()
    {
        int shardId = 0;
        short shardErrorCode = 0;
        MessageList messageList = MessageListTest.buildInstance();

        return new GetResponse.QueueGetResult.ShardGetResult(shardId, shardErrorCode, messageList);
    }

    @Test
    public void serializeQueueGetResult()
    {
        GetResponse.QueueGetResult queueGetResult = buildQueueGetResult();
        int length = queueGetResult.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);
        queueGetResult.writeToBuffer(buffer);

        buffer.rewind();

        GetResponse.QueueGetResult ret = GetResponse.QueueGetResult.fromByteBuffer(buffer);
        Assert.assertEquals(queueGetResult.getQueue(), ret.getQueue());
        Assert.assertEquals(queueGetResult.getShardGetResults().size(), ret.getShardGetResults().size());
    }

    public static GetResponse.QueueGetResult buildQueueGetResult()
    {
        String queue = "any-queue";
        List<GetResponse.QueueGetResult.ShardGetResult> shardGetResults = new ArrayList<>();
        for(int i = 0; i < 3; i++)
        {
            GetResponse.QueueGetResult.ShardGetResult shardGetResult = buildShardGetResult();
            shardGetResults.add(shardGetResult);
        }

        return new GetResponse.QueueGetResult(queue, shardGetResults);
    }

    @Test
    public void serializeGetResponse()
    {
        GetResponse getResponse = buildGetResponse();
        int length = getResponse.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);
        getResponse.writeToBuffer(buffer);

        buffer.rewind();

        GetResponse ret = GetResponse.fromByteBuffer(buffer);

        Assert.assertEquals(getResponse.getBaseResponseHeader().getMessageId(), ret.getBaseResponseHeader().getMessageId());
        Assert.assertEquals(getResponse.getQueueGetResults().size(), ret.getQueueGetResults().size());
    }

    public static GetResponse buildGetResponse()
    {
        BaseResponseHeader baseResponseHeader = BaseResponseHeaderTest.buildInstance();
        List<GetResponse.QueueGetResult> queueGetResults = new ArrayList<>();
        for(int i = 0; i < 5; i++)
        {
            GetResponse.QueueGetResult queueGetResult = buildQueueGetResult();
            queueGetResults.add(queueGetResult);
        }

        return new GetResponse(baseResponseHeader, queueGetResults);
    }

    @Test
    public void serializeResponse()
    {
        GetResponse getResponse = buildGetResponse();
        ByteBuffer buffer = getResponse.write();

        buffer.rewind();

        int totalSize = buffer.getInt();
        System.out.println("totalSize: [" + totalSize + "]");

        GetResponse ret = GetResponse.fromByteBuffer(buffer);

        Assert.assertEquals(getResponse.getBaseResponseHeader().getMessageId(), ret.getBaseResponseHeader().getMessageId());
        Assert.assertEquals(getResponse.getQueueGetResults().size(), ret.getQueueGetResults().size());
    }
}
