package io.shunters.coda.command;

import io.shunters.coda.message.BaseRequestHeader;
import io.shunters.coda.message.BaseRequestHeaderTest;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageListTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2016-08-30.
 */
public class PutRequestTest {

    @Test
    public void serializeShardMessageWrap()
    {
        PutRequest.QueueMessageWrap.ShardMessageWrap shardMessageWrap = buildShardMessageWrap();

        int length = shardMessageWrap.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        shardMessageWrap.writeToBuffer(buffer);

        buffer.rewind();

        PutRequest.QueueMessageWrap.ShardMessageWrap ret = PutRequest.QueueMessageWrap.ShardMessageWrap.fromByteBuffer(buffer);

        Assert.assertTrue(shardMessageWrap.getShardId() == ret.getShardId());
    }

    public static PutRequest.QueueMessageWrap.ShardMessageWrap buildShardMessageWrap()
    {
        int shardId = 0;

        MessageList messageList = MessageListTest.buildInstance();

        return new PutRequest.QueueMessageWrap.ShardMessageWrap(shardId, messageList);
    }


    @Test
    public void serializeQueueMessageWrap()
    {
        PutRequest.QueueMessageWrap queueMessageWrap = buildQueueMessageWrap();

        int length = queueMessageWrap.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        queueMessageWrap.writeToBuffer(buffer);

        buffer.rewind();

        PutRequest.QueueMessageWrap ret = PutRequest.QueueMessageWrap.fromByteBuffer(buffer);

        Assert.assertEquals(queueMessageWrap.getQueue(), ret.getQueue());
    }


    public static PutRequest.QueueMessageWrap buildQueueMessageWrap()
    {
        String queue = "any-queue";
        List<PutRequest.QueueMessageWrap.ShardMessageWrap> shardMessageWraps = new ArrayList<>();

        for(int i = 0; i < 5; i++)
        {
            PutRequest.QueueMessageWrap.ShardMessageWrap shardMessageWrap = buildShardMessageWrap();
            shardMessageWraps.add(shardMessageWrap);
        }

        PutRequest.QueueMessageWrap queueMessageWrap = new PutRequest.QueueMessageWrap(queue, shardMessageWraps);

        return queueMessageWrap;
    }


    @Test
    public void serializePutRequest()
    {
        PutRequest putRequest = buildPutRequest();

        int length = putRequest.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        putRequest.writeToBuffer(buffer);

        buffer.rewind();

        PutRequest ret = PutRequest.fromByteBuffer(buffer);

        Assert.assertTrue(putRequest.getAcks() == ret.getAcks());
    }


    public static PutRequest buildPutRequest()
    {
        BaseRequestHeader baseRequestHeader = BaseRequestHeaderTest.buildInstance();
        short acks = 0;

        List<PutRequest.QueueMessageWrap> queueMessageWraps = new ArrayList<>();
        for(int i = 0; i < 3; i++)
        {
            PutRequest.QueueMessageWrap queueMessageWrap = buildQueueMessageWrap();

            queueMessageWraps.add(queueMessageWrap);
        }

        return new PutRequest(baseRequestHeader, acks, queueMessageWraps);
    }

    @Test
    public void serializeRequest()
    {
        PutRequest putRequest = buildPutRequest();

        ByteBuffer buffer = putRequest.write();

        buffer.rewind();

        int totalSize = buffer.getInt();
        System.out.println("totalSize: [" + totalSize + "]");

        PutRequest ret = PutRequest.fromByteBuffer(buffer);

        Assert.assertTrue(putRequest.getAcks() == ret.getAcks());
    }
}
