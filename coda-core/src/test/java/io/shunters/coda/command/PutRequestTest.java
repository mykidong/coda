package io.shunters.coda.command;

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
        System.out.println("length: [" + length + "]");

        ByteBuffer buffer = ByteBuffer.allocate(length);

        shardMessageWrap.writeToBuffer(buffer);

        buffer.rewind();

        PutRequest.QueueMessageWrap.ShardMessageWrap ret = PutRequest.QueueMessageWrap.ShardMessageWrap.fromByteBuffer(buffer);

        Assert.assertTrue(shardMessageWrap.getShardId() == ret.getShardId());
        Assert.assertTrue(shardMessageWrap.getLength() == ret.getLength());
    }

    public static PutRequest.QueueMessageWrap.ShardMessageWrap buildShardMessageWrap()
    {
        int shardId = 0;

        MessageList messageList = MessageListTest.buildInstance();

        int length = messageList.length();

        return new PutRequest.QueueMessageWrap.ShardMessageWrap(shardId, length, messageList);
    }


    @Test
    public void serializeQueueMessageWrap()
    {
        PutRequest.QueueMessageWrap queueMessageWrap = buildQueueMessageWrap();

        int length = queueMessageWrap.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        queueMessageWrap.writeToBuffer(buffer);

        buffer.rewind();

        PutRequest.QueueMessageWrap ret = PutRequest.QueueMessageWrap.fromByteBuffer(buffer, length);

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


}
