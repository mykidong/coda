package io.shunters.coda.command;

import io.shunters.coda.message.BaseRequestHeader;
import io.shunters.coda.message.BaseRequestHeaderTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2016-09-12.
 */
public class GetRequestTest {

    @Test
    public void serializeShardGet()
    {
        GetRequest.QueueGet.ShardGet shardGet = buildShardGet();
        int length = shardGet.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);
        shardGet.writeToBuffer(buffer);

        buffer.rewind();

        GetRequest.QueueGet.ShardGet ret = GetRequest.QueueGet.ShardGet.fromByteBuffer(buffer);
        Assert.assertEquals(shardGet.getShardId(), ret.getShardId());
        Assert.assertEquals(shardGet.getOffset(), ret.getOffset());
        Assert.assertEquals(shardGet.getMaxBytes(), ret.getMaxBytes());
    }

    public static GetRequest.QueueGet.ShardGet buildShardGet()
    {
        int shardId = 0;
        long offset = 500;
        int maxBytes = 3000;

        return new GetRequest.QueueGet.ShardGet(shardId, offset, maxBytes);
    }

    @Test
    public void serializeQueueGet()
    {
        GetRequest.QueueGet queueGet = buildQueueGet();
        int length = queueGet.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);
        queueGet.writeToBuffer(buffer);

        buffer.rewind();

        GetRequest.QueueGet ret = GetRequest.QueueGet.fromByteBuffer(buffer);
        Assert.assertEquals(queueGet.getQueue(), ret.getQueue());
        Assert.assertEquals(queueGet.getShardGets().size(), ret.getShardGets().size());
    }

    public static GetRequest.QueueGet buildQueueGet()
    {
        String queue = "any-queue";
        List<GetRequest.QueueGet.ShardGet> shardGets = new ArrayList<>();
        for(int i = 0; i < 5; i++)
        {
            GetRequest.QueueGet.ShardGet shardGet = buildShardGet();
            shardGets.add(shardGet);
        }

        return new GetRequest.QueueGet(queue, shardGets);
    }

    @Test
    public void serializeGetRequest()
    {
        GetRequest getRequest = buildGetRequest();
        int length = getRequest.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);
        getRequest.writeToBuffer(buffer);

        buffer.rewind();

        GetRequest ret = GetRequest.fromByteBuffer(buffer);
        Assert.assertEquals(getRequest.getNodeId(), ret.getNodeId());
        Assert.assertEquals(getRequest.getQueueGets().size(), ret.getQueueGets().size());
    }

    public static GetRequest buildGetRequest()
    {
        BaseRequestHeader baseRequestHeader = BaseRequestHeaderTest.buildInstance();
        int nodeId = 0;
        List<GetRequest.QueueGet> queueGets = new ArrayList<>();
        for(int i = 0; i < 3; i++)
        {
            GetRequest.QueueGet queueGet = buildQueueGet();
            queueGets.add(queueGet);
        }

        return new GetRequest(baseRequestHeader, nodeId, queueGets);
    }

    @Test
    public void serializeRequest()
    {
        GetRequest getRequest = buildGetRequest();

        ByteBuffer buffer = getRequest.write();

        buffer.rewind();

        int totalSize = buffer.getInt();
        System.out.println("totalSize: [" + totalSize + "]");

        GetRequest ret = GetRequest.fromByteBuffer(buffer);

        Assert.assertEquals(getRequest.getNodeId(), ret.getNodeId());
        Assert.assertEquals(getRequest.getQueueGets().size(), ret.getQueueGets().size());
    }
}
