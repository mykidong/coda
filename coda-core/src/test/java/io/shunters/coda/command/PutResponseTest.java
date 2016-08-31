package io.shunters.coda.command;

import io.shunters.coda.message.BaseResponseHeader;
import io.shunters.coda.message.BaseResponseHeaderTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by mykidong on 2016-08-31.
 */
public class PutResponseTest {

    @Test
    public void serializeShardPutResult()
    {
        PutResponse.QueuePutResult.ShardPutResult shardPutResult = buildShardPutResult();

        int length = shardPutResult.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        shardPutResult.writeToBuffer(buffer);

        buffer.rewind();

        PutResponse.QueuePutResult.ShardPutResult ret = PutResponse.QueuePutResult.ShardPutResult.fromByteBuffer(buffer);

        Assert.assertTrue(shardPutResult.getShardId() == ret.getShardId());
        Assert.assertTrue(shardPutResult.getShardErrorCode() == ret.getShardErrorCode());
        Assert.assertTrue(shardPutResult.getOffset() == ret.getOffset());
        Assert.assertTrue(shardPutResult.getTimestamp() == ret.getTimestamp());
    }

    public static PutResponse.QueuePutResult.ShardPutResult buildShardPutResult()
    {
        int shardId = 1;
        short shardErrorCode = 0;
        long offset = 33424;
        long timestamp = new Date().getTime();

        return new PutResponse.QueuePutResult.ShardPutResult(shardId, shardErrorCode, offset, timestamp);
    }

    @Test
    public void serializeQueuePutResult()
    {
        PutResponse.QueuePutResult queuePutResult = buildQueuePutResult();

        int length = queuePutResult.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        queuePutResult.writeToBuffer(buffer);

        buffer.rewind();

        PutResponse.QueuePutResult ret = PutResponse.QueuePutResult.fromByteBuffer(buffer);

        Assert.assertEquals(queuePutResult.getQueue(), ret.getQueue());
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

    @Test
    public void serializePutResponse()
    {
        PutResponse putResponse = buildPutResponse();

        int length = putResponse.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        putResponse.writeToBuffer(buffer);

        buffer.rewind();

        PutResponse ret = PutResponse.fromByteBuffer(buffer);

        Assert.assertEquals(putResponse.getQueuePutResults().size(), ret.getQueuePutResults().size());
    }

    public static PutResponse buildPutResponse()
    {
        BaseResponseHeader baseResponseHeader = BaseResponseHeaderTest.buildInstance();

        List<PutResponse.QueuePutResult> queuePutResults = new ArrayList<>();

        for(int i = 0; i < 7; i++)
        {
            PutResponse.QueuePutResult queuePutResult = buildQueuePutResult();
            queuePutResults.add(queuePutResult);
        }

        return new PutResponse(baseResponseHeader, queuePutResults);
    }

    @Test
    public void serializeResponse()
    {
        PutResponse putResponse = buildPutResponse();

        ByteBuffer buffer = putResponse.write();

        buffer.rewind();

        int totalSize = buffer.getInt();

        PutResponse ret = PutResponse.fromByteBuffer(buffer);

        Assert.assertEquals(putResponse.getQueuePutResults().size(), ret.getQueuePutResults().size());
    }
}
