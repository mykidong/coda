package io.shunters.coda.command;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;

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
}
