package io.shunters.coda.message;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * Created by mykidong on 2016-08-30.
 */
public class MessageTest {

    @Test
    public void serialize()
    {
        int crc = 30404002;
        byte formatVersion = 1;
        byte compression = 0;
        byte timestampType = 0;
        long timestamp = new Date().getTime();
        byte[] key = "any-key".getBytes();
        byte[] value = "any-value".getBytes();

        Message message = new Message(crc, formatVersion, compression, timestampType, timestamp, key, value);

        int length = message.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        message.writeToBuffer(buffer);

        buffer.rewind();

        Message ret = Message.fromByteBuffer(buffer);

        Assert.assertTrue(crc == ret.getCrc());
        Assert.assertTrue(formatVersion == ret.getFormatVersion());
        Assert.assertTrue(compression == ret.getCompression());
        Assert.assertTrue(timestampType == ret.getTimestampType());
        Assert.assertTrue(timestamp == ret.getTimestamp());
        Assert.assertEquals(new String(key), new String(ret.getKey()));
        Assert.assertEquals(new String(value), new String(ret.getValue()));
    }
}
