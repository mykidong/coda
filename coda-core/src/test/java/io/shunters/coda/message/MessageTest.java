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
        Message message = buildInstance();

        int length = message.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        message.writeToBuffer(buffer);

        buffer.rewind();

        Message ret = Message.fromByteBuffer(buffer);

        Assert.assertTrue(message.getCrc() == ret.getCrc());
        Assert.assertTrue(message.getFormatVersion() == ret.getFormatVersion());
        Assert.assertTrue(message.getCompression() == ret.getCompression());
        Assert.assertTrue(message.getTimestampType() == ret.getTimestampType());
        Assert.assertTrue(message.getTimestamp() == ret.getTimestamp());
        Assert.assertEquals(new String(message.getKey()), new String(ret.getKey()));
        Assert.assertEquals(new String(message.getValue()), new String(ret.getValue()));
    }

    public static Message buildInstance()
    {
        int crc = 30404002;
        byte formatVersion = 1;
        byte compression = 0;
        byte timestampType = 0;
        long timestamp = new Date().getTime();
        byte[] key = "any-key".getBytes();
        byte[] value = "any-value".getBytes();

        Message message = new Message(crc, formatVersion, compression, timestampType, timestamp, key, value);

        return message;
    }
}
