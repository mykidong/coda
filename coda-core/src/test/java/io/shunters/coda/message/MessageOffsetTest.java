package io.shunters.coda.message;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-30.
 */
public class MessageOffsetTest {

    @Test
    public void serialize()
    {
        MessageOffset messageOffset = buildInstance();

        int length = messageOffset.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        messageOffset.writeToBuffer(buffer);

        buffer.rewind();

        MessageOffset ret = MessageOffset.fromByteBuffer(buffer);

        Assert.assertTrue(messageOffset.getOffset() == ret.getOffset());
        Assert.assertTrue(messageOffset.getLength() == ret.getLength());
    }

    public static MessageOffset buildInstance()
    {
        long offset = 23003039L;
        Message message = MessageTest.buildInstance();
        int length = message.length();

        return new MessageOffset(offset, length, message);
    }
}
