package io.shunters.coda.message;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-30.
 */
public class BaseResponseHeaderTest {

    @Test
    public void serialize()
    {

        BaseResponseHeader baseResponseHeader = buildInstance();

        int length = baseResponseHeader.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        baseResponseHeader.writeToBuffer(buffer);

        buffer.rewind();

        BaseResponseHeader ret = BaseResponseHeader.fromByteBuffer(buffer);

        Assert.assertTrue(baseResponseHeader.getMessageId() == ret.getMessageId());
    }

    public static BaseResponseHeader buildInstance()
    {
        int messageId = 234584;

        BaseResponseHeader baseResponseHeader = new BaseResponseHeader(messageId);

        return baseResponseHeader;
    }
}
