package io.shunters.coda.message;

import io.shunters.coda.CommandProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-30.
 */
public class BaseRequestHeaderTest {

    @Test
    public void serialize()
    {
        short commandId = CommandProcessor.PUT_REQUEST;
        short version = 1;
        int messageId = 20345;
        String clientId = "test-client-id";

        BaseRequestHeader baseRequestHeader = new BaseRequestHeader(commandId, version, messageId, clientId);

        int length = baseRequestHeader.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        // convert to byte buffer.
        baseRequestHeader.writeToBuffer(buffer);

        buffer.rewind();

        // convert from byte buffer.
        BaseRequestHeader ret = BaseRequestHeader.fromByteBuffer(buffer);


        Assert.assertTrue(commandId == ret.getCommandId());
        Assert.assertTrue(version == ret.getVersion());
        Assert.assertTrue(messageId == ret.getMessageId());
        Assert.assertEquals(clientId, ret.getClientId());
    }

}
