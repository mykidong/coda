package io.shunters.coda.message;

import io.shunters.coda.processor.ToRequestProcessor;
import io.shunters.coda.protocol.ClientServerSpec;
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
        BaseRequestHeader baseRequestHeader = buildInstance();

        int length = baseRequestHeader.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        // convert to byte buffer.
        baseRequestHeader.writeToBuffer(buffer);

        buffer.rewind();

        // convert from byte buffer.
        BaseRequestHeader ret = BaseRequestHeader.fromByteBuffer(buffer);


        Assert.assertTrue(baseRequestHeader.getCommandId() == ret.getCommandId());
        Assert.assertTrue(baseRequestHeader.getVersion() == ret.getVersion());
        Assert.assertTrue(baseRequestHeader.getMessageId() == ret.getMessageId());
        Assert.assertEquals(baseRequestHeader.getClientId(), ret.getClientId());
    }

    public static BaseRequestHeader buildInstance()
    {
        short commandId = ClientServerSpec.API_KEY_PRODUCE_REQUEST;
        short version = 1;
        int messageId = 20345;
        String clientId = "test-client-id";

        BaseRequestHeader baseRequestHeader = new BaseRequestHeader(commandId, version, messageId, clientId);

        return baseRequestHeader;
    }

}
