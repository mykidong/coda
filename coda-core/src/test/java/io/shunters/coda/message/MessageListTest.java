package io.shunters.coda.message;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2016-08-30.
 */
public class MessageListTest {

    @Test
    public void serialize()
    {
        MessageList messageList = buildInstance();

        int length = messageList.length();

        ByteBuffer buffer = ByteBuffer.allocate(length);

        messageList.writeToBuffer(buffer);

        buffer.rewind();

        MessageList ret = MessageList.fromByteBuffer(buffer);

        Assert.assertTrue(length == ret.length());
    }

    public static MessageList buildInstance() {

        List<MessageOffset> messageOffsets = new ArrayList<>();
        for(int i = 0; i < 1; i++)
        {
            MessageOffset messageOffset = MessageOffsetTest.buildInstance();
            messageOffsets.add(messageOffset);
        }

        MessageList messageList = new MessageList(messageOffsets);

        return messageList;
    }
}
