package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by mykidong on 2016-08-27.
 */
public class MessageList implements ToByteBuffer{

    private List<MessageOffset> messageOffsets;

    public MessageList(List<MessageOffset> messageOffsets)
    {
        this.messageOffsets = messageOffsets;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        for(MessageOffset messageOffset : messageOffsets)
        {
            messageOffset.writeToBuffer(buffer);
        }
    }

    @Override
    public int length() {
        int length = 0;

        for(MessageOffset messageOffset : messageOffsets)
        {
            length += messageOffset.length();
        }

        return length;
    }

}
