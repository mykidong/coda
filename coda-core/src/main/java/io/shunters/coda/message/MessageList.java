package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * MessageList := [offset length Message]
 */
public class MessageList implements ToByteBuffer{

    private List<MessageOffset> messageOffsets;

    public MessageList(List<MessageOffset> messageOffsets)
    {
        this.messageOffsets = messageOffsets;
    }


    public List<MessageOffset> getMessageOffsets() {
        return messageOffsets;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        for(MessageOffset messageOffset : messageOffsets)
        {
            messageOffset.writeToBuffer(buffer);
        }
    }

    public static MessageList fromByteBuffer(ByteBuffer buffer)
    {
        List<MessageOffset> messageOffsetsTemp = new ArrayList<>();

        while (buffer.hasRemaining()) {
            MessageOffset messageOffsetTemp = MessageOffset.fromByteBuffer(buffer);
            messageOffsetsTemp.add(messageOffsetTemp);
        }

        return new MessageList(messageOffsetsTemp);
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
