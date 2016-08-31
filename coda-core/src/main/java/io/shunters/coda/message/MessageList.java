package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * MessageList := [MessageOffset]
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

        buffer.putInt(this.messageOffsets.size()); // count.

        for(MessageOffset messageOffset : messageOffsets)
        {
            messageOffset.writeToBuffer(buffer);
        }
    }

    public static MessageList fromByteBuffer(ByteBuffer buffer)
    {
        int count = buffer.getInt();

        List<MessageOffset> messageOffsetsTemp = new ArrayList<>();

        int readCount = 0;

        while (readCount < count) {
            MessageOffset messageOffsetTemp = MessageOffset.fromByteBuffer(buffer);
            messageOffsetsTemp.add(messageOffsetTemp);

            readCount++;
        }

        return new MessageList(messageOffsetsTemp);
    }

    @Override
    public int length() {
        int length = 0;

        length += 4; // count.

        for(MessageOffset messageOffset : messageOffsets)
        {
            length += messageOffset.length();
        }

        return length;
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        int count = 0;

        int SIZE = messageOffsets.size();

        for(MessageOffset messageOffset : messageOffsets) {
            sb.append("messageOffset: ").append("[" + messageOffset.toString() + "]");

            count++;

            if(count != SIZE)
            {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

}
