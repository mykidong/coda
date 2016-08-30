package io.shunters.coda.message;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-28.
 */
public class MessageOffset implements ToByteBuffer{

    private long offset;
    private int length;
    private Message message;

    public MessageOffset(long offset, int length, Message message)
    {
        this.offset = offset;
        this.length = length;
        this.message = message;
    }

    public long getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public Message getMessage() {
        return message;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putLong(offset);
        buffer.putInt(length);
        message.writeToBuffer(buffer);
    }

    public static MessageOffset fromByteBuffer(ByteBuffer buffer)
    {
        long offsetTemp = buffer.getLong();
        int lengthTemp = buffer.getInt();
        Message messageTemp = Message.fromByteBuffer(buffer);

        return new MessageOffset(offsetTemp, lengthTemp, messageTemp);
    }

    @Override
    public int length() {

        int length = 0;

        length += 8; // offset.
        length += 4; // length.
        length += this.message.length(); // message.

        return length;
    }
}
