package io.shunters.coda.message;

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

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putLong(offset);
        buffer.putInt(length);
        message.writeToBuffer(buffer);
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
