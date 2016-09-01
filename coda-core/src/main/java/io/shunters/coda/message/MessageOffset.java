package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;

/**
 * MessageOffset := offset Message
 */
public class MessageOffset implements ToByteBuffer{

    private long offset;
    private Message message;

    public MessageOffset(long offset, Message message)
    {
        this.offset = offset;
        this.message = message;
    }

    public long getOffset() {
        return offset;
    }

    public Message getMessage() {
        return message;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putLong(offset);
        message.writeToBuffer(buffer);
    }

    public static MessageOffset fromByteBuffer(ByteBuffer buffer)
    {
        long offsetTemp = buffer.getLong();
        Message messageTemp = Message.fromByteBuffer(buffer);

        return new MessageOffset(offsetTemp, messageTemp);
    }

    @Override
    public int length() {

        int length = 0;

        length += 8; // offset.
        length += this.message.length(); // message.

        return length;
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("offset: ").append(this.offset).append(", ");
        sb.append("message: ").append("[" + this.message.toString() + "]");

        return sb.toString();
    }
}
