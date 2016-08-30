package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;

/**
 * BaseResponseHeader := messageId
 */
public class BaseResponseHeader implements ToByteBuffer {

    private int messageId;


    public BaseResponseHeader(int messageId)
    {
        this.messageId = messageId;
    }


    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(messageId);
    }

    public static BaseResponseHeader fromByteBuffer(ByteBuffer buffer)
    {
        int messageIdTemp = buffer.getInt();

        return new BaseResponseHeader(messageIdTemp);
    }

    @Override
    public int length() {

        int length = 0;

        length += 4; // messageId.

        return length;
    }
}
