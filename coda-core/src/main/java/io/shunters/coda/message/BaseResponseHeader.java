package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-28.
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

    @Override
    public int length() {

        int length = 0;

        length += 4; // messageId.

        return length;
    }
}
