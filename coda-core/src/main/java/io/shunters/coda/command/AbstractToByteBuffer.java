package io.shunters.coda.command;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-28.
 */
public abstract class AbstractToByteBuffer implements ToByteBuffer {

    public abstract void writeToBuffer(ByteBuffer buffer);


    public abstract int length();


    public ByteBuffer write()
    {
        int totalSize = this.length();

        // buffer allocation with capacity of the request bytes and totalSize length(4 bytes length).
        ByteBuffer buffer = ByteBuffer.allocate(totalSize + 4);
        buffer.putInt(totalSize); // totalSize.

        this.writeToBuffer(buffer);

        return buffer;
    }
}
