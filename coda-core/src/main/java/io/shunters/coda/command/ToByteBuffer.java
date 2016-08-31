package io.shunters.coda.command;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-27.
 */
public interface ToByteBuffer {

    public void writeToBuffer(ByteBuffer buffer);

    public int length();
}

