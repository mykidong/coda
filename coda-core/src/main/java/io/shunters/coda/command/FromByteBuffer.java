package io.shunters.coda.command;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-27.
 */
public interface FromByteBuffer<T> {

    public T readFromByteBuffer(ByteBuffer buffer);
}
