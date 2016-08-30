package io.shunters.coda.command;

import io.shunters.coda.NioSelector;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-29.
 */
public class RequestByteBuffer {

    private NioSelector nioSelector;

    private String channelId;

    private int totalSize;

    private short commandId;

    private ByteBuffer buffer;


    public RequestByteBuffer(NioSelector nioSelector, String channelId, int totalSize, short commandId, ByteBuffer buffer)
    {
        this.nioSelector = nioSelector;
        this.channelId = channelId;
        this.totalSize = totalSize;
        this.commandId = commandId;
        this.buffer = buffer;
    }

    public NioSelector getNioSelector() {
        return nioSelector;
    }

    public String getChannelId() {
        return channelId;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public short getCommandId() {
        return commandId;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
