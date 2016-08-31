package io.shunters.coda.command;

import io.shunters.coda.NioSelector;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-08-29.
 */
public class RequestByteBuffer {

    private NioSelector nioSelector;

    private String channelId;

    private short commandId;

    private ByteBuffer buffer;


    public RequestByteBuffer(NioSelector nioSelector, String channelId, short commandId, ByteBuffer buffer)
    {
        this.nioSelector = nioSelector;
        this.channelId = channelId;
        this.commandId = commandId;
        this.buffer = buffer;
    }

    public NioSelector getNioSelector() {
        return nioSelector;
    }

    public String getChannelId() {
        return channelId;
    }

    public short getCommandId() {
        return commandId;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
