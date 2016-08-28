package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;

/**
 * BaseRequestHeader := totalSize commandId version messageId clientId
 */
public class BaseRequestHeader implements ToByteBuffer {
    private short commandId;
    private  short version;
    private int messageId;
    private String clientId;

    public BaseRequestHeader(short commandId, short version, int messageId, String clientId)
    {
        this.commandId = commandId;
        this.version = version;
        this.messageId = messageId;
        this.clientId = clientId;
    }



    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putShort(commandId);
        buffer.putShort(version);
        buffer.putInt(messageId);
        buffer.putInt(this.clientId.getBytes().length); // clientId length.
        buffer.put(this.clientId.getBytes());
    }

    @Override
    public int length() {
        int length = 0;

        length += 2; // commandId.
        length += 2; // version.
        length += 4; // messageId;
        length += 4; // clientId length.
        length += clientId.getBytes().length; // clientId.

        return length;
    }
}
