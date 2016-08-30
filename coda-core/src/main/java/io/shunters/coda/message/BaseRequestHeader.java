package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;

/**
 * BaseRequestHeader := commandId version messageId clientId
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

    public short getCommandId() {
        return commandId;
    }

    public short getVersion() {
        return version;
    }

    public int getMessageId() {
        return messageId;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putShort(commandId);
        buffer.putShort(version);
        buffer.putInt(messageId);
        buffer.putInt(this.clientId.getBytes().length); // clientId length.
        buffer.put(this.clientId.getBytes());
    }

    public static BaseRequestHeader fromByteBuffer(ByteBuffer buffer)
    {
        short commandIdTemp = buffer.getShort();
        short versionTemp = buffer.getShort();
        int messageIdTemp = buffer.getInt();

        int clientIdLength = buffer.getInt(); // clientId length.
        byte[] clientIdBytes = new byte[clientIdLength];
        buffer.get(clientIdBytes);

        String clientId = new String(clientIdBytes);

        return new BaseRequestHeader(commandIdTemp, versionTemp, messageIdTemp, clientId);
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

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("commandId: ").append(this.commandId).append(", ");
        sb.append("version: ").append(this.version).append(", ");
        sb.append("messageId: ").append(this.messageId).append(", ");
        sb.append("clientId: ").append(clientId);

        return sb.toString();
    }
}
