package io.shunters.coda.message;

import io.shunters.coda.command.ToByteBuffer;

import java.nio.ByteBuffer;

/**
 * Message := crc formatVersion compression timestampType timestamp key value
 */
public class Message implements ToByteBuffer {

    private int crc;
    private byte formatVersion;
    private byte compression;
    private byte timestampType;
    private long timestamp;
    private byte[] key;
    private byte[] value;

    public Message(int crc, byte formatVersion, byte compression, byte timestampType, long timestamp, byte[] key, byte[] value)
    {
        this.crc = crc;
        this.formatVersion = formatVersion;
        this.compression = compression;
        this.timestampType = timestampType;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }


    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(crc);
        buffer.put(formatVersion);
        buffer.put(compression);
        buffer.put(timestampType);
        buffer.putLong(timestamp);
        buffer.putInt(key.length); // key length.
        buffer.put(key);
        buffer.putInt(value.length); // value length.
        buffer.put(value);
    }

    @Override
    public int length() {
        int length = 0;

        length += 4; // crc.
        length += 1; // compression.
        length += 1; // timestampType.
        length += 8; // timestamp.
        length += 4; // key length.
        length += this.key.length; // key.
        length += 4; // value length.
        length += this.value.length; // value.

        return length;
    }
}
