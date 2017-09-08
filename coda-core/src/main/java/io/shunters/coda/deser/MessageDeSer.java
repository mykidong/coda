package io.shunters.coda.deser;

import io.shunters.coda.protocol.ClientServerSpec;
import org.apache.avro.generic.GenericRecord;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2017-09-01.
 */
public class MessageDeSer {

    private AvroDeSer avroDeSer;

    private static MessageDeSer messageDeSer;

    private static final Object lock = new Object();

    public static MessageDeSer singleton() {
        if (messageDeSer == null) {
            synchronized (lock) {
                if (messageDeSer == null) {
                    messageDeSer = new MessageDeSer();
                }
            }
        }
        return messageDeSer;
    }


    private MessageDeSer()
    {
        avroDeSer = AvroDeSer.getAvroDeSerSingleton();
    }


    public byte[] serializeRequest(short apiKey, short apiVersion, byte compressionCodec, GenericRecord genericRecord)
    {
        ByteBufferAndSize bufferAndSize = serializeRequestToByteBuffer(apiKey, apiVersion, compressionCodec, genericRecord);
        byte[] bytes = new byte[bufferAndSize.getSize()];
        bufferAndSize.getByteBuffer().get(bytes);

        return bytes;
    }

    public ByteBufferAndSize serializeRequestToByteBuffer(short apiKey, short apiVersion, byte compressionCodec, GenericRecord genericRecord)
    {
        try {
            // serialize avro.
            byte[] recordBytes = avroDeSer.serialize(genericRecord);

            if (compressionCodec == ClientServerSpec.COMPRESSION_CODEC_SNAPPY) {
                // snappy compressed avro bytes.
                recordBytes = Snappy.compress(recordBytes);
            }

            // total message size.
            int totalSize = (2 + 2 + 1 + 1) + recordBytes.length;

            ByteBuffer buffer = ByteBuffer.allocate(4 + totalSize);
            buffer.putInt(totalSize); // total size.
            buffer.putShort(apiKey); // api key.
            buffer.putShort(apiVersion); // api version.
            buffer.put(ClientServerSpec.MESSAGE_FORMAT_AVRO); // message format.
            buffer.put(compressionCodec);
            buffer.put(recordBytes); // produce request avro bytes.

            buffer.rewind();

            return new ByteBufferAndSize(buffer, 4 + totalSize);
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }



    public byte[] serializeResponse(byte compressionCodec, GenericRecord genericRecord)
    {
        ByteBufferAndSize bufferAndSize = serializeResponseToByteBuffer(compressionCodec, genericRecord);
        byte[] bytes = new byte[bufferAndSize.getSize()];
        bufferAndSize.getByteBuffer().get(bytes);

        return bytes;
    }

    public ByteBufferAndSize serializeResponseToByteBuffer(byte compressionCodec, GenericRecord genericRecord)
    {
        try {
            // serialize avro.
            byte[] recordBytes = avroDeSer.serialize(genericRecord);

            if (compressionCodec == ClientServerSpec.COMPRESSION_CODEC_SNAPPY) {
                // snappy compressed avro bytes.
                recordBytes = Snappy.compress(recordBytes);
            }

            // total message size.
            int totalSize = (1 + 1) + recordBytes.length;

            ByteBuffer buffer = ByteBuffer.allocate(4 + totalSize);
            buffer.putInt(totalSize); // total size.
            buffer.put(ClientServerSpec.MESSAGE_FORMAT_AVRO); // message format.
            buffer.put(compressionCodec);
            buffer.put(recordBytes); // produce request avro bytes.

            buffer.rewind();

            return new ByteBufferAndSize(buffer, 4 + totalSize);
        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public GenericRecord deserializeResponse(String schemaName, int totalSize, ByteBuffer buffer)
    {
        try {
            byte messageFormat = buffer.get();

            byte compressionCodec = buffer.get();

            int responseMessageSize = totalSize - (1 + 1);
            byte[] responseMessageBytes = new byte[responseMessageSize];
            buffer.get(responseMessageBytes);

            if (compressionCodec == ClientServerSpec.COMPRESSION_CODEC_SNAPPY) {
                responseMessageBytes = Snappy.uncompress(responseMessageBytes);
            }

            return avroDeSer.deserialize(schemaName, responseMessageBytes);

        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }

    }

    public static class ByteBufferAndSize
    {
        private ByteBuffer byteBuffer;
        private int size;

        public ByteBufferAndSize(ByteBuffer byteBuffer, int size)
        {
            this.byteBuffer = byteBuffer;
            this.size = size;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public int getSize() {
            return size;
        }
    }
}
