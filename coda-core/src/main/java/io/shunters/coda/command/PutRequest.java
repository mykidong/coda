package io.shunters.coda.command;

import io.shunters.coda.message.BaseRequestHeader;
import io.shunters.coda.message.MessageList;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * PutRequest := BaseRequestHeader acks [QueueMessageWrap]
 *  QueueMessageWrap := queue [ShardMessageWrap]
 *      ShardMessageWrap := shardId length MessageList
 */
public class PutRequest extends AbstractToByteBuffer {

    private BaseRequestHeader baseRequestHeader;
    private short acks;
    private List<QueueMessageWrap> queueMessageWraps;


    public PutRequest(BaseRequestHeader baseRequestHeader,
                      short acks,
                      List<QueueMessageWrap> queueMessageWraps)
    {
        this.baseRequestHeader = baseRequestHeader;
        this.acks = acks;
        this.queueMessageWraps = queueMessageWraps;
    }


    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        this.baseRequestHeader.writeToBuffer(buffer);
        buffer.putShort(this.acks);

        for(QueueMessageWrap queueMessageWrap : queueMessageWraps)
        {
            queueMessageWrap.writeToBuffer(buffer);
        }
    }

    public static PutRequest fromByteBuffer(ByteBuffer buffer)
    {
        // TODO: do read buffer.
        return null;
    }

    @Override
    public int length() {
        int length = 0;

        length += baseRequestHeader.length(); // baseRequestHeader.
        length += 2; // acks.
        for(QueueMessageWrap queueMessageWrap : queueMessageWraps)
        {
            length += queueMessageWrap.length();
        }

        return length;
    }

    public static class QueueMessageWrap implements ToByteBuffer
    {
        private String queue;
        private List<ShardMessageWrap> shardMessageWraps;

        public QueueMessageWrap(String queue, List<ShardMessageWrap> shardMessageWraps)
        {
            this.queue = queue;
            this.shardMessageWraps = shardMessageWraps;
        }

        @Override
        public void writeToBuffer(ByteBuffer buffer) {

            byte[] queueBytes = this.queue.getBytes();

            buffer.putInt(queueBytes.length); // queue length.
            buffer.put(queueBytes);

            for(ShardMessageWrap shardMessageWrap : shardMessageWraps)
            {
                shardMessageWrap.writeToBuffer(buffer);
            }
        }

        public static QueueMessageWrap fromByteBuffer(ByteBuffer buffer)
        {
            // TODO: do read buffer.
            return null;
        }

        @Override
        public int length() {

            int length = 0;

            length += 4; // queue length;
            length += queue.getBytes().length; // queue.

            for(ShardMessageWrap shardMessageWrap : shardMessageWraps)
            {
                length += shardMessageWrap.length();
            }

            return length;
        }


        public static class ShardMessageWrap implements ToByteBuffer
        {
            private int shardId;
            private int length;
            private MessageList messageList;

            public ShardMessageWrap(int shardId, int length, MessageList messageList)
            {
                this.shardId = shardId;
                this.length = length;
                this.messageList = messageList;
            }

            @Override
            public void writeToBuffer(ByteBuffer buffer) {
                buffer.putInt(this.shardId);
                buffer.putInt(this.length);
                messageList.writeToBuffer(buffer);
            }

            public static ShardMessageWrap fromByteBuffer(ByteBuffer buffer)
            {
                // TODO: do read buffer.
                return null;
            }

            @Override
            public int length() {

                int length = 0;

                length += 4; // shardId.
                length += 4; // length.
                length += this.messageList.length(); // messageList.

                return length;
            }
        }
    }
}
