package io.shunters.coda.command;

import io.shunters.coda.message.BaseRequestHeader;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    public BaseRequestHeader getBaseRequestHeader() {
        return baseRequestHeader;
    }

    public short getAcks() {
        return acks;
    }

    public List<QueueMessageWrap> getQueueMessageWraps() {
        return queueMessageWraps;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        this.baseRequestHeader.writeToBuffer(buffer);
        buffer.putShort(this.acks);

        buffer.putInt(this.queueMessageWraps.size()); // count.

        for(QueueMessageWrap queueMessageWrap : queueMessageWraps)
        {
            queueMessageWrap.writeToBuffer(buffer);
        }
    }

    public static PutRequest fromByteBuffer(ByteBuffer buffer)
    {
        BaseRequestHeader baseRequestHeaderTemp = BaseRequestHeader.fromByteBuffer(buffer);
        short acksTemp = buffer.getShort();

        int count = buffer.getInt(); // count.

        List<QueueMessageWrap> queueMessageWrapsTemp = new ArrayList<>();

        int readCount = 0;

        while(readCount < count)
        {
            QueueMessageWrap queueMessageWrapTemp = QueueMessageWrap.fromByteBuffer(buffer);
            queueMessageWrapsTemp.add(queueMessageWrapTemp);

            readCount++;
        }

        return new PutRequest(baseRequestHeaderTemp, acksTemp, queueMessageWrapsTemp);
    }

    @Override
    public int length() {
        int length = 0;

        length += baseRequestHeader.length(); // baseRequestHeader.
        length += 2; // acks.

        length += 4; // count.

        for(QueueMessageWrap queueMessageWrap : queueMessageWraps)
        {
            length += queueMessageWrap.length();
        }

        return length;
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("baseRequestHeader: ").append("[" + baseRequestHeader.toString() + "]");
        sb.append("acks: ").append(this.acks).append(", ");

        int count = 0;

        int SIZE = this.queueMessageWraps.size();

        for(QueueMessageWrap queueMessageWrap : queueMessageWraps) {
            sb.append("queueMessageWrap: ").append("[" + queueMessageWrap.toString() + "]");

            count++;

            if(count != SIZE)
            {
                sb.append(", ");
            }
        }

        return sb.toString();
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

        public String getQueue() {
            return queue;
        }

        public List<ShardMessageWrap> getShardMessageWraps() {
            return shardMessageWraps;
        }

        @Override
        public void writeToBuffer(ByteBuffer buffer) {

            byte[] queueBytes = this.queue.getBytes();

            buffer.putInt(queueBytes.length); // queue length.
            buffer.put(queueBytes);

            buffer.putInt(shardMessageWraps.size()); // count.

            for(ShardMessageWrap shardMessageWrap : shardMessageWraps)
            {
                shardMessageWrap.writeToBuffer(buffer);
            }
        }

        public static QueueMessageWrap fromByteBuffer(ByteBuffer buffer)
        {
            int queueLengthTemp = buffer.getInt(); // queue length.
            byte[] queueBytesTemp = new byte[queueLengthTemp];
            buffer.get(queueBytesTemp);
            String queueTemp = new String(queueBytesTemp);

            int count = buffer.getInt(); // count.

            List<ShardMessageWrap> shardMessageWrapsTemp = new ArrayList<>();

            int readCount = 0;

            while(readCount < count)
            {
                ShardMessageWrap shardMessageWrapTemp = ShardMessageWrap.fromByteBuffer(buffer);
                shardMessageWrapsTemp.add(shardMessageWrapTemp);

                readCount++;
            }

            QueueMessageWrap queueMessageWrap = new QueueMessageWrap(queueTemp, shardMessageWrapsTemp);

            return queueMessageWrap;
        }

        @Override
        public int length() {

            int length = 0;

            length += 4; // queue length;
            length += queue.getBytes().length; // queue.

            length += 4; // count.

            for(ShardMessageWrap shardMessageWrap : shardMessageWraps)
            {
                length += shardMessageWrap.length();
            }

            return length;
        }

        @Override
        public String toString()
        {
            StringBuffer sb = new StringBuffer();

            sb.append("queue: ").append(this.queue).append(", ");

            int count = 0;

            int SIZE = this.shardMessageWraps.size();

            for(ShardMessageWrap shardMessageWrap : shardMessageWraps) {
                sb.append("shardMessageWrap: ").append("[" + shardMessageWrap.toString() + "]");

                count++;

                if(count != SIZE)
                {
                    sb.append(", ");
                }
            }

            return sb.toString();
        }


        public static class ShardMessageWrap implements ToByteBuffer
        {
            private int shardId;
            private MessageList messageList;

            public ShardMessageWrap(int shardId, MessageList messageList)
            {
                this.shardId = shardId;
                this.messageList = messageList;
            }

            public int getShardId() {
                return shardId;
            }

            public MessageList getMessageList() {
                return messageList;
            }

            @Override
            public void writeToBuffer(ByteBuffer buffer) {
                buffer.putInt(this.shardId);
                messageList.writeToBuffer(buffer);
            }

            public static ShardMessageWrap fromByteBuffer(ByteBuffer buffer)
            {
                int shardIdTemp = buffer.getInt();
                MessageList messageListTemp = MessageList.fromByteBuffer(buffer);

                return new ShardMessageWrap(shardIdTemp, messageListTemp);
            }

            @Override
            public int length() {

                int length = 0;

                length += 4; // shardId.
                length += 4; // length.
                length += this.messageList.length(); // messageList.

                return length;
            }

            @Override
            public String toString()
            {
                StringBuffer sb = new StringBuffer();

                sb.append("shardId: ").append(this.shardId).append(", ");
                sb.append("messageList: ").append("[" + this.messageList.toString() + "]");

                return sb.toString();
            }
        }
    }
}
