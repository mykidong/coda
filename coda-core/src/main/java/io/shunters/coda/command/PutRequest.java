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

        for(QueueMessageWrap queueMessageWrap : queueMessageWraps)
        {
            queueMessageWrap.writeToBuffer(buffer);
        }
    }

    public static PutRequest fromByteBuffer(ByteBuffer buffer, int maxLength)
    {
        BaseRequestHeader baseRequestHeaderTemp = BaseRequestHeader.fromByteBuffer(buffer);
        short acksTemp = buffer.getShort();

        // max length descreased.
        maxLength -= baseRequestHeaderTemp.length(); // baseRequestHeader length.
        maxLength -= 2; // acks.

        List<QueueMessageWrap> queueMessageWrapsTemp = new ArrayList<>();

        int readLength = 0;
        while(buffer.hasRemaining())
        {
            // TODO: determine the max length.
            QueueMessageWrap queueMessageWrapTemp = QueueMessageWrap.fromByteBuffer(buffer, maxLength);

            queueMessageWrapsTemp.add(queueMessageWrapTemp);

            readLength += queueMessageWrapTemp.length();
            if(readLength == maxLength)
            {
                break;
            }
        }

        return new PutRequest(baseRequestHeaderTemp, acksTemp, queueMessageWrapsTemp);
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

            for(ShardMessageWrap shardMessageWrap : shardMessageWraps)
            {
                shardMessageWrap.writeToBuffer(buffer);
            }
        }

        public static QueueMessageWrap fromByteBuffer(ByteBuffer buffer, int maxLength)
        {
            int queueLengthTemp = buffer.getInt(); // queue length.
            byte[] queueBytesTemp = new byte[queueLengthTemp];
            buffer.get(queueBytesTemp);
            String queueTemp = new String(queueBytesTemp);

            List<ShardMessageWrap> shardMessageWrapsTemp = new ArrayList<>();

            // max length descreased.
            maxLength -= 4; // queue length.
            maxLength -= queueLengthTemp; // queue.


            int readLength = 0;

            while(buffer.hasRemaining())
            {
                ShardMessageWrap shardMessageWrapTemp = ShardMessageWrap.fromByteBuffer(buffer);
                shardMessageWrapsTemp.add(shardMessageWrapTemp);

                readLength += shardMessageWrapTemp.length();
                if(readLength == maxLength)
                {
                    break;
                }
            }

            QueueMessageWrap queueMessageWrap = new QueueMessageWrap(queueTemp, shardMessageWrapsTemp);

            return queueMessageWrap;
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
            private int length;
            private MessageList messageList;

            public ShardMessageWrap(int shardId, int length, MessageList messageList)
            {
                this.shardId = shardId;
                this.length = length;
                this.messageList = messageList;
            }

            public int getShardId() {
                return shardId;
            }

            public int getLength() {
                return length;
            }

            public MessageList getMessageList() {
                return messageList;
            }

            @Override
            public void writeToBuffer(ByteBuffer buffer) {
                buffer.putInt(this.shardId);
                buffer.putInt(this.length);
                messageList.writeToBuffer(buffer);
            }

            public static ShardMessageWrap fromByteBuffer(ByteBuffer buffer)
            {
                int shardIdTemp = buffer.getInt();
                int lengthTemp = buffer.getInt();
                MessageList messageListTemp = MessageList.fromByteBuffer(buffer, lengthTemp);

                return new ShardMessageWrap(shardIdTemp, lengthTemp, messageListTemp);
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
                sb.append("length: ").append(this.length).append(", ");
                sb.append("messageList: ").append("[" + this.messageList.toString() + "]");

                return sb.toString();
            }
        }
    }
}
