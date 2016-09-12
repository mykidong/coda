package io.shunters.coda.command;

import io.shunters.coda.message.BaseRequestHeader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * GetRequest := BaseRequestHeader nodeId [QueueGet]
 *  QueueGet := queue [ShardGet]
 *      ShardGet := shardId offset maxBytes
 */
public class GetRequest extends AbstractToByteBuffer {

    private BaseRequestHeader baseRequestHeader;
    private int nodeId;
    private List<QueueGet> queueGets;

    public GetRequest(BaseRequestHeader baseRequestHeader, int nodeId, List<QueueGet> queueGets)
    {
        this.baseRequestHeader = baseRequestHeader;
        this.nodeId = nodeId;
        this.queueGets = queueGets;
    }

    public BaseRequestHeader getBaseRequestHeader() {
        return baseRequestHeader;
    }

    public int getNodeId() {
        return nodeId;
    }

    public List<QueueGet> getQueueGets() {
        return queueGets;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        baseRequestHeader.writeToBuffer(buffer);
        buffer.putInt(nodeId);

        buffer.putInt(this.queueGets.size()); // count.

        for(QueueGet queueGet : queueGets)
        {
            queueGet.writeToBuffer(buffer);
        }

    }

    public static GetRequest fromByteBuffer(ByteBuffer buffer)
    {
        BaseRequestHeader baseRequestHeaderTemp = BaseRequestHeader.fromByteBuffer(buffer);
        int nodeIdTemp = buffer.getInt();

        int count = buffer.getInt();

        List<QueueGet> queueGetsTemp = new ArrayList<>();
        int readCount = 0;
        while(readCount < count)
        {
            QueueGet queueGetTemp = QueueGet.fromByteBuffer(buffer);
            queueGetsTemp.add(queueGetTemp);

            readCount++;
        }

        return new GetRequest(baseRequestHeaderTemp, nodeIdTemp, queueGetsTemp);
    }

    @Override
    public int length() {
        int length = 0;

        length += baseRequestHeader.length(); // baseRequestHeader.
        length += 4; // nodeId.

        length += 4; // count.

        for(QueueGet queueGet : queueGets)
        {
            length += queueGet.length();
        }

        return length;
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("baseRequestHeader: ").append("[" + baseRequestHeader.toString() + "]");
        sb.append("nodeId: ").append(this.nodeId).append(", ");

        int count = 0;

        int SIZE = this.queueGets.size();

        for(QueueGet queueGet : queueGets) {
            sb.append("queueGet: ").append("[" + queueGet.toString() + "]");

            count++;

            if(count != SIZE)
            {
                sb.append(", ");
            }
        }

        return sb.toString();
    }

    public static class QueueGet implements ToByteBuffer
    {
        private String queue;
        private List<ShardGet> shardGets;

        public QueueGet(String queue, List<ShardGet> shardGets)
        {
            this.queue = queue;
            this.shardGets = shardGets;
        }

        public String getQueue() {
            return queue;
        }

        public List<ShardGet> getShardGets() {
            return shardGets;
        }

        @Override
        public void writeToBuffer(ByteBuffer buffer) {
            byte[] queueBytes = this.queue.getBytes();

            buffer.putInt(queueBytes.length); // queue length.
            buffer.put(queueBytes);

            buffer.putInt(this.shardGets.size()); // count.

            for(ShardGet shardGet : shardGets)
            {
                shardGet.writeToBuffer(buffer);
            }
        }

        public static QueueGet fromByteBuffer(ByteBuffer buffer)
        {
            int queueLengthTemp = buffer.getInt(); // queue length.
            byte[] queueBytesTemp = new byte[queueLengthTemp];
            buffer.get(queueBytesTemp);
            String queueTemp = new String(queueBytesTemp);

            int count = buffer.getInt(); // count.

            List<ShardGet> shardGetsTemp = new ArrayList<>();
            int readCount = 0;
            while(readCount < count)
            {
                ShardGet shardGetTemp = ShardGet.fromByteBuffer(buffer);
                shardGetsTemp.add(shardGetTemp);

                readCount++;
            }

            return new QueueGet(queueTemp, shardGetsTemp);
        }

        @Override
        public int length() {
            int length = 0;

            length += 4; // queue length;
            length += queue.getBytes().length; // queue.

            length += 4; // count.

            for(ShardGet shardGet : shardGets)
            {
                length += shardGet.length();
            }

            return length;
        }

        @Override
        public String toString()
        {
            StringBuffer sb = new StringBuffer();

            sb.append("queue: ").append(this.queue).append(", ");

            int count = 0;

            int SIZE = this.shardGets.size();

            for(ShardGet shardGet : shardGets) {
                sb.append("shardGet: ").append("[" + shardGet.toString() + "]");

                count++;

                if(count != SIZE)
                {
                    sb.append(", ");
                }
            }

            return sb.toString();
        }

        public static class ShardGet implements ToByteBuffer
        {
            private int shardId;
            private long offset;
            private int maxBytes;

            public ShardGet(int shardId, long offset, int maxBytes)
            {
                this.shardId = shardId;
                this.offset = offset;
                this.maxBytes = maxBytes;
            }

            public int getShardId() {
                return shardId;
            }

            public long getOffset() {
                return offset;
            }

            public int getMaxBytes() {
                return maxBytes;
            }

            @Override
            public void writeToBuffer(ByteBuffer buffer) {
                buffer.putInt(shardId);
                buffer.putLong(offset);
                buffer.putInt(maxBytes);
            }

            public static ShardGet fromByteBuffer(ByteBuffer buffer)
            {
                int shardIdTemp = buffer.getInt();
                long offsetTemp = buffer.getLong();
                int maxBytesTemp = buffer.getInt();

                return new ShardGet(shardIdTemp, offsetTemp, maxBytesTemp);
            }

            @Override
            public int length() {

                int length = 0;

                length += 4; // shardId.
                length += 8; // offset.
                length += 4; // maxBytes.

                return length;
            }

            @Override
            public String toString()
            {
                StringBuffer sb = new StringBuffer();

                sb.append("shardId: ").append(this.shardId).append(", ");
                sb.append("offset: ").append(this.offset).append(", ");
                sb.append("maxBytes: ").append(this.maxBytes);

                return sb.toString();
            }
        }
    }
}
