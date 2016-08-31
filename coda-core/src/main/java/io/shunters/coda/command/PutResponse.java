package io.shunters.coda.command;

import io.shunters.coda.message.BaseResponseHeader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * PutResponse := BaseReponseHeader [QueuePutResult]
 *  QueuePutResult := queue [ShardPutResult]
 *      ShardPutResult := shardId shardErrorCode offset timestamp
 */
public class PutResponse extends AbstractToByteBuffer{

    private BaseResponseHeader baseResponseHeader;
    private List<QueuePutResult> queuePutResults;

    public PutResponse(BaseResponseHeader baseResponseHeader, List<QueuePutResult> queuePutResults)
    {
        this.baseResponseHeader = baseResponseHeader;
        this.queuePutResults = queuePutResults;
    }

    public BaseResponseHeader getBaseResponseHeader() {
        return baseResponseHeader;
    }

    public List<QueuePutResult> getQueuePutResults() {
        return queuePutResults;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {

        baseResponseHeader.writeToBuffer(buffer);

        buffer.putInt(getQueuePutResults().size()); // count.

        for(QueuePutResult queuePutResult : queuePutResults)
        {
            queuePutResult.writeToBuffer(buffer);
        }

    }

    public static PutResponse fromByteBuffer(ByteBuffer buffer)
    {
        BaseResponseHeader baseResponseHeaderTemp = BaseResponseHeader.fromByteBuffer(buffer);

        int count = buffer.getInt(); // count;

        int readCount = 0;

        List<QueuePutResult> queuePutResultsTemp = new ArrayList<>();
        while(readCount < count)
        {
            QueuePutResult queuePutResultTemp = QueuePutResult.fromByteBuffer(buffer);
            queuePutResultsTemp.add(queuePutResultTemp);

            readCount++;
        }

        return new PutResponse(baseResponseHeaderTemp, queuePutResultsTemp);
    }

    @Override
    public int length() {

        int length = 0;

        length += this.baseResponseHeader.length();
        length += 4; // count.
        for(QueuePutResult queuePutResult : queuePutResults)
        {
            length += queuePutResult.length();
        }

        return length;
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("baseResponseHeader: ").append("[" + baseResponseHeader.toString() + "]");

        int count = 0;

        int SIZE = this.queuePutResults.size();

        for(QueuePutResult queuePutResult : queuePutResults) {
            sb.append("queuePutResult: ").append("[" + queuePutResult.toString() + "]");

            count++;

            if(count != SIZE)
            {
                sb.append(", ");
            }
        }

        return sb.toString();
    }


    public static class QueuePutResult implements ToByteBuffer
    {
        private String queue;
        private List<ShardPutResult> shardPutResults;

        public QueuePutResult(String queue, List<ShardPutResult> shardPutResults)
        {
            this.queue = queue;
            this.shardPutResults = shardPutResults;
        }

        public String getQueue() {
            return queue;
        }

        public List<ShardPutResult> getShardPutResults() {
            return shardPutResults;
        }

        @Override
        public void writeToBuffer(ByteBuffer buffer) {
            byte[] queueBytes = this.queue.getBytes();

            buffer.putInt(queueBytes.length); // queue length.
            buffer.put(queueBytes);

            buffer.putInt(this.shardPutResults.size()); // count.

            for(ShardPutResult shardPutResult : shardPutResults)
            {
                shardPutResult.writeToBuffer(buffer);
            }
        }

        public static QueuePutResult fromByteBuffer(ByteBuffer buffer)
        {
            int queueLengthTemp = buffer.getInt(); // queue length.
            byte[] queueBytesTemp = new byte[queueLengthTemp];
            buffer.get(queueBytesTemp);
            String queueTemp = new String(queueBytesTemp);

            int count = buffer.getInt(); // count.

            List<ShardPutResult> shardPutResultsTemp = new ArrayList<>();

            int readCount = 0;
            while(readCount < count)
            {
                ShardPutResult shardPutResultTemp = ShardPutResult.fromByteBuffer(buffer);
                shardPutResultsTemp.add(shardPutResultTemp);

                readCount++;
            }

            return new QueuePutResult(queueTemp, shardPutResultsTemp);
        }

        @Override
        public int length() {

            int length = 0;

            length += 4; // queue length;
            length += queue.getBytes().length; // queue.

            length += 4; // count.

            for(ShardPutResult shardPutResult : shardPutResults)
            {
                length += shardPutResult.length();
            }

            return length;
        }

        @Override
        public String toString()
        {
            StringBuffer sb = new StringBuffer();

            sb.append("queue: ").append(this.queue).append(", ");

            int count = 0;

            int SIZE = this.shardPutResults.size();

            for(ShardPutResult shardPutResult : shardPutResults) {
                sb.append("shardPutResult: ").append("[" + shardPutResult.toString() + "]");

                count++;

                if(count != SIZE)
                {
                    sb.append(", ");
                }
            }

            return sb.toString();
        }

        public static class ShardPutResult implements ToByteBuffer
        {
            private int shardId;
            private short shardErrorCode;
            private long offset;
            private long timestamp;

            public ShardPutResult(int shardId, short shardErrorCode, long offset, long timestamp)
            {
                this.shardId = shardId;
                this.shardErrorCode = shardErrorCode;
                this.offset = offset;
                this.timestamp = timestamp;
            }

            public int getShardId() {
                return shardId;
            }

            public short getShardErrorCode() {
                return shardErrorCode;
            }

            public long getOffset() {
                return offset;
            }

            public long getTimestamp() {
                return timestamp;
            }

            @Override
            public void writeToBuffer(ByteBuffer buffer) {
                buffer.putInt(shardId);
                buffer.putShort(shardErrorCode);
                buffer.putLong(offset);
                buffer.putLong(timestamp);
            }

            public static ShardPutResult fromByteBuffer(ByteBuffer buffer)
            {
                int shardIdTemp = buffer.getInt();
                short shardErrorCodeTemp = buffer.getShort();
                long offsetTemp = buffer.getLong();
                long timestampTemp = buffer.getLong();

                return new ShardPutResult(shardIdTemp, shardErrorCodeTemp, offsetTemp, timestampTemp);
            }


            @Override
            public int length() {

                int length = 0;

                length += 4; // shardId.
                length += 2; // shardErrorCode.
                length += 8; // offset.
                length += 8; // timestamp.

                return length;
            }

            @Override
            public String toString()
            {
                StringBuffer sb = new StringBuffer();

                sb.append("shardId: ").append(this.shardId).append(", ");
                sb.append("shardErrorCode: ").append(this.shardErrorCode).append(", ");
                sb.append("offset: ").append(this.offset).append(", ");
                sb.append("timestamp: ").append(this.timestamp);

                return sb.toString();
            }
        }
    }
}
