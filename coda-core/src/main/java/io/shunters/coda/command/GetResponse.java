package io.shunters.coda.command;

import io.shunters.coda.message.BaseResponseHeader;
import io.shunters.coda.message.MessageList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * GetResponse := BaseReponseHeader [QueueGetResult]
 *  QueueGetResult := queue [ShardGetResult]
 *      ShardGetResult := shardId shardErrorCode MessageList
 */
public class GetResponse extends AbstractToByteBuffer{

    private BaseResponseHeader baseResponseHeader;
    private List<QueueGetResult> queueGetResults;

    public GetResponse(BaseResponseHeader baseResponseHeader, List<QueueGetResult> queueGetResults)
    {
        this.baseResponseHeader = baseResponseHeader;
        this.queueGetResults = queueGetResults;
    }

    public BaseResponseHeader getBaseResponseHeader() {
        return baseResponseHeader;
    }

    public List<QueueGetResult> getQueueGetResults() {
        return queueGetResults;
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        baseResponseHeader.writeToBuffer(buffer);

        buffer.putInt(this.queueGetResults.size()); // count.

        for(QueueGetResult queueGetResult : queueGetResults)
        {
            queueGetResult.writeToBuffer(buffer);
        }
    }

    public static GetResponse fromByteBuffer(ByteBuffer buffer)
    {
        BaseResponseHeader baseResponseHeaderTemp = BaseResponseHeader.fromByteBuffer(buffer);

        int count = buffer.getInt(); // count.

        List<QueueGetResult> queueGetResultsTemp = new ArrayList<>();
        int readCount = 0;
        while(readCount < count)
        {
            QueueGetResult queueGetResultTemp = QueueGetResult.fromByteBuffer(buffer);
            queueGetResultsTemp.add(queueGetResultTemp);

            readCount++;
        }

        return new GetResponse(baseResponseHeaderTemp, queueGetResultsTemp);
    }

    @Override
    public int length() {

        int length = 0;

        length += this.baseResponseHeader.length();
        length += 4; // count.
        for(QueueGetResult queueGetResult : queueGetResults)
        {
            length += queueGetResult.length();
        }

        return length;
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("baseResponseHeader: ").append("[" + baseResponseHeader.toString() + "]");

        int count = 0;

        int SIZE = this.queueGetResults.size();

        for(QueueGetResult queueGetResult : queueGetResults)
        {
            sb.append("queueGetResult: ").append("[" + queueGetResult.toString() + "]");

            count++;

            if(count != SIZE)
            {
                sb.append(", ");
            }
        }

        return sb.toString();
    }

    /**
     * QueueGetResult := queue [ShardGetResult]
     */
    public static class QueueGetResult implements ToByteBuffer
    {
        private String queue;
        private List<ShardGetResult> shardGetResults;

        public QueueGetResult(String queue, List<ShardGetResult> shardGetResults)
        {
            this.queue = queue;
            this.shardGetResults = shardGetResults;
        }

        public String getQueue() {
            return queue;
        }

        public List<ShardGetResult> getShardGetResults() {
            return shardGetResults;
        }

        @Override
        public void writeToBuffer(ByteBuffer buffer) {
            byte[] queueBytes = this.queue.getBytes();

            buffer.putInt(queueBytes.length); // queue length.
            buffer.put(queueBytes);

            buffer.putInt(this.shardGetResults.size()); // count.

            for(ShardGetResult shardGetResult : shardGetResults)
            {
                shardGetResult.writeToBuffer(buffer);
            }

        }

        public static QueueGetResult fromByteBuffer(ByteBuffer buffer)
        {
            int queueLengthTemp = buffer.getInt(); // queue length.
            byte[] queueBytesTemp = new byte[queueLengthTemp];
            buffer.get(queueBytesTemp);
            String queueTemp = new String(queueBytesTemp);

            int count = buffer.getInt(); // count.

            List<ShardGetResult> shardGetResultsTemp = new ArrayList<>();
            int readCount = 0;
            while(readCount < count)
            {
                ShardGetResult shardGetResultTemp = ShardGetResult.fromByteBuffer(buffer);
                shardGetResultsTemp.add(shardGetResultTemp);

                readCount++;
            }

            return new QueueGetResult(queueTemp, shardGetResultsTemp);
        }

        @Override
        public int length() {

            int length = 0;

            length += 4; // queue length;
            length += queue.getBytes().length; // queue.

            length += 4; // count.

            for(ShardGetResult shardGetResult : shardGetResults)
            {
                length += shardGetResult.length();
            }

            return length;
        }

        @Override
        public String toString()
        {
            StringBuffer sb = new StringBuffer();

            sb.append("queue: ").append(this.queue).append(", ");

            int count = 0;

            int SIZE = this.shardGetResults.size();

            for(ShardGetResult shardGetResult : shardGetResults)
            {
                sb.append("shardGetResult: ").append("[" + shardGetResult.toString() + "]");

                count++;

                if(count != SIZE)
                {
                    sb.append(", ");
                }
            }

            return sb.toString();
        }

        /**
         * ShardGetResult := shardId shardErrorCode MessageList
         */
        public static class ShardGetResult implements ToByteBuffer
        {
            private int shardId;
            private short shardErrorCode;
            private MessageList messageList;

            public ShardGetResult(int shardId, short shardErrorCode, MessageList messageList)
            {
                this.shardId = shardId;
                this.shardErrorCode = shardErrorCode;
                this.messageList = messageList;
            }

            public int getShardId() {
                return shardId;
            }

            public short getShardErrorCode() {
                return shardErrorCode;
            }

            public MessageList getMessageList() {
                return messageList;
            }

            @Override
            public void writeToBuffer(ByteBuffer buffer) {
                buffer.putInt(shardId);
                buffer.putShort(shardErrorCode);
                messageList.writeToBuffer(buffer);
            }

            public static ShardGetResult fromByteBuffer(ByteBuffer buffer)
            {
                int shardIdTemp = buffer.getInt();
                short shardErrorCodeTemp = buffer.getShort();
                MessageList messageListTemp = MessageList.fromByteBuffer(buffer);

                return new ShardGetResult(shardIdTemp, shardErrorCodeTemp, messageListTemp);
            }

            @Override
            public int length() {

                int length = 0;

                length += 4; // shardId.
                length += 2; // shardErrorCode.
                length += messageList.length(); // messageList.

                return length;
            }

            @Override
            public String toString()
            {
                StringBuffer sb = new StringBuffer();

                sb.append("shardId: ").append(this.shardId).append(", ");
                sb.append("shardErrorCode: ").append(this.shardErrorCode).append(", ");
                sb.append("messageList: ").append("[" + this.messageList.toString() + "]");

                return sb.toString();
            }
        }
    }
}
