package io.shunters.coda.processor;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.message.*;
import io.shunters.coda.offset.OffsetHandler;
import io.shunters.coda.offset.QueueShard;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddOffsetProcessor extends AbstractQueueThread {

    private OffsetHandler offsetHandler;

    private StoreProcessor storeProcessor;

    public AddOffsetProcessor(OffsetHandler offsetHandler)
    {
        this.offsetHandler = offsetHandler;

        this.storeProcessor = new StoreProcessor();
        this.storeProcessor.start();
    }


    @Override
    public void run() {
        try {
            while (true) {
                Object obj = this.queue.take();

                AddOffsetEvent addOffsetEvent = (AddOffsetEvent) obj;
                process(addOffsetEvent);
            }
        }catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void process(AddOffsetEvent addOffsetEvent)
    {
        BaseEvent baseEvent = addOffsetEvent.getBaseEvent();
        PutRequest putRequest = addOffsetEvent.getPutRequest();

        // TODO: replication  implementation according to the acking mode.
        short acks = putRequest.getAcks();

        BaseRequestHeader baseRequestHeader = putRequest.getBaseRequestHeader();

        int messageId = baseRequestHeader.getMessageId();

        List<PutRequest.QueueMessageWrap> queueMessageWrapList = putRequest.getQueueMessageWraps();
        for(PutRequest.QueueMessageWrap queueMessageWrap : queueMessageWrapList)
        {
            String queue = queueMessageWrap.getQueue();

            List<PutRequest.QueueMessageWrap.ShardMessageWrap> shardMessageWrapList = queueMessageWrap.getShardMessageWraps();
            for(PutRequest.QueueMessageWrap.ShardMessageWrap shardMessageWrap : shardMessageWrapList)
            {
                int shardId = shardMessageWrap.getShardId();

                MessageList messageList = shardMessageWrap.getMessageList();

                List<MessageOffset> messageOffsetList = messageList.getMessageOffsets();
                for(MessageOffset messageOffset : messageOffsetList)
                {
//                    // get current offset for the shard of the queue.
//                    QueueShard queueShard = new QueueShard(queue, shardId);
//                    long currentOffset = offsetHandler.getCurrentOffset(queueShard);
//
//                    // increase offset.
//                    currentOffset++;
//
//                    // set message offset for the shard of the queue.
//                    messageOffset.setOffset(currentOffset);
//
//                    // update offset for the shard of the queue.
//                    offsetHandler.updateOffset(queueShard, currentOffset);


                    Message message = messageOffset.getMessage();

                    // message compression.
                    byte compression = message.getCompression();

                    // TODO:
                    // if message is compressioned, message value bytes is compressed MessageList
                    // which must be decompressed and splitted into individual Messages.
                    byte[] value = message.getValue();
                }

                // construct StoreEvent.
                StoreEvent storeEvent = new StoreEvent(baseEvent, messageId, new QueueShard(queue, shardId), messageList);


                // TODO: selector does not read channels correctly.
                // send to store processor.
                //storeProcessor.put(storeEvent);
            }
        }



        // IT IS JUST TEST PURPOSE.
        PutResponse putResponse = buildPutResponse();

        ByteBuffer responseBuffer = putResponse.write();

        String channelId = baseEvent.getChannelId();
        NioSelector nioSelector = baseEvent.getNioSelector();

        // attache response to channel with SelectionKey.OP_WRITE, which causes channel processor to send response to the client.
        nioSelector.attach(channelId, SelectionKey.OP_WRITE, responseBuffer);

        // wakeup must be called.
        nioSelector.wakeup();
    }


    public static BaseResponseHeader buildInstance()
    {
        int messageId = 234584;

        BaseResponseHeader baseResponseHeader = new BaseResponseHeader(messageId);

        return baseResponseHeader;
    }

    public static PutResponse.QueuePutResult.ShardPutResult buildShardPutResult()
    {
        int shardId = 1;
        short shardErrorCode = 0;
        long offset = 33424;
        long timestamp = new Date().getTime();

        return new PutResponse.QueuePutResult.ShardPutResult(shardId, shardErrorCode, offset, timestamp);
    }

    public static PutResponse.QueuePutResult buildQueuePutResult()
    {
        String queue = "some-queue-name";

        List<PutResponse.QueuePutResult.ShardPutResult> shardPutResults = new ArrayList<>();
        for(int i = 0; i < 3; i++)
        {
            PutResponse.QueuePutResult.ShardPutResult shardPutResult = buildShardPutResult();
            shardPutResults.add(shardPutResult);
        }

        return new PutResponse.QueuePutResult(queue, shardPutResults);
    }

    public static PutResponse buildPutResponse()
    {
        BaseResponseHeader baseResponseHeader = buildInstance();

        List<PutResponse.QueuePutResult> queuePutResults = new ArrayList<>();

        for(int i = 0; i < 2; i++)
        {
            PutResponse.QueuePutResult queuePutResult = buildQueuePutResult();
            queuePutResults.add(queuePutResult);
        }

        return new PutResponse(baseResponseHeader, queuePutResults);
    }
}
