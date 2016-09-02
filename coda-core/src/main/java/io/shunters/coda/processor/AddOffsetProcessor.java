package io.shunters.coda.processor;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.PutResponse;
import io.shunters.coda.message.*;
import io.shunters.coda.offset.OffsetHandler;
import io.shunters.coda.offset.OffsetManager;
import io.shunters.coda.offset.QueueShard;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddOffsetProcessor extends AbstractQueueThread<AddOffsetEvent> {

    private StoreProcessor storeProcessor;

    private OffsetHandler offsetHandler;

    public AddOffsetProcessor()
    {
        this.storeProcessor = new StoreProcessor();
        this.storeProcessor.start();

        offsetHandler = OffsetManager.singleton();
    }


    @Override
    public void process(AddOffsetEvent addOffsetEvent)
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

                // get current offset for the shard of the queue.
                QueueShard queueShard = new QueueShard(queue, shardId);

                // get current offset for the shard of the queue.
                long currentOffset = offsetHandler.getCurrentOffsetAndIncrease(queueShard, messageOffsetList.size());

                for(MessageOffset messageOffset : messageOffsetList)
                {
                    // increase offset.
                    currentOffset++;

                    // set message offset.
                    messageOffset.setOffset(currentOffset);

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
                storeProcessor.put(storeEvent);
            }
        }
    }
}
