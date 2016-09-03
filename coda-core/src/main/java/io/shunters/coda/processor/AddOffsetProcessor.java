package io.shunters.coda.processor;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.message.BaseRequestHeader;
import io.shunters.coda.message.Message;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageOffset;
import io.shunters.coda.offset.OffsetHandler;
import io.shunters.coda.offset.OffsetManager;
import io.shunters.coda.offset.QueueShard;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddOffsetProcessor extends AbstractQueueThread<AddOffsetEvent> {

    private AddMessageListProcessor addMessageListProcessor;

    private OffsetHandler offsetHandler;

    public AddOffsetProcessor()
    {
        this.addMessageListProcessor = new AddMessageListProcessor();
        this.addMessageListProcessor.start();

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

        List<AddMessageListEvent.QueueShardMessageList> queueShardMessageLists = new ArrayList<>();

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

                // TODO: getting current offset is a bottleneck cause.
                // get current offset for the shard of the queue.
                long currentOffset = offsetHandler.getCurrentOffsetAndIncrease(queueShard, messageOffsetList.size());

                // firstOffset for this MessageList.
                long firstOffset = currentOffset + 1;
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


                // construct QueueShardMessageList of AddMessageListEvent.
                AddMessageListEvent.QueueShardMessageList queueShardMessageList = new AddMessageListEvent.QueueShardMessageList(new QueueShard(queue, shardId), firstOffset, messageList);
                queueShardMessageLists.add(queueShardMessageList);
            }
        }

        // send to AddMessageList processor.
        AddMessageListEvent storeEvent = new AddMessageListEvent(baseEvent, messageId, queueShardMessageLists);
        this.addMessageListProcessor.put(storeEvent);
    }
}
