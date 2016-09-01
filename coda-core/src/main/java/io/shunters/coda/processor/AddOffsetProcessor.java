package io.shunters.coda.processor;

import io.shunters.coda.command.PutRequest;
import io.shunters.coda.message.BaseRequestHeader;
import io.shunters.coda.message.Message;
import io.shunters.coda.message.MessageList;
import io.shunters.coda.message.MessageOffset;
import io.shunters.coda.offset.OffsetHandler;
import io.shunters.coda.offset.QueueShard;

import java.util.List;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddOffsetProcessor extends AbstractQueueThread {

    private static AddOffsetProcessor addOffsetProcessor;

    private final static Object lock = new Object();

    private OffsetHandler offsetHandler;

    private StoreProcessor storeProcessor;

    public static AddOffsetProcessor singleton(OffsetHandler offsetHandler)
    {
        if(addOffsetProcessor == null)
        {
            synchronized (lock)
            {
                if(addOffsetProcessor == null)
                {
                    addOffsetProcessor = new AddOffsetProcessor(offsetHandler);
                    addOffsetProcessor.start();
                }
            }
        }

        return addOffsetProcessor;
    }

    private AddOffsetProcessor(OffsetHandler offsetHandler)
    {
        this.offsetHandler = offsetHandler;

        this.storeProcessor = StoreProcessor.singleton();
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
                    // get current offset for the shard of the queue.
                    QueueShard queueShard = new QueueShard(queue, shardId);
                    long currentOffset = offsetHandler.getCurrentOffset(queueShard);

                    // increase offset.
                    currentOffset++;

                    // set message offset for the shard of the queue.
                    messageOffset.setOffset(currentOffset);

                    // update offset for the shard of the queue.
                    offsetHandler.updateOffset(queueShard, currentOffset);


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

                // send to store processor.
                storeProcessor.put(storeEvent);
            }
        }

    }
}
