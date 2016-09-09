package io.shunters.coda.store;

import io.shunters.coda.message.MessageList;
import io.shunters.coda.offset.QueueShard;

/**
 * Created by mykidong on 2016-09-09.
 */
public interface StoreHandler {

    public void add(QueueShard queueShard, long firstOffset, MessageList messageList);

    public MessageList getMessageList(QueueShard queueShard, long offset, int maxByteSize);
}
