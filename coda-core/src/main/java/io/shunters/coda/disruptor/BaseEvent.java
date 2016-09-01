package io.shunters.coda.disruptor;

import io.shunters.coda.NioSelector;

/**
 * Created by mykidong on 2016-09-01.
 */
public class BaseEvent {

    private String channelId;

    private NioSelector nioSelector;

    public BaseEvent(String channelId, NioSelector nioSelector)
    {
        this.channelId = channelId;
        this.nioSelector = nioSelector;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public NioSelector getNioSelector() {
        return nioSelector;
    }

    public void setNioSelector(NioSelector nioSelector) {
        this.nioSelector = nioSelector;
    }
}
