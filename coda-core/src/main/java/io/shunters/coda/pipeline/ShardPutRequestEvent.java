package io.shunters.coda.pipeline;

import com.lmax.disruptor.EventFactory;
import io.shunters.coda.command.PutRequest;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ShardPutRequestEvent {

    private BaseEvent baseEvent;

    private PutRequest putRequest;

    public ShardPutRequestEvent(BaseEvent baseEvent, PutRequest putRequest)
    {
        this.baseEvent = baseEvent;
        this.putRequest = putRequest;
    }

    public BaseEvent getBaseEvent() {
        return baseEvent;
    }

    public PutRequest getPutRequest() {
        return putRequest;
    }
}
