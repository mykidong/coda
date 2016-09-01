package io.shunters.coda.processor;

import io.shunters.coda.command.PutRequest;

/**
 * Created by mykidong on 2016-09-01.
 */
public class AddOffsetEvent {
    private BaseEvent baseEvent;

    private PutRequest putRequest;

    public AddOffsetEvent(BaseEvent baseEvent, PutRequest putRequest)
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
