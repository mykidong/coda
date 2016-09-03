package io.shunters.coda.processor;

import com.lmax.disruptor.EventFactory;
import io.shunters.coda.command.PutRequest;

/**
 * Created by mykidong on 2016-09-03.
 */
public class SetOffsetEvent {
    private BaseEvent baseEvent;
    private PutRequest putRequest;

    public BaseEvent getBaseEvent() {
        return baseEvent;
    }

    public void setBaseEvent(BaseEvent baseEvent) {
        this.baseEvent = baseEvent;
    }

    public PutRequest getPutRequest() {
        return putRequest;
    }

    public void setPutRequest(PutRequest putRequest) {
        this.putRequest = putRequest;
    }

    public static final EventFactory<SetOffsetEvent> FACTORY = new EventFactory<SetOffsetEvent>() {
        @Override
        public SetOffsetEvent newInstance() {
            return new SetOffsetEvent();
        }
    };
}
