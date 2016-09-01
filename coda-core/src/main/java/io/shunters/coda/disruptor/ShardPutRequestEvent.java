package io.shunters.coda.disruptor;

import com.lmax.disruptor.EventFactory;
import io.shunters.coda.command.PutRequest;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ShardPutRequestEvent {

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

    public static final EventFactory<ShardPutRequestEvent> FACTORY = new EventFactory<ShardPutRequestEvent>() {
        @Override
        public ShardPutRequestEvent newInstance() {
            return new ShardPutRequestEvent();
        }
    };
}
