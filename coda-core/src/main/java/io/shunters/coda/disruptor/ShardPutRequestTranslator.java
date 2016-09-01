package io.shunters.coda.disruptor;

import com.lmax.disruptor.EventTranslator;
import io.shunters.coda.command.PutRequest;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ShardPutRequestTranslator implements EventTranslator<ShardPutRequestEvent> {

    private BaseEvent baseEvent;

    private PutRequest putRequest;

    public void setBaseEvent(BaseEvent baseEvent) {
        this.baseEvent = baseEvent;
    }

    public void setPutRequest(PutRequest putRequest) {
        this.putRequest = putRequest;
    }

    @Override
    public void translateTo(ShardPutRequestEvent shardPutRequestEvent, long l) {
        shardPutRequestEvent.setBaseEvent(this.baseEvent);
        shardPutRequestEvent.setPutRequest(this.putRequest);
    }
}
