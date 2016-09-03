package io.shunters.coda.processor;

import com.lmax.disruptor.EventTranslator;
import io.shunters.coda.command.PutRequest;

/**
 * Created by mykidong on 2016-09-03.
 */
public class SetOffsetEventTranslator implements EventTranslator<SetOffsetEvent> {

    private BaseEvent baseEvent;
    private PutRequest putRequest;

    public void setBaseEvent(BaseEvent baseEvent) {
        this.baseEvent = baseEvent;
    }

    public void setPutRequest(PutRequest putRequest) {
        this.putRequest = putRequest;
    }

    @Override
    public void translateTo(SetOffsetEvent setOffsetEvent, long l) {
        setOffsetEvent.setBaseEvent(baseEvent);
        setOffsetEvent.setPutRequest(putRequest);
    }
}
