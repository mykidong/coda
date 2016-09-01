package io.shunters.coda.disruptor;

import com.lmax.disruptor.EventTranslator;
import io.shunters.coda.command.RequestByteBuffer;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ToRequestTranslator implements EventTranslator<ToRequestEvent> {

    private RequestByteBuffer requestByteBuffer;

    public void setRequestByteBuffer(RequestByteBuffer requestByteBuffer) {
        this.requestByteBuffer = requestByteBuffer;
    }

    @Override
    public void translateTo(ToRequestEvent toRequestEvent, long l) {
        toRequestEvent.setRequestByteBuffer(this.requestByteBuffer);
    }
}
