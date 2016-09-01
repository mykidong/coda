package io.shunters.coda.disruptor;

import com.lmax.disruptor.EventFactory;
import io.shunters.coda.command.RequestByteBuffer;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ToRequestEvent {

    private RequestByteBuffer requestByteBuffer;

    public RequestByteBuffer getRequestByteBuffer() {
        return requestByteBuffer;
    }

    public void setRequestByteBuffer(RequestByteBuffer requestByteBuffer) {
        this.requestByteBuffer = requestByteBuffer;
    }

    public static final EventFactory<ToRequestEvent> FACTORY = new EventFactory<ToRequestEvent>() {
        @Override
        public ToRequestEvent newInstance() {
            return new ToRequestEvent();
        }
    };
}
