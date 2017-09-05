package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreProcessor implements EventHandler<BaseMessage.RequestEvent> {

    private static Logger log = LoggerFactory.getLogger(StoreProcessor.class);

    private RequestHandler produceRequestHandler;

    private static final Object lock = new Object();

    private static StoreProcessor storeProcessor;

    public static StoreProcessor singleton() {
        if (storeProcessor == null) {
            synchronized (lock) {
                if (storeProcessor == null) {
                    storeProcessor = new StoreProcessor();
                }
            }
        }
        return storeProcessor;
    }


    private StoreProcessor() {
        produceRequestHandler = new ProduceRequestHandler();
    }

    @Override
    public void onEvent(BaseMessage.RequestEvent requestEvent, long l, boolean b) throws Exception {
        this.produceRequestHandler.handleAndResponse(requestEvent.getChannelId(), requestEvent.getNioSelector(), requestEvent.getGenericRecord());
    }
}
