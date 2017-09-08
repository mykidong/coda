package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;

import java.nio.channels.SelectionKey;

/**
 * Created by mykidong on 2017-08-29.
 */
public class ResponseProcessor implements EventHandler<BaseMessage.ResponseEvent> {

    private static final Object lock = new Object();

    private static ResponseProcessor responseProcessor;

    public static ResponseProcessor singleton()
    {
        if(responseProcessor == null)
        {
            synchronized (lock)
            {
                if(responseProcessor == null)
                {
                    responseProcessor = new ResponseProcessor();
                }
            }
        }
        return responseProcessor;
    }

    private ResponseProcessor()
    {

    }



    @Override
    public void onEvent(BaseMessage.ResponseEvent responseEvent, long l, boolean b) throws Exception {
        String channelId = responseEvent.getChannelId();
        NioSelector nioSelector = responseEvent.getNioSelector();

        // attache response to channel with SelectionKey.OP_WRITE, which causes channel processor to send response to the client.
        nioSelector.attach(channelId, SelectionKey.OP_WRITE, responseEvent.getResponseBuffer());

        // wakeup must be called.
        nioSelector.wakeup();
    }
}
