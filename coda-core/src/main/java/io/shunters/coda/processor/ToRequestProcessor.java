package io.shunters.coda.processor;

import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.command.PutRequest;
import io.shunters.coda.command.RequestByteBuffer;
import io.shunters.coda.util.DisruptorBuilder;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ToRequestProcessor extends AbstractQueueThread<RequestByteBuffer> {

    public static final short META_REQUEST = 0;

    public static final short PUT_REQUEST = 100;
    public static final short GET_REQUEST = 101;

    public static final short OFFSET_REQUEST = 200;
    public static final short OFFSET_PUT_REQUEST = 201;
    public static final short OFFSET_GET_REQUEST = 202;

    public static final short CONSUMER_GROUP_COORDINATOR_REQUEST = 300;
    public static final short JOIN_GROUP_REQUEST = 301;
    public static final short HEARTBEAT_GROUP_REQUEST = 302;
    public static final short LEAVE_GROUP_REQUEST = 303;
    public static final short SYNC_GROUP_REQUEST = 304;
    public static final short DESCRIBE_GROUPS_REQUEST = 305;
    public static final short LIST_GROUPS_REQUEST = 306;

    private Disruptor<SetOffsetEvent> setOffsetDisruptor;
    private SetOffsetEventTranslator setOffsetEventTranslator;

    public ToRequestProcessor()
    {
        SetOffsetProcessor setOffsetProcessor = new SetOffsetProcessor();
        setOffsetProcessor.start();
        setOffsetDisruptor = DisruptorBuilder.singleton("SetOffset", SetOffsetEvent.FACTORY, 1024, setOffsetProcessor);
        this.setOffsetEventTranslator = new SetOffsetEventTranslator();
    }


    @Override
    public void process(RequestByteBuffer requestByteBuffer)
    {
        String channelId = requestByteBuffer.getChannelId();
        short commandId = requestByteBuffer.getCommandId();
        ByteBuffer buffer = requestByteBuffer.getBuffer();
        NioSelector nioSelector = requestByteBuffer.getNioSelector();

        if(commandId == PUT_REQUEST)
        {
            PutRequest putRequest = PutRequest.fromByteBuffer(buffer);

            // send to SetOffsetProcessor.
            this.setOffsetEventTranslator.setBaseEvent(new BaseEvent(channelId, nioSelector));
            this.setOffsetEventTranslator.setPutRequest(putRequest);
            this.setOffsetDisruptor.publishEvent(this.setOffsetEventTranslator);
        }
        // TODO: add another commands.
        else
        {
            // TODO:
        }
    }
}
