package io.shunters.coda.processor;

import com.cedarsoftware.util.io.JsonWriter;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.api.service.AvroDeSerService;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.util.DisruptorBuilder;
import io.shunters.coda.util.SingletonUtils;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mykidong on 2016-09-01.
 */
public class ToRequestProcessor implements EventHandler<BaseMessage.BaseMessageBytesEvent> {

    private static Logger log = LoggerFactory.getLogger(ToRequestProcessor.class);

    /**
     * avro de-/serialization service.
     */
    private static AvroDeSerService avroDeSerService = SingletonUtils.getClasspathAvroDeSerServiceSingleton();

    /**
     * base message event disruptor.
     */
    private Disruptor<BaseMessage.BaseMessageEvent> baseMessageEventDisruptor;

    /**
     * base message event translator.
     */
    private BaseMessage.BaseMessageEventTranslator baseMessageEventTranslator;

    private static final Object lock = new Object();

    private static ToRequestProcessor toRequestProcessor;

    public static ToRequestProcessor singleton()
    {
        if(toRequestProcessor == null)
        {
            synchronized (lock)
            {
                if(toRequestProcessor == null)
                {
                    toRequestProcessor = new ToRequestProcessor();
                }
            }
        }
        return toRequestProcessor;
    }


    private ToRequestProcessor()
    {
        this.baseMessageEventDisruptor = DisruptorBuilder.singleton("AddOffset", BaseMessage.BaseMessageEvent.FACTORY, 1024, AddOffsetProcessor.singleton());
        this.baseMessageEventTranslator = new BaseMessage.BaseMessageEventTranslator();
    }

    @Override
    public void onEvent(BaseMessage.BaseMessageBytesEvent baseMessageBytesEvent, long l, boolean b) throws Exception {
        short apiKey = baseMessageBytesEvent.getApiKey();
        byte[] messsageBytes = baseMessageBytesEvent.getMessageBytes();

        // avro schema name.
        String schemaName = SingletonUtils.getApiKeyAvroSchemaMapSingleton().getSchemaName(apiKey);

        // deserialize avro bytes message.
        GenericRecord genericRecord = avroDeSerService.deserialize(schemaName, messsageBytes);


        if(apiKey == ClientServerSpec.API_KEY_PRODUCE_REQUEST)
        {
            String prettyJson = JsonWriter.formatJson(genericRecord.toString());
            log.info("produce request message: \n" + prettyJson);
            log.info("--------------------");


//            // construct base message event.
//            this.baseMessageEventTranslator.setChannelId(baseMessageBytesEvent.getChannelId());
//            this.baseMessageEventTranslator.setNioSelector(baseMessageBytesEvent.getNioSelector());
//            this.baseMessageEventTranslator.setApiKey(baseMessageBytesEvent.getApiKey());
//            this.baseMessageEventTranslator.setApiVersion(baseMessageBytesEvent.getApiVersion());
//            this.baseMessageEventTranslator.setMessageFormat(baseMessageBytesEvent.getMessageFormat());
//            this.baseMessageEventTranslator.setGenericRecord(genericRecord);
//
//            // send base message event to disruptor.
//            this.baseMessageEventDisruptor.publishEvent(this.baseMessageEventTranslator);
        }
        // TODO: add another api implementation.
        else
        {
            // TODO:
        }
    }
}
