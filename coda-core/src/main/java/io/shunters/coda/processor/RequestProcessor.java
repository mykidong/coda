package io.shunters.coda.processor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.deser.AvroDeSer;
import io.shunters.coda.protocol.ApiKeyAvroSchemaMap;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.util.DisruptorCreator;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mykidong on 2016-09-01.
 */
public class RequestProcessor implements EventHandler<BaseMessage.RequestBytesEvent> {

    private static Logger log = LoggerFactory.getLogger(RequestProcessor.class);

    /**
     * request event disruptor.
     */
    private Disruptor<BaseMessage.RequestEvent> requestEventDisruptor;

    /**
     * request event translator.
     */
    private BaseMessage.RequestEventTranslator requestEventTranslator;

    private RequestHandler fetchRequestHandler;

    private AvroDeSer avroDeSer;

    private static final Object lock = new Object();

    private static RequestProcessor requestProcessor;

    public static RequestProcessor singleton() {
        if (requestProcessor == null) {
            synchronized (lock) {
                if (requestProcessor == null) {
                    requestProcessor = new RequestProcessor();
                }
            }
        }
        return requestProcessor;
    }


    private RequestProcessor() {
        this.avroDeSer = AvroDeSer.getAvroDeSerSingleton();
        this.fetchRequestHandler = new FetchRequestHandler();

        this.requestEventDisruptor = DisruptorCreator.singleton("StoreProcessor", BaseMessage.RequestEvent.FACTORY, 1024, StoreProcessor.singleton());
        this.requestEventTranslator = new BaseMessage.RequestEventTranslator();
    }

    @Override
    public void onEvent(BaseMessage.RequestBytesEvent requestBytesEvent, long l, boolean b) throws Exception {
        String channelId = requestBytesEvent.getChannelId();
        NioSelector nioSelector = requestBytesEvent.getNioSelector();

        short apiKey = requestBytesEvent.getApiKey();

        short apiVersion = requestBytesEvent.getApiVersion();

        // api version 1 is allowed.
        if (apiVersion != ClientServerSpec.API_VERSION_1) {
            log.error("API Version [" + apiVersion + "] not allowed!");

            return;
        }

        byte[] messsageBytes = requestBytesEvent.getMessageBytes();

        // avro schema name.
        String schemaName = ApiKeyAvroSchemaMap.getApiKeyAvroSchemaMapSingleton().getSchemaName(apiKey);

        // deserialize avro bytes message.
        GenericRecord genericRecord = avroDeSer.deserialize(schemaName, messsageBytes);

        // ProduceRequest.
        if (apiKey == ClientServerSpec.API_KEY_PRODUCE_REQUEST) {
//            String prettyJson = JsonWriter.formatJson(genericRecord.toString());
//            log.info("produce request message: \n" + prettyJson);

            // construct request event.
            this.requestEventTranslator.setChannelId(requestBytesEvent.getChannelId());
            this.requestEventTranslator.setNioSelector(requestBytesEvent.getNioSelector());
            this.requestEventTranslator.setApiKey(requestBytesEvent.getApiKey());
            this.requestEventTranslator.setApiVersion(requestBytesEvent.getApiVersion());
            this.requestEventTranslator.setMessageFormat(requestBytesEvent.getMessageFormat());
            this.requestEventTranslator.setGenericRecord(genericRecord);

            // send request event to StoreProcessor.
            this.requestEventDisruptor.publishEvent(this.requestEventTranslator);
        }
        // FetchRequest.
        else if (apiKey == ClientServerSpec.API_KEY_FETCH_REQUEST) {
            this.fetchRequestHandler.handleAndResponse(channelId, nioSelector, genericRecord);
        } else {
            // TODO:
        }
    }
}
