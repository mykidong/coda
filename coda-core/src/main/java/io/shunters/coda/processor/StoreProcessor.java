package io.shunters.coda.processor;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.deser.MessageDeSer;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.metrics.SystemOutMetricsReporter;
import io.shunters.coda.offset.OffsetHandler;
import io.shunters.coda.offset.OffsetManager;
import io.shunters.coda.offset.TopicPartition;
import io.shunters.coda.protocol.ApiKeyAvroSchemaMap;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.store.LogHandler;
import io.shunters.coda.util.DisruptorBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;

/**
 * Created by mykidong on 2016-09-03.
 */
public class StoreProcessor implements EventHandler<BaseMessage.RequestEvent> {

    private static Logger log = LoggerFactory.getLogger(StoreProcessor.class);

    private MetricRegistry metricRegistry;

    private LogHandler logHandler;

    private OffsetHandler offsetHandler;

    /**
     * response event disruptor.
     */
    private Disruptor<BaseMessage.ResponseEvent> responseEventDisruptor;

    /**
     * response event translator.
     */
    private BaseMessage.ResponseEventTranslator responseEventTranslator;

    private ApiKeyAvroSchemaMap apiKeyAvroSchemaMap;

    private MessageDeSer messageDeSer;

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
        logHandler = LogHandler.singleton();
        offsetHandler = OffsetManager.singleton();

        apiKeyAvroSchemaMap = ApiKeyAvroSchemaMap.getApiKeyAvroSchemaMapSingleton();
        messageDeSer = MessageDeSer.singleton();

        // metric registry.
        metricRegistry = MetricRegistryFactory.getInstance();

        // std out reporter for metrics.
        new SystemOutMetricsReporter(metricRegistry).start();

        this.responseEventDisruptor = DisruptorBuilder.singleton("ResponseProcessor", BaseMessage.ResponseEvent.FACTORY, 1024, ResponseProcessor.singleton());
        this.responseEventTranslator = new BaseMessage.ResponseEventTranslator();
    }

    @Override
    public void onEvent(BaseMessage.RequestEvent requestEvent, long l, boolean b) throws Exception {

        // ============== ProduceResponse Schema =================

        // ProduceResponse Schema.
        Schema produceResponseSchema = apiKeyAvroSchemaMap.getSchema(ClientServerSpec.API_KEY_PRODUCE_RESPONSE);

        // ResponseHeader schema.
        Schema responseHeaderSchema = produceResponseSchema.getField("responseHeader").schema();

        // produceResponseMessageArray schema.
        Schema produceResponseMessageArraySchema = produceResponseSchema.getField("produceResponseMessageArray").schema();

        // produceResponseMessage schema.
        Schema produceResponseMessageSchema = produceResponseMessageArraySchema.getElementType();

        // produceResponseSubMessageArray schema.
        Schema produceResponseSubMessageArraySchema = produceResponseMessageSchema.getField("produceResponseSubMessageArray").schema();

        // produceResponseSubMessage schema.
        Schema produceResponseSubMessageSchema = produceResponseSubMessageArraySchema.getElementType();

        // ========================================================


        // ProduceRequest.
        GenericRecord requestMessage = requestEvent.getGenericRecord();

        GenericRecord requestHeader = (GenericRecord) requestMessage.get("requestHeader");

        int correlationId = (Integer) requestHeader.get("correlationId");


        Collection<GenericRecord> produceRequestMessageArray = (Collection<GenericRecord>) requestMessage.get("produceRequestMessageArray");

        // produceResponseMessageArray.
        GenericData.Array<GenericData.Record> produceResponseMessageArray = new GenericData.Array<GenericData.Record>(produceRequestMessageArray.size(), produceResponseMessageArraySchema);

        for (GenericRecord produceRequestMessage : produceRequestMessageArray) {
            String topicName = ((Utf8) produceRequestMessage.get("topicName")).toString();

            Collection<GenericRecord> produceRequestSubMessageArray = (Collection<GenericRecord>) produceRequestMessage.get("produceRequestSubMessageArray");


            // produceResponseSubMessageArray.
            GenericData.Array<GenericData.Record> produceResponseSubMessageArray = new GenericData.Array<GenericData.Record>(produceRequestSubMessageArray.size(), produceResponseSubMessageArraySchema);

            for (GenericRecord produceRequestSubMessage : produceRequestSubMessageArray) {
                int partition = (Integer) produceRequestSubMessage.get("partition");

                // avro data records.
                GenericRecord records = (GenericRecord) produceRequestSubMessage.get("records");

                int recordSize = ((Collection<GenericRecord>) records.get("records")).size();

                TopicPartition topicPartition = new TopicPartition(topicName, partition);

                // firstOffset for this record array.
                long firstOffset = offsetHandler.getCurrentOffsetAndIncrease(topicPartition, recordSize);

                records.put("firstOffset", firstOffset);

                logHandler.add(topicPartition, firstOffset, records);

                this.metricRegistry.meter("StoreProcessor.save.records").mark(recordSize);

                long timeStamp = new Date().getTime();

                // produceResponseSubMessage.
                GenericData.Record produceResponseSubMessage = new GenericData.Record(produceResponseSubMessageSchema);
                produceResponseSubMessage.put("partition", partition);
                produceResponseSubMessage.put("errorCode", 0);
                produceResponseSubMessage.put("offset", firstOffset);
                produceResponseSubMessage.put("timestamp", timeStamp);

                produceResponseSubMessageArray.add(produceResponseSubMessage);
            }

            // produceResponseMessage.
            GenericData.Record produceResponseMessage = new GenericData.Record(produceResponseMessageSchema);
            produceResponseMessage.put("topicName", topicName);
            produceResponseMessage.put("produceResponseSubMessageArray", produceResponseSubMessageArray);

            produceResponseMessageArray.add(produceResponseMessage);
        }


        // responseHeader.
        GenericData.Record responseHeader = new GenericData.Record(responseHeaderSchema);
        responseHeader.put("correlationId", correlationId);


        // ProduceResponse.
        GenericData.Record produceResponse = new GenericData.Record(produceResponseSchema);
        produceResponse.put("responseHeader", responseHeader);
        produceResponse.put("throttleTime", 4);
        produceResponse.put("produceResponseMessageArray", produceResponseMessageArray);


        String channelId = requestEvent.getChannelId();
        NioSelector nioSelector = requestEvent.getNioSelector();
        ByteBuffer responseBuffer = messageDeSer.serializeResponseToByteBuffer(ClientServerSpec.COMPRESSION_CODEC_SNAPPY, produceResponse).getByteBuffer();

        // send response event to response disruptor.
        this.responseEventTranslator.setChannelId(channelId);
        this.responseEventTranslator.setNioSelector(nioSelector);
        this.responseEventTranslator.setResponseBuffer(responseBuffer);

        this.responseEventDisruptor.publishEvent(this.responseEventTranslator);
    }
}
