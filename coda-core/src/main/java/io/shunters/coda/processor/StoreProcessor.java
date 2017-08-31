package io.shunters.coda.processor;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.metrics.SystemOutMetricsReporter;
import io.shunters.coda.offset.*;
import io.shunters.coda.store.LogHandler;
import io.shunters.coda.util.DisruptorBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;

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



    private StoreProcessor()
    {
        logHandler = LogHandler.singleton();
        offsetHandler = OffsetManager.singleton();

        // metric registry.
        metricRegistry = MetricRegistryFactory.getInstance();

        // std out reporter for metrics.
        new SystemOutMetricsReporter(metricRegistry).start();

        this.responseEventDisruptor = DisruptorBuilder.singleton("ResponseProcessor", BaseMessage.ResponseEvent.FACTORY, 1024, ResponseProcessor.singleton());
        this.responseEventTranslator = new BaseMessage.ResponseEventTranslator();
    }

    @Override
    public void onEvent(BaseMessage.RequestEvent requestEvent, long l, boolean b) throws Exception {

        // ProduceRequest.
        GenericRecord requestMessage = requestEvent.getGenericRecord();

        Collection<GenericRecord> produceRequestMessageArray = (Collection<GenericRecord>)requestMessage.get("produceRequestMessageArray");

        for(GenericRecord produceRequestMessage : produceRequestMessageArray)
        {
            String topicName = ((Utf8)produceRequestMessage.get("topicName")).toString();

            Collection<GenericRecord> produceRequestSubMessageArray = (Collection<GenericRecord>) produceRequestMessage.get("produceRequestSubMessageArray");

            for(GenericRecord produceRequestSubMessage : produceRequestSubMessageArray)
            {
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
            }
        }

        // TODO: Construct Response Message conformed to protocol specs.


        String channelId = requestEvent.getChannelId();
        NioSelector nioSelector = requestEvent.getNioSelector();
        ByteBuffer responseBuffer = null;

        // TODO: build response message to respond back to client.
        //       IT IS JUST TEST PURPOSE, which must be removed in future.
        byte[] testResponse = "hello, I'm response!".getBytes();
        responseBuffer = ByteBuffer.allocate(testResponse.length);
        responseBuffer.put(testResponse);
        responseBuffer.rewind();

        // send response event to response disruptor.
        this.responseEventTranslator.setChannelId(channelId);
        this.responseEventTranslator.setNioSelector(nioSelector);
        this.responseEventTranslator.setResponseBuffer(responseBuffer);

        this.responseEventDisruptor.publishEvent(this.responseEventTranslator);
    }
}
