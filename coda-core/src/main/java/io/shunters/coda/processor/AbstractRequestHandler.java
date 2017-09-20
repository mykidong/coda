package io.shunters.coda.processor;

import com.codahale.metrics.MetricRegistry;
import com.lmax.disruptor.dsl.Disruptor;
import io.shunters.coda.deser.MessageDeSer;
import io.shunters.coda.metrics.MetricRegistryFactory;
import io.shunters.coda.metrics.SystemOutMetricsReporter;
import io.shunters.coda.offset.OffsetHandler;
import io.shunters.coda.offset.PartitionOffsetHandler;
import io.shunters.coda.protocol.ApiKeyAvroSchemaMap;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.store.LogHandler;
import io.shunters.coda.store.PartitionLogHandler;
import io.shunters.coda.util.DisruptorCreator;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2017-09-05.
 */
public abstract class AbstractRequestHandler implements RequestHandler {

    protected LogHandler logHandler;

    protected OffsetHandler offsetHandler;

    protected MetricRegistry metricRegistry;

    protected ApiKeyAvroSchemaMap apiKeyAvroSchemaMap;

    protected MessageDeSer messageDeSer;

    /**
     * response event disruptor.
     */
    private Disruptor<BaseMessage.ResponseEvent> responseEventDisruptor;

    /**
     * response event translator.
     */
    private BaseMessage.ResponseEventTranslator responseEventTranslator;

    public AbstractRequestHandler() {
        logHandler = PartitionLogHandler.singleton();
        offsetHandler = PartitionOffsetHandler.singleton();

        apiKeyAvroSchemaMap = ApiKeyAvroSchemaMap.getApiKeyAvroSchemaMapSingleton();
        messageDeSer = MessageDeSer.singleton();

        // metric registry.
        metricRegistry = MetricRegistryFactory.getInstance();

        // std out reporter for metrics.
        new SystemOutMetricsReporter(metricRegistry).start();

        this.responseEventDisruptor = DisruptorCreator.singleton(DisruptorCreator.DISRUPTOR_NAME_RESPONSE_PROCESSOR, BaseMessage.ResponseEvent.FACTORY, 1024, ResponseProcessor.singleton());
        this.responseEventTranslator = new BaseMessage.ResponseEventTranslator();
    }

    public abstract GenericRecord handle(String channelId, NioSelector nioSelector, GenericRecord requestRecord);


    @Override
    public void handleAndResponse(String channelId, NioSelector nioSelector, GenericRecord requestRecord) {
        GenericRecord responseRecord = handle(channelId, nioSelector, requestRecord);

        ByteBuffer responseBuffer = messageDeSer.serializeResponseToByteBuffer(ClientServerSpec.COMPRESSION_CODEC_SNAPPY, responseRecord).getByteBuffer();

        // send response event to response disruptor.
        this.responseEventTranslator.setChannelId(channelId);
        this.responseEventTranslator.setNioSelector(nioSelector);
        this.responseEventTranslator.setResponseBuffer(responseBuffer);

        this.responseEventDisruptor.publishEvent(this.responseEventTranslator);
    }
}
