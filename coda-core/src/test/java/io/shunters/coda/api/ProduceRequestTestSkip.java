package io.shunters.coda.api;

import com.cedarsoftware.util.io.JsonWriter;
import io.shunters.coda.api.service.AvroDeSerService;
import io.shunters.coda.util.AvroSchemaBuilder;
import io.shunters.coda.util.SingletonUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by mykidong on 2017-08-27.
 */
public class ProduceRequestTestSkip extends BaseRequestTest {

    private static Logger log = LoggerFactory.getLogger(ProduceRequestTestSkip.class);

    String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";

    @Test
    public void serialize()
    {
        // produce request.
        GenericRecord produceRequest = this.buildProduceRequest();

        AvroDeSerService avroDeSerService = SingletonUtils.getClasspathAvroDeSerServiceSingleton();

        byte[] serializedAvro = avroDeSerService.serialize(produceRequest);


        GenericRecord deserializedAvro = avroDeSerService.deserialize(schemaKey, serializedAvro);

        log.info("deserializedAvro json: \n" + JsonWriter.formatJson(deserializedAvro.toString()));
    }


    public GenericRecord buildProduceRequest()
    {
        Schema schema = avroSchemaBuilder.getSchema(schemaKey);

        Schema produceRequestMessageArraySchema = schema.getField("produceRequestMessageArray").schema();

        Schema produceRequestMessageSchema = produceRequestMessageArraySchema.getElementType();

        Schema produceRequestSubMessageArraySchema = produceRequestMessageSchema.getField("produceRequestSubMessageArray").schema();

        Schema produceRequestSubMessageSchema = produceRequestSubMessageArraySchema.getElementType();

        Schema recordsSchema = produceRequestSubMessageSchema.getField("records").schema();

        Schema recordArraySchema = recordsSchema.getField("records").schema();

        Schema recordSchema = recordArraySchema.getElementType();

        Schema recordHeaderArraySchema = recordSchema.getField("recordHeaders").schema();

        Schema recordHeaderSchema = recordHeaderArraySchema.getElementType();

        int recordSize = 100;

        // record array.
        GenericData.Array<GenericData.Record> recordArray = new GenericData.Array<GenericData.Record>(recordSize, recordArraySchema);
        for(int i = 0; i < recordSize; i++)
        {
            // record header.
            GenericData.Record recordHeader = new GenericData.Record(recordHeaderSchema);
            recordHeader.put("key", "header-key");
            recordHeader.put("value", ByteBuffer.wrap("header-value".getBytes()));

            // record header array.
            GenericData.Array<GenericData.Record> recordHeaderArray = new GenericData.Array<GenericData.Record>(1, recordHeaderArraySchema);
            recordHeaderArray.add(recordHeader);

            // record.
            GenericData.Record record = new GenericData.Record(recordSchema);
            record.put("attributes", 1);
            record.put("timestampDelta", 4);
            record.put("offsetDelta", 400);
            record.put("key", ByteBuffer.wrap(new String("any-key" + i).getBytes()));
            record.put("value", ByteBuffer.wrap(new String("any-record-value-" + i).getBytes()));
            record.put("recordHeaders", recordHeaderArray);

            recordArray.add(record);
        }



        // records.
        GenericData.Record records = new GenericData.Record(recordsSchema);
        records.put("firstOffset", 1L);
        records.put("partitionLeaderEpoch", 4);
        records.put("magic", 0);
        records.put("crc", 4);
        records.put("attributes", 3);
        records.put("lastOffsetDelta", 200);
        records.put("firstTimestamp", 0L);
        records.put("maxTimestamp", 222220000L);
        records.put("producerId", 8L);
        records.put("producerEpoch", 4);
        records.put("firstSequence", 4);
        records.put("records", recordArray);


        // produceRequestSubMessage.
        GenericData.Record produceRequestSubMessage = new GenericData.Record(produceRequestSubMessageSchema);
        produceRequestSubMessage.put("partition", 0);
        produceRequestSubMessage.put("records", records);


        // produceRequestSubMessage array.
        GenericData.Array<GenericData.Record> produceRequestSubMessageArray = new GenericData.Array<GenericData.Record>(1, produceRequestSubMessageArraySchema);
        produceRequestSubMessageArray.add(produceRequestSubMessage);

        // ProduceRequestMessage
        GenericData.Record produceRequestMessage = new GenericData.Record(produceRequestMessageSchema);
        produceRequestMessage.put("topicName", "any-topic");
        produceRequestMessage.put("produceRequestSubMessageArray", produceRequestSubMessageArray);

        // produceRequestMessageArray.
        GenericData.Array<GenericData.Record> produceRequestMessageArray = new GenericData.Array<GenericData.Record>(1, produceRequestMessageArraySchema);
        produceRequestMessageArray.add(produceRequestMessage);

        // produce request.
        GenericRecord produceRequest = new GenericData.Record(schema);
        produceRequest.put("requiredAcks", 1);
        produceRequest.put("timeout", 1);
        produceRequest.put("produceRequestMessageArray", produceRequestMessageArray);

        return produceRequest;
    }
}