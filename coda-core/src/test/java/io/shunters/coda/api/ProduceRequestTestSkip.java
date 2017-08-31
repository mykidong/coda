package io.shunters.coda.api;

import com.cedarsoftware.util.io.JsonWriter;
import io.shunters.coda.deser.AvroDeSer;
import io.shunters.coda.util.SingletonUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Created by mykidong on 2017-08-27.
 */
public class ProduceRequestTestSkip extends BaseRequestTest {

    private static Logger log = LoggerFactory.getLogger(ProduceRequestTestSkip.class);

    String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";

    @Test
    public void serialize() {
        // produce request.
        GenericRecord produceRequest = this.buildProduceRequest();

        AvroDeSer avroDeSer = SingletonUtils.getAvroDeSerSingleton();

        byte[] serializedAvro = avroDeSer.serialize(produceRequest);


        GenericRecord deserializedAvro = avroDeSer.deserialize(schemaKey, serializedAvro);

        log.info("deserializedAvro json: \n" + JsonWriter.formatJson(deserializedAvro.toString()));
    }


    public GenericRecord buildProduceRequest() {
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
        for (int i = 0; i < recordSize; i++) {
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
            record.put("offsetDelta", i);
            record.put("key", ByteBuffer.wrap(new String("any-key" + i).getBytes()));

            byte[] value = RecordValueGenerator.generateBytesWithString(100);

            record.put("value", ByteBuffer.wrap(value));
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


    @Test
    public void printGenericRecord() {
        GenericRecord produceRequest = this.buildProduceRequest();

        Collection<GenericRecord> produceRequestMessageArray = (Collection<GenericRecord>) produceRequest.get("produceRequestMessageArray");

        for (GenericRecord produceRequestMessage : produceRequestMessageArray) {
            String topicName = ((String) produceRequestMessage.get("topicName")).toString();

            Collection<GenericRecord> produceRequestSubMessageArray = (Collection<GenericRecord>) produceRequestMessage.get("produceRequestSubMessageArray");

            for (GenericRecord produceRequestSubMessage : produceRequestSubMessageArray) {
                int partition = (Integer) produceRequestSubMessage.get("partition");

                // avro data records.
                GenericRecord records = (GenericRecord) produceRequestSubMessage.get("records");

                int recordSize = ((Collection<GenericRecord>) records.get("records")).size();

                log.info("records json: \n" + JsonWriter.formatJson(records.toString()));
            }
        }
    }
}
