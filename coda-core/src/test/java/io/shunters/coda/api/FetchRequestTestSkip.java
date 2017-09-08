package io.shunters.coda.api;

import com.cedarsoftware.util.io.JsonWriter;
import io.shunters.coda.deser.AvroDeSer;
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
public class FetchRequestTestSkip extends BaseRequestTest {

    private static Logger log = LoggerFactory.getLogger(FetchRequestTestSkip.class);

    String schemaKey = "io.shunters.coda.avro.api.FetchRequest";

    @Test
    public void serialize() {
        // fetch request.
        GenericRecord fetchRequest = this.buildFetchRequest(0, 400);

        AvroDeSer avroDeSer = AvroDeSer.getAvroDeSerSingleton();

        byte[] serializedAvro = avroDeSer.serialize(fetchRequest);


        GenericRecord deserializedAvro = avroDeSer.deserialize(schemaKey, serializedAvro);

        log.info("deserializedAvro json: \n" + JsonWriter.formatJson(deserializedAvro.toString()));
    }


    public GenericRecord buildFetchRequest(long fetchOffset, int maxBytes) {
        Schema schema = avroSchemaBuilder.getSchema(schemaKey);

        Schema requestHeaderSchema = schema.getField("requestHeader").schema();

        Schema fetchRequestMessageArraySchema = schema.getField("fetchRequestMessageArray").schema();

        Schema fetchRequestMessageSchema = fetchRequestMessageArraySchema.getElementType();

        Schema fetchRequestSubMessageArraySchema = fetchRequestMessageSchema.getField("fetchRequestSubMessageArray").schema();

        Schema fetchRequestSubMessageSchema = fetchRequestSubMessageArraySchema.getElementType();

        // fetchRequestSubMessage.
        GenericData.Record fetchRequestSubMessage = new GenericData.Record(fetchRequestSubMessageSchema);
        fetchRequestSubMessage.put("partition", 0);
        fetchRequestSubMessage.put("fetchOffset", fetchOffset);
        fetchRequestSubMessage.put("maxBytes", maxBytes);


        // fetchRequestSubMessage array.
        GenericData.Array<GenericData.Record> fetchRequestSubMessageArray = new GenericData.Array<GenericData.Record>(1, fetchRequestSubMessageArraySchema);
        fetchRequestSubMessageArray.add(fetchRequestSubMessage);

        // FetchRequestMessage
        GenericData.Record fetchRequestMessage = new GenericData.Record(fetchRequestMessageSchema);
        fetchRequestMessage.put("topicName", "any-topic");
        fetchRequestMessage.put("fetchRequestSubMessageArray", fetchRequestSubMessageArray);

        // fetchRequestMessageArray.
        GenericData.Array<GenericData.Record> fetchRequestMessageArray = new GenericData.Array<GenericData.Record>(1, fetchRequestMessageArraySchema);
        fetchRequestMessageArray.add(fetchRequestMessage);

        // RequestHeader.
        GenericData.Record requestHeader = new GenericData.Record(requestHeaderSchema);
        requestHeader.put("correlationId", 5);
        requestHeader.put("clientId", "any-client-id");

        // fetch request.
        GenericRecord fetchRequest = new GenericData.Record(schema);
        fetchRequest.put("requestHeader", requestHeader);
        fetchRequest.put("replicaId", 0);
        fetchRequest.put("maxWaitTime", 0);
        fetchRequest.put("minBytes", 0);
        fetchRequest.put("fetchRequestMessageArray", fetchRequestMessageArray);

        return fetchRequest;
    }
}
