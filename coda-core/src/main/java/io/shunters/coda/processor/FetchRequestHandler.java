package io.shunters.coda.processor;

import io.shunters.coda.offset.TopicPartition;
import io.shunters.coda.protocol.ClientServerSpec;
import io.shunters.coda.store.PartitionLog;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created by mykidong on 2017-09-05.
 */
public class FetchRequestHandler extends AbstractRequestHandler {

    private static Logger log = LoggerFactory.getLogger(FetchRequestHandler.class);

    @Override
    public GenericRecord handle(String channelId, NioSelector nioSelector, GenericRecord requestRecord) {

        // ============== FetchResponse Schema =================

        // fetchResponse Schema.
        Schema fetchResponseSchema = apiKeyAvroSchemaMap.getSchema(ClientServerSpec.API_KEY_FETCH_RESPONSE);

        // ResponseHeader schema.
        Schema responseHeaderSchema = fetchResponseSchema.getField("responseHeader").schema();

        // fetchResponseMessageArray schema.
        Schema fetchResponseMessageArraySchema = fetchResponseSchema.getField("fetchResponseMessageArray").schema();

        // fetchResponseMessage schema.
        Schema fetchResponseMessageSchema = fetchResponseMessageArraySchema.getElementType();

        // fetchResponseSubMessageArray schema.
        Schema fetchResponseSubMessageArraySchema = fetchResponseMessageSchema.getField("fetchResponseSubMessageArray").schema();

        // fetchResponseSubMessage schema.
        Schema fetchResponseSubMessageSchema = fetchResponseSubMessageArraySchema.getElementType();

        // recordsArray schema.
        Schema recordsArraySchema = fetchResponseSubMessageSchema.getField("recordsArray").schema();

        // ========================================================


        GenericRecord requestHeader = (GenericRecord) requestRecord.get("requestHeader");

        int correlationId = (Integer) requestHeader.get("correlationId");


        Collection<GenericRecord> fetchRequestMessageArray = (Collection<GenericRecord>) requestRecord.get("fetchRequestMessageArray");

        // fetchResponseMessageArray.
        GenericData.Array<GenericData.Record> fetchResponseMessageArray = new GenericData.Array<GenericData.Record>(fetchRequestMessageArray.size(), fetchResponseMessageArraySchema);

        for (GenericRecord fetchRequestMessage : fetchRequestMessageArray) {
            String topicName = ((Utf8) fetchRequestMessage.get("topicName")).toString();

            Collection<GenericRecord> fetchRequestSubMessageArray = (Collection<GenericRecord>) fetchRequestMessage.get("fetchRequestSubMessageArray");

            // fetchResponseSubMessageArray.
            GenericData.Array<GenericData.Record> fetchResponseSubMessageArray = new GenericData.Array<GenericData.Record>(fetchRequestSubMessageArray.size(), fetchResponseSubMessageArraySchema);

            for (GenericRecord fetchRequestSubMessage : fetchRequestSubMessageArray) {
                int partition = (Integer) fetchRequestSubMessage.get("partition");
                long fetchOffset = (Long) fetchRequestSubMessage.get("fetchOffset");
                int maxBytes = (Integer) fetchRequestSubMessage.get("maxBytes");

                // fetch records.
                PartitionLog.FetchRecord fetchRecord = logHandler.fetch(new TopicPartition(topicName, partition), fetchOffset, maxBytes);
                int errorCode = fetchRecord.getErrorCode();
                long highwaterMarkOffset = fetchRecord.getHighwaterMarkOffset();
                List<GenericRecord> recordsList = fetchRecord.getRecordsList();

                // recordsArray.
                GenericData.Array<GenericRecord> recordsArray = new GenericData.Array<>(recordsList.size(), recordsArraySchema);
                recordsArray.addAll(recordsList);

                // fetchResponseSubMessage.
                GenericData.Record fetchResponseSubMessage = new GenericData.Record(fetchResponseSubMessageSchema);
                fetchResponseSubMessage.put("partition", partition);
                fetchResponseSubMessage.put("errorCode", errorCode);
                fetchResponseSubMessage.put("highwaterMarkOffset", highwaterMarkOffset);
                fetchResponseSubMessage.put("recordsArray", recordsArray);

                fetchResponseSubMessageArray.add(fetchResponseSubMessage);
            }

            // fetchResponseMessage.
            GenericData.Record fetchResponseMessage = new GenericData.Record(fetchResponseMessageSchema);
            fetchResponseMessage.put("topicName", topicName);
            fetchResponseMessage.put("fetchResponseSubMessageArray", fetchResponseSubMessageArray);

            fetchResponseMessageArray.add(fetchResponseMessage);
        }

        // responseHeader.
        GenericData.Record responseHeader = new GenericData.Record(responseHeaderSchema);
        responseHeader.put("correlationId", correlationId);


        // fetchResponse.
        GenericRecord responseRecord = new GenericData.Record(fetchResponseSchema);
        responseRecord.put("responseHeader", responseHeader);
        responseRecord.put("throttleTime", 4); // TODO: ...
        responseRecord.put("fetchResponseMessageArray", fetchResponseMessageArray);

        return responseRecord;
    }
}
