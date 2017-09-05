package io.shunters.coda.processor;

import io.shunters.coda.offset.TopicPartition;
import io.shunters.coda.protocol.ClientServerSpec;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.Collection;
import java.util.Date;

/**
 * Created by mykidong on 2017-09-05.
 */
public class ProduceRequestHandler extends AbstractRequestHandler {

    @Override
    public GenericRecord handle(String channelId, NioSelector nioSelector, GenericRecord requestRecord) {

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

        GenericRecord requestHeader = (GenericRecord) requestRecord.get("requestHeader");

        int correlationId = (Integer) requestHeader.get("correlationId");


        Collection<GenericRecord> produceRequestMessageArray = (Collection<GenericRecord>) requestRecord.get("produceRequestMessageArray");

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

                int errorCode = logHandler.add(topicPartition, firstOffset, records);

                this.metricRegistry.meter("StoreProcessor.save.records").mark(recordSize);

                long timeStamp = new Date().getTime();

                // produceResponseSubMessage.
                GenericData.Record produceResponseSubMessage = new GenericData.Record(produceResponseSubMessageSchema);
                produceResponseSubMessage.put("partition", partition);
                produceResponseSubMessage.put("errorCode", errorCode);
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
        GenericRecord responseRecord = new GenericData.Record(produceResponseSchema);
        responseRecord.put("responseHeader", responseHeader);
        responseRecord.put("throttleTime", 4); // TODO: ...
        responseRecord.put("produceResponseMessageArray", produceResponseMessageArray);

        return responseRecord;
    }
}
