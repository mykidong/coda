package io.shunters.coda.deser;

import io.shunters.coda.util.AvroSchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;

/**
 * Created by mykidong on 2017-08-25.
 */
public class AvroDeSer {

    private static AvroDeSer avroDeSer;

    private AvroSchemaBuilder avroSchemaBuilder;

    private static final Object lock = new Object();

    public static AvroDeSer singleton(AvroSchemaBuilder avroSchemaBuilder) {
        if (avroDeSer == null) {
            synchronized (lock) {
                if (avroDeSer == null) {
                    avroDeSer = new AvroDeSer(avroSchemaBuilder);
                }
            }
        }
        return avroDeSer;
    }


    private AvroDeSer(AvroSchemaBuilder avroSchemaBuilder) {
        this.avroSchemaBuilder = avroSchemaBuilder;
    }


    public GenericRecord deserialize(String schemaName, byte[] avroBytes) {
        Schema schema = this.avroSchemaBuilder.getSchema(schemaName);

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);

        try {
            GenericRecord genericRecord = reader.read(null, decoder);

            return genericRecord;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] serialize(GenericRecord genericRecord) {
        Schema schema = genericRecord.getSchema();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(genericRecord, encoder);
            encoder.flush();

            byte[] avroBytes = out.toByteArray();
            out.close();

            return avroBytes;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
