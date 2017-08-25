package io.shunters.coda.service;

import io.shunters.coda.api.service.AvroDeSerService;
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
public class ClasspathAvroDeSerServiceImpl implements AvroDeSerService {

    private static ClasspathAvroDeSerServiceImpl classpathAvroDeSerService;

    private AvroSchemaBuilder avroSchemaBuilder;

    private static final Object lock = new Object();

    public static ClasspathAvroDeSerServiceImpl singleton(AvroSchemaBuilder avroSchemaBuilder)
    {
        if(classpathAvroDeSerService == null)
        {
            synchronized (lock)
            {
                if(classpathAvroDeSerService == null)
                {
                    classpathAvroDeSerService = new ClasspathAvroDeSerServiceImpl(avroSchemaBuilder);
                }
            }
        }
        return classpathAvroDeSerService;
    }


    private ClasspathAvroDeSerServiceImpl(AvroSchemaBuilder avroSchemaBuilder)
    {
        this.avroSchemaBuilder = avroSchemaBuilder;
    }


    @Override
    public GenericRecord deserialize(String schemaName, byte[] avroBytes) {
        Schema schema = this.avroSchemaBuilder.getSchema(schemaName);

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);

        try {
            GenericRecord genericRecord = reader.read(null, decoder);

            return genericRecord;
        }catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
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
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
