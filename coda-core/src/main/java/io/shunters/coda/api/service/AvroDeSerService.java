package io.shunters.coda.api.service;

import org.apache.avro.generic.GenericRecord;

/**
 * Created by mykidong on 2017-08-25.
 */
public interface AvroDeSerService {

    public GenericRecord deserialize(String schemaName, byte[] avroBytes);

    public byte[] serialize(GenericRecord genericRecord);
}
