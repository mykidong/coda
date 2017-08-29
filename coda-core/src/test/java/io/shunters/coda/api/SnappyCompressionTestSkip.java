package io.shunters.coda.api;

import com.cedarsoftware.util.io.JsonWriter;
import io.shunters.coda.api.service.AvroDeSerService;
import io.shunters.coda.util.SingletonUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Created by mykidong on 2017-08-29.
 */
public class SnappyCompressionTestSkip extends BaseRequestTest {

    private static Logger log = LoggerFactory.getLogger(SnappyCompressionTestSkip.class);

    String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";

    @Test
    public void compressAvro() throws Exception
    {
        // produce request.
        GenericRecord produceRequest = new ProduceRequestTestSkip().buildProduceRequest();

        AvroDeSerService avroDeSerService = SingletonUtils.getClasspathAvroDeSerServiceSingleton();

        byte[] serializedAvro = avroDeSerService.serialize(produceRequest);
        log.info("serializedAvro size: [" + serializedAvro.length + "]");

        // snappy compressed avro bytes.
        byte[] snappyCompressedAvro = Snappy.compress(serializedAvro);
        log.info("snappyCompressedAvro size: [" + snappyCompressedAvro.length + "]");


        // uncompressed avro bytes.
        byte[] uncompressedAvro = Snappy.uncompress(snappyCompressedAvro);
        log.info("uncompressedAvro size: [" + uncompressedAvro.length + "]");


        GenericRecord deserializedAvro = avroDeSerService.deserialize(schemaKey, uncompressedAvro);
        //log.info("deserializedAvro json: \n" + JsonWriter.formatJson(deserializedAvro.toString()));
    }
}
