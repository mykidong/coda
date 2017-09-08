package io.shunters.coda.api;

import io.shunters.coda.deser.AvroDeSer;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by mykidong on 2017-08-29.
 */
public class SnappyCompressionTestSkip extends BaseRequestTest {

    private static Logger log = LoggerFactory.getLogger(SnappyCompressionTestSkip.class);

    String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";

    @Test
    public void compressAvro() throws Exception {
        // produce request.
        GenericRecord produceRequest = new ProduceRequestTestSkip().buildProduceRequest();

        AvroDeSer avroDeSer = AvroDeSer.getAvroDeSerSingleton();

        byte[] serializedAvro = avroDeSer.serialize(produceRequest);
        log.info("serializedAvro size: [" + serializedAvro.length + "]");

        // snappy compressed avro bytes.
        byte[] snappyCompressedAvro = Snappy.compress(serializedAvro);
        log.info("snappyCompressedAvro size: [" + snappyCompressedAvro.length + "]");


        // uncompressed avro bytes.
        byte[] uncompressedAvro = Snappy.uncompress(snappyCompressedAvro);
        log.info("uncompressedAvro size: [" + uncompressedAvro.length + "]");


        GenericRecord deserializedAvro = avroDeSer.deserialize(schemaKey, uncompressedAvro);
        //log.info("deserializedAvro json: \n" + JsonWriter.formatJson(deserializedAvro.toString()));
    }


    @Test
    public void validateSnappyCompression() throws Exception
    {
        byte[] input = RecordValueGenerator.generateBytesWithString(100);
        byte[] output = new byte[Snappy.maxCompressedLength(input.length)];

        int compressedSize = Snappy.compress(input, 0, input.length, output, 0);

        assertTrue(Snappy.isValidCompressedBuffer(output, 0, compressedSize));

        byte[] uncompressed = new byte[input.length];
        int uncompressedSize = Snappy.uncompress(output, 0, compressedSize, uncompressed, 0);

        assertEquals(input.length, uncompressedSize);
    }
}
