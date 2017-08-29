package io.shunters.coda.protocol;

/**
 * Created by mykidong on 2017-08-25.
 */
public class ClientServerSpec {

    /**
     * message format.
     */
    public static final byte MESSAGE_FORMAT_AVRO = 1;

    /**
     * compression codec.
     */
    public static final byte COMPRESSION_CODEC_NONE = 0;
    public static final byte COMPRESSION_CODEC_SNAPPY = 1;


    /**
     * api key and corresponding avro schema name.
     */
    public static final short API_KEY_PRODUCE_REQUEST = 100;
    public static final String AVRO_SCHEMA_NAME_PRODUCE_REQUEST = "io.shunters.coda.avro.api.ProduceRequest";

}