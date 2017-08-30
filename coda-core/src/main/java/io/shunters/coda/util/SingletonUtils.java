package io.shunters.coda.util;

import io.shunters.coda.deser.AvroDeSer;
import io.shunters.coda.protocol.ApiKeyAvroSchemaMap;

/**
 * Created by mykidong on 2017-08-25.
 */
public class SingletonUtils {

    public static AvroDeSer getAvroDeSerSingleton()
    {
        return AvroDeSer.singleton(AvroSchemaBuilder.singleton(AvroSchemaBuilder.DEFAULT_AVRO_SCHEMA_DIR_PATH));
    }

    public static ApiKeyAvroSchemaMap getApiKeyAvroSchemaMapSingleton()
    {
        return ApiKeyAvroSchemaMap.singleton();
    }
}
