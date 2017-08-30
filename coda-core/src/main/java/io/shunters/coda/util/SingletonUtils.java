package io.shunters.coda.util;

import io.shunters.coda.api.service.AvroDeSerService;
import io.shunters.coda.protocol.ApiKeyAvroSchemaMap;
import io.shunters.coda.service.AvroDeSerServiceImpl;

/**
 * Created by mykidong on 2017-08-25.
 */
public class SingletonUtils {

    public static AvroDeSerService getAvroDeSerServiceSingleton()
    {
        return AvroDeSerServiceImpl.singleton(AvroSchemaBuilder.singleton(AvroSchemaBuilder.DEFAULT_AVRO_SCHEMA_DIR_PATH));
    }

    public static ApiKeyAvroSchemaMap getApiKeyAvroSchemaMapSingleton()
    {
        return ApiKeyAvroSchemaMap.singleton();
    }
}
