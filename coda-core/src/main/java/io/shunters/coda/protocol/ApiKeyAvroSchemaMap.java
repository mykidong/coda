package io.shunters.coda.protocol;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mykidong on 2017-08-25.
 */
public class ApiKeyAvroSchemaMap {

    private AvroSchemaLoader avroSchemaBuilder;

    private static ApiKeyAvroSchemaMap apiKeyAvroSchemaMap;

    private static final Object lock = new Object();

    private Map<Short, String> apiMap = new HashMap<>();

    public static ApiKeyAvroSchemaMap singleton(AvroSchemaLoader avroSchemaBuilder)
    {
        if(apiKeyAvroSchemaMap == null)
        {
            synchronized (lock)
            {
                if(apiKeyAvroSchemaMap == null)
                {
                    apiKeyAvroSchemaMap = new ApiKeyAvroSchemaMap(avroSchemaBuilder);
                }
            }
        }
        return apiKeyAvroSchemaMap;
    }

    public static ApiKeyAvroSchemaMap getApiKeyAvroSchemaMapSingleton()
    {
        return ApiKeyAvroSchemaMap.singleton(AvroSchemaLoader.singleton(AvroSchemaLoader.DEFAULT_AVRO_SCHEMA_DIR_PATH));
    }

    private ApiKeyAvroSchemaMap(AvroSchemaLoader avroSchemaBuilder)
    {
        this.avroSchemaBuilder = avroSchemaBuilder;

        // Produce.
        apiMap.put(ClientServerSpec.API_KEY_PRODUCE_REQUEST, ClientServerSpec.AVRO_SCHEMA_NAME_PRODUCE_REQUEST);
        apiMap.put(ClientServerSpec.API_KEY_PRODUCE_RESPONSE, ClientServerSpec.AVRO_SCHEMA_NAME_PRODUCE_RESPONSE);

        // Fetch.
        apiMap.put(ClientServerSpec.API_KEY_FETCH_REQUEST, ClientServerSpec.AVRO_SCHEMA_NAME_FETCH_REQUEST);
        apiMap.put(ClientServerSpec.API_KEY_FETCH_RESPONSE, ClientServerSpec.AVRO_SCHEMA_NAME_FETCH_RESPONSE);
    }


    public String getSchemaName(short apiKey)
    {
        return this.apiMap.get(apiKey);
    }

    public Schema getSchema(short apiKey)
    {
        String schemaName = this.getSchemaName(apiKey);

        return avroSchemaBuilder.getSchema(schemaName);
    }

}
