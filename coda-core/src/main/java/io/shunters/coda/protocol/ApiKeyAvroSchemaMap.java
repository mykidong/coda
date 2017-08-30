package io.shunters.coda.protocol;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mykidong on 2017-08-25.
 */
public class ApiKeyAvroSchemaMap {

    private static ApiKeyAvroSchemaMap apiKeyAvroSchemaMap;

    private static final Object lock = new Object();

    private Map<Short, String> apiMap = new HashMap<>();

    public static ApiKeyAvroSchemaMap singleton()
    {
        if(apiKeyAvroSchemaMap == null)
        {
            synchronized (lock)
            {
                if(apiKeyAvroSchemaMap == null)
                {
                    apiKeyAvroSchemaMap = new ApiKeyAvroSchemaMap();
                }
            }
        }
        return apiKeyAvroSchemaMap;
    }

    private ApiKeyAvroSchemaMap()
    {
        apiMap.put(ClientServerSpec.API_KEY_PRODUCE_REQUEST, ClientServerSpec.AVRO_SCHEMA_NAME_PRODUCE_REQUEST);
    }


    public String getSchemaName(short apiKey)
    {
        return this.apiMap.get(apiKey);
    }

}
