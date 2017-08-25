package io.shunters.coda.util;

import io.shunters.coda.store.StoreManager;
import org.apache.avro.Schema;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by mykidong on 2017-08-24.
 */
public class AvroSchemaBuilder {

    private static Logger log = LoggerFactory.getLogger(AvroSchemaBuilder.class);

    public static final String DEFAULT_AVRO_SCHEMA_DIR_PATH = "/META-INF/avro";

    private Map<String, Schema> schemas = new HashMap<>();

    private static final Object lock = new Object();

    private static AvroSchemaBuilder avroSchemaBuilder;

    public static AvroSchemaBuilder singleton(String pathDir)
    {
        if(avroSchemaBuilder == null)
        {
            synchronized (lock)
            {
                if(avroSchemaBuilder == null)
                {
                    avroSchemaBuilder = new AvroSchemaBuilder(pathDir);
                }
            }
        }
        return avroSchemaBuilder;
    }

    public static AvroSchemaBuilder singletonForSchemaPaths(String... schemaPaths)
    {
        if(avroSchemaBuilder == null)
        {
            synchronized (lock)
            {
                if(avroSchemaBuilder == null)
                {
                    avroSchemaBuilder = new AvroSchemaBuilder(schemaPaths);
                }
            }
        }
        return avroSchemaBuilder;
    }


    private AvroSchemaBuilder(String... schemaPaths) {

        List<String> jsonList = new ArrayList<>();
        for (String schemaPath : schemaPaths) {
            String json = fileToString(schemaPath);

            jsonList.add(json);
        }

        resolveSchemaRepeatedly(jsonList);
    }

    private AvroSchemaBuilder(String pathDir)
    {
        try {
            List<String> files = IOUtils.readLines(this.getClass().getResourceAsStream(pathDir), Charsets.UTF_8);

            List<String> jsonList = new ArrayList<>();

            files.stream().forEach(f -> {
                String path = pathDir + "/" + f;

                String json = fileToString(path);

                jsonList.add(json);
            });

            resolveSchemaRepeatedly(jsonList);

        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void resolveSchemaRepeatedly(List<String> jsonList) {
        List<String> unresolvedSchemaList = this.putSchemaToMap(jsonList);
        log.info("unresolvedSchemaList: [" + unresolvedSchemaList.size() + "]");

        while (unresolvedSchemaList.size() > 0)
        {
            unresolvedSchemaList  = this.putSchemaToMap(unresolvedSchemaList);
            log.info("unresolvedSchemaList: [" + unresolvedSchemaList.size() + "]");
        }
    }


    private List<String> putSchemaToMap(List<String> jsonList) {
        List<String> unresolvedSchemaList = new ArrayList<>();

        for(String json : jsonList) {
            try {
                String completeSchema = resolveSchema(json);
                Schema schema = new Schema.Parser().parse(completeSchema);
                String name = schema.getFullName();

                log.info("schema: " + name + "\n" + schema.toString(true));
                log.info("\n");

                schemas.put(name, schema);
            }catch (RuntimeException e)
            {
                unresolvedSchemaList.add(json);
            }
        }

        return unresolvedSchemaList;
    }


    private String fileToString(String filePath) {
        try (InputStream inputStream = this.getClass().getResourceAsStream(filePath)) {
            return IOUtils.toString(inputStream);
        } catch (IOException ie) {
            throw new RuntimeException(ie);
        }
    }


    public Schema getSchema(String name) {
        return schemas.get(name);
    }

    public Map<String, Schema> getSchemas() {
        return schemas;
    }

    private String resolveSchema(String json) {
        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            json = json.replaceAll("\"" + entry.getKey() + "\"", entry.getValue().toString());
        }

        return json;
    }
}
