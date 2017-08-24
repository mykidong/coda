package io.shunters.coda.util;

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

    private Map<String, Schema> schemas = new HashMap<>();


    public AvroSchemaBuilder(String... schemaPaths) {

        List<String> jsonList = new ArrayList<>();
        for (String schemaPath : schemaPaths) {
            String json = fileToString(schemaPath);

            jsonList.add(json);
        }

        resolveSchemaRecursively(jsonList);
    }

    public AvroSchemaBuilder(String pathDir)
    {
        try {
            List<String> files = IOUtils.readLines(this.getClass().getResourceAsStream(pathDir), Charsets.UTF_8);

            List<String> jsonList = new ArrayList<>();

            files.stream().forEach(f -> {
                String path = pathDir + "/" + f;

                String json = fileToString(path);

                jsonList.add(json);
            });

            resolveSchemaRecursively(jsonList);

        }catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void resolveSchemaRecursively(List<String> jsonList) {
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
