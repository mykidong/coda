package io.shunters.coda.protocol;

import org.apache.avro.Schema;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2017-08-24.
 */
public class AvroSchemaLoaderTest {

    private static Logger log;

    @Before
    public void init() throws Exception {
        java.net.URL url = new AvroSchemaLoaderTest().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        log = LoggerFactory.getLogger(AvroSchemaLoader.class);
    }

    @Test
    public void loadSchemas() throws Exception
    {
        List<String> pathList = new ArrayList<>();
        pathList.add("/META-INF/avro/request-header.avsc");
        pathList.add("/META-INF/avro/record-header.avsc");
        pathList.add("/META-INF/avro/record.avsc");
        pathList.add("/META-INF/avro/records.avsc");
        pathList.add("/META-INF/avro/produce-request.avsc");

        AvroSchemaLoader avroSchemaLoader = AvroSchemaLoader.singletonForSchemaPaths((String[])pathList.toArray(new String[0]));

        String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";
        Schema schema = avroSchemaLoader.getSchema(schemaKey);
        System.out.println("schema key: [" + schemaKey + "]\n" + schema.toString(true));
    }

    @Test
    public void loadSchemasWithPathDir() throws Exception
    {
        String pathDir = "/META-INF/avro";

        AvroSchemaLoader avroSchemaLoader = AvroSchemaLoader.singleton(pathDir);
        String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";
        Schema schema = avroSchemaLoader.getSchema(schemaKey);
        log.info("schema key: [" + schemaKey + "]\n" + schema.toString(true));
    }
}
