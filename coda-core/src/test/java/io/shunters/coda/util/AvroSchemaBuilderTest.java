package io.shunters.coda.util;

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
public class AvroSchemaBuilderTest {

    private static Logger log;

    @Before
    public void init() throws Exception {
        java.net.URL url = new AvroSchemaBuilderTest().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        log = LoggerFactory.getLogger(AvroSchemaBuilder.class);
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

        AvroSchemaBuilder avroSchemaBuilder = AvroSchemaBuilder.singletonForSchemaPaths((String[])pathList.toArray(new String[0]));

        String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";
        Schema schema = avroSchemaBuilder.getSchema(schemaKey);
        System.out.println("schema key: [" + schemaKey + "]\n" + schema.toString(true));
    }

    @Test
    public void loadSchemasWithPathDir() throws Exception
    {
        String pathDir = "/META-INF/avro";

        AvroSchemaBuilder avroSchemaBuilder = AvroSchemaBuilder.singleton(pathDir);
        String schemaKey = "io.shunters.coda.avro.api.ProduceRequest";
        Schema schema = avroSchemaBuilder.getSchema(schemaKey);
        log.info("schema key: [" + schemaKey + "]\n" + schema.toString(true));
    }
}
