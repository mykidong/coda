package io.shunters.coda.api;

import io.shunters.coda.util.AvroSchemaBuilder;
import io.shunters.coda.util.AvroSchemaBuilderTest;
import org.apache.avro.Schema;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;

/**
 * Created by mykidong on 2017-08-27.
 */
public class BaseRequestTest {

    protected AvroSchemaBuilder avroSchemaBuilder;

    public BaseRequestTest()
    {
        java.net.URL url = new AvroSchemaBuilderTest().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);
        String pathDir = "/META-INF/avro";
        avroSchemaBuilder = AvroSchemaBuilder.singleton(pathDir);
    }
}
