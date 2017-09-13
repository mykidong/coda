package io.shunters.coda.api;

import io.shunters.coda.protocol.AvroSchemaLoader;
import io.shunters.coda.protocol.AvroSchemaLoaderTest;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Created by mykidong on 2017-08-27.
 */
public class BaseRequestTest {

    protected AvroSchemaLoader avroSchemaBuilder;

    public BaseRequestTest()
    {
        java.net.URL url = new AvroSchemaLoaderTest().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);
        String pathDir = "/META-INF/avro";
        avroSchemaBuilder = AvroSchemaLoader.singleton(pathDir);
    }
}
