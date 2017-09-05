package io.shunters.coda.processor;

import org.apache.avro.generic.GenericRecord;

/**
 * Created by mykidong on 2017-09-05.
 */
public interface RequestHandler {

    public void handleAndResponse(String channelId, NioSelector nioSelector, GenericRecord requestRecord);
}
