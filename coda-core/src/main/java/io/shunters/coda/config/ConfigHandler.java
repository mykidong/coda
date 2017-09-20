package io.shunters.coda.config;

/**
 * Created by mykidong on 2017-09-06.
 */
public interface ConfigHandler {

    public static final String CONFIG_DATA_DIRS = "data.dirs";
    public static final String CONFIG_DATA_SEGMENT_MAX_BYTES = "data.segment.maxBytes";

    public static final String CONFIG_CONSUL_AGENT_HOST = "consul.agent.host";
    public static final String CONFIG_CONSUL_AGENT_PORT = "consul.agent.port";

    public static final String CONFIG_LOG4J_XML_PATH = "log4j.xml.path";


    Object get(String key);
}
