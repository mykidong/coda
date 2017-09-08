package io.shunters.coda.config;

/**
 * Created by mykidong on 2017-09-06.
 */
public interface ConfigHandler {

    public static final String CONFIG_DATA_DIRS = "data.dirs";
    public static final String CONFIG_DATA_SEGMENT_MAX_BYTES = "data.segment.maxBytes";

    Object get(String key);
}
