package io.shunters.coda.config;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by mykidong on 2017-09-06.
 */
public class YamlConfigHandlerTestSkip {

    @Test
    public void loadYamlConfig() throws IOException {
        ConfigHandler configHandler = YamlConfigHandler.getConfigHandler();

        List<String> dataDirs = (List<String>) configHandler.get(ConfigHandler.CONFIG_DATA_DIRS);
        System.out.println(dataDirs);

        Object segmentMaxBytesObj = configHandler.get(ConfigHandler.CONFIG_DATA_SEGMENT_MAX_BYTES);
        long segmentMaxBytes = 0L;
        if(segmentMaxBytesObj instanceof Long)
        {
            segmentMaxBytes = (Long) segmentMaxBytesObj;
        }
        else if(segmentMaxBytesObj instanceof Integer)
        {
            segmentMaxBytes = ((Integer) segmentMaxBytesObj).longValue();
        }

        System.out.println(segmentMaxBytes);
    }
}
