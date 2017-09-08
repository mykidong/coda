package io.shunters.coda.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Created by mykidong on 2017-09-06.
 */
public class YamlConfigHandler implements ConfigHandler {

    public static final String DEFAULT_CONFIG_PATH = "/config/coda.yml";

    private Map<String, Object> configMap;

    private static final Object lock = new Object();

    private static ConfigHandler configHandler;

    public static ConfigHandler getConfigHandler() {
        return YamlConfigHandler.singleton(DEFAULT_CONFIG_PATH);
    }

    public static ConfigHandler singleton(String configPath) {
        if (configHandler == null) {
            synchronized (lock) {
                if (configHandler == null) {
                    configHandler = new YamlConfigHandler(configPath);
                }
            }
        }
        return configHandler;
    }


    private YamlConfigHandler(String configPath) {
        java.net.URL url = this.getClass().getResource(configPath);

        try {
            Yaml yaml = new Yaml();
            Map<String, Object> yamlMap = (Map<String, Object>) yaml.load(url.openStream());

            configMap = Collections.unmodifiableMap(yamlMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public Object get(String key) {

        if (this.configMap.containsKey(key)) {
            return this.configMap.get(key);
        } else {
            return null;
        }
    }
}
