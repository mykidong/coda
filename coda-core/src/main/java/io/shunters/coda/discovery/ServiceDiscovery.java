package io.shunters.coda.discovery;

import java.util.List;
import java.util.Map;

/**
 * Created by mykidong on 2017-09-13.
 */
public interface ServiceDiscovery {

    void createService(String serviceName, String id, List<String> tags, String address, int port, String script, String tcp, String interval, String timeout);

    List<HostPort> getHealthServices(String path);

    Map<String, String> getKVValues(String keyPath);

    void setKVValue(String key, String value);

    String createSession(String name, String node, String ttl, long lockDelay);

    boolean acquireLock(String key, String value, String session);

    void destroySession(String session);

    public static class HostPort
    {
        private String host;
        private int port;

        public HostPort(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}
