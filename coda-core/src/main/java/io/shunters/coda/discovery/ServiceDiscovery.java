package io.shunters.coda.discovery;

import io.shunters.coda.server.CodaServer;

import java.util.List;
import java.util.Map;

/**
 * Created by mykidong on 2017-09-13.
 */
public interface ServiceDiscovery {

    public static final String SERVICE_NAME = "coda";

    public static final String LEADER_KEY = "service/" + SERVICE_NAME + "/leader";

    public static final String SESSION_CODA_LOCK = "coda-lock";

    void createService(String serviceName, String id, List<String> tags, String address, int port, String script, String tcp, String interval, String timeout);

    List<HostPort> getHealthServices(String path);

    Map<String, String> getKVValues(String keyPath);

    Map<String, String> getLeader(String keyPath);

    void setKVValue(String key, String value);

    String createSession(String name, String node, String ttl, long lockDelay);

    void renewSession(String session);

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
