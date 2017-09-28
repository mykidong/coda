package io.shunters.coda.discovery;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by mykidong on 2017-09-13.
 */
public interface ServiceDiscovery {

    public static final String SERVICE_CONTROLLER = "controller";
    public static final String KEY_SERVICE_CONTROLLER_LEADER = "service/" + SERVICE_CONTROLLER + "/leader";
    public static final String SESSION_LOCK_SERVICE_CONTROLLER = SERVICE_CONTROLLER + "-lock";

    public static final String KEY_TOPIC_PREFIX = "topic/";
    public static final String KEY_TOPIC_LIST = KEY_TOPIC_PREFIX + "list";

    /**
     * topic partition broker leader / isr / replicas key surfix.
     *
     * topic partition broker key convention for leader, isr, replicas:
     *      broker/[topic-name]/[partition]/leader
     *                                     /isr
     *                                     /replicas
     */
    public static final String KEY_TOPIC_PARTITION_BROKER_PREFIX = "broker/";
    public static final String KEY_TOPIC_PARTITION_BROKER_LEADER_SURFIX = "/leader";
    public static final String KEY_TOPIC_PARTITION_BROKER_ISR_SURFIX = "/isr";
    public static final String KEY_TOPIC_PARTITION_BROKER_REPLICAS_SURFIX = "/replicas";



    void createService(String serviceName, String id, List<String> tags, String address, int port, String script, String tcp, String interval, String timeout);

    List<ServiceNode> getHealthServices(String path);

    Set<String> getKVKeysOnly(String keyPath);

    String getKVValue(String key);

    Map<String, String> getKVValues(String keyPath);

    Map<String, String> getLeader(String keyPath);

    void setKVValue(String key, String value);

    void deleteKVValue(String key);

    void deleteKVValuesRecursively(String key);

    String createSession(String name, String node, String ttl, long lockDelay);

    void renewSession(String session);

    boolean acquireLock(String key, String value, String session);

    void destroySession(String session);

    public static class ServiceNode
    {
        private String id;
        private String host;
        private int port;

        public ServiceNode(String id, String host, int port)
        {
            this.id = id;
            this.host = host;
            this.port = port;
        }

        public String getId() {
            return id;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public static String describe(int id, String host, int port)
        {
            return id + "-" + host + "-" + port;
        }

        public int getBrokerId()
        {
            return Integer.valueOf(this.id.split("-")[0]);
        }
    }
}
