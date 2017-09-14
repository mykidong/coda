package io.shunters.coda.discovery;

import io.shunters.coda.server.CodaServer;
import io.shunters.coda.util.NetworkUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by mykidong on 2017-09-14.
 */
public class ConsulServiceDiscoveryTestSkip {

    private ServiceDiscovery serviceDiscovery;

    @Before
    public void init()
    {
        serviceDiscovery = ConsulServiceDiscovery.getConsulServiceDiscovery();
    }


    @Test
    public void electLeader() throws Exception {
        // before electing leader, make sure that ServerTestSkip is run with
        // mvn -e -Dtest=ServerTestSkip -Dport=9911 test;
        // mvn -e -Dtest=ServerTestSkip -Dport=9912 test;
        // mvn -e -Dtest=ServerTestSkip -Dport=9913 test;

        String key = "service/" + CodaServer.CONSUL_SERVICE_NAME + "/leader";

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

        // create session and acquire lock.
        for (int i = 0; i < 3; i++) {
            String hostIp = NetworkUtils.getHostIp();
            int port = 9911 + i;
            executor.execute(new ServiceDiscoveryTask(serviceDiscovery, key, hostIp, port));
        }

        Thread.sleep(Long.MAX_VALUE);


        // elect leader.
        Map<String, String> leaderMap = serviceDiscovery.getKVValues(key);
        for (String k : leaderMap.keySet()) {
            String value = leaderMap.get(k);
            System.out.println("leader: " + value);
        }
    }

    private static class ServiceDiscoveryTask implements Runnable {
        private ServiceDiscovery serviceDiscovery;
        private String key;
        private String hostIp;
        private int port;

        public ServiceDiscoveryTask(ServiceDiscovery serviceDiscovery, String key, String hostIp, int port) {
            this.serviceDiscovery = serviceDiscovery;
            this.key = key;
            this.hostIp = hostIp;
            this.port = port;
        }

        @Override
        public void run() {
            String session = serviceDiscovery.createSession(CodaServer.CONSUL_SERVICE_NAME, null, "10s", 10);

            String nodeDescription = this.hostIp + ":" + this.port;
            boolean lockAcquired = serviceDiscovery.acquireLock(key, nodeDescription, session);

            System.out.printf("node desc: %s, lock acquired: %s\n", nodeDescription, String.valueOf(lockAcquired));
        }
    }

    @Test
    public void getHealthService()
    {
        List<ServiceDiscovery.HostPort> healthServiceList = this.serviceDiscovery.getHealthServices(CodaServer.CONSUL_SERVICE_NAME);

        healthServiceList.stream().forEach(h -> System.out.printf("health service host: %s, port: %d\n", h.getHost(), h.getPort()));
    }

}
