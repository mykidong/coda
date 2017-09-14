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
    private String key = "service/" + ServiceDiscovery.SERVICE_NAME + "/leader";

    @Before
    public void init() {
        serviceDiscovery = ConsulServiceDiscovery.getConsulServiceDiscovery();
    }


    @Test
    public void createSession() throws Exception {
        // before creating session, make sure that ServerTestSkip is run with
        // mvn -e -Dtest=ServerTestSkip -Dport=9911 test;
        // mvn -e -Dtest=ServerTestSkip -Dport=9912 test;
        // mvn -e -Dtest=ServerTestSkip -Dport=9913 test;

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

        // create session and acquire lock.
        for (int i = 0; i < 3; i++) {
            String hostName = NetworkUtils.getSimpleHostName();
            int port = 9911 + i;
            executor.execute(new ServiceDiscoveryTask(serviceDiscovery, key, hostName, port));
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    private static class ServiceDiscoveryTask implements Runnable {
        private ServiceDiscovery serviceDiscovery;
        private String key;
        private String hostName;
        private int port;

        public ServiceDiscoveryTask(ServiceDiscovery serviceDiscovery, String key, String hostName, int port) {
            this.serviceDiscovery = serviceDiscovery;
            this.key = key;
            this.hostName = hostName;
            this.port = port;
        }

        @Override
        public void run() {
            String session = serviceDiscovery.createSession(ServiceDiscovery.SESSION_CODA_LOCK, hostName, "10s", 10);
            System.out.printf("thread id: %d, session: %s\n", Thread.currentThread().getId(), session);

            String nodeDescription = this.hostName + ":" + this.port;
            boolean lockAcquired = serviceDiscovery.acquireLock(key, nodeDescription, session);

            System.out.printf("thread id: %d, node desc: %s, lock acquired: %s\n", Thread.currentThread().getId(), nodeDescription, String.valueOf(lockAcquired));
        }
    }

    @Test
    public void getLeader() {
        // get leader.
        Map<String, String> leaderMap = serviceDiscovery.getLeader(key);
        if(leaderMap != null) {
            for (String k : leaderMap.keySet()) {
                String value = leaderMap.get(k);
                System.out.println("leader: " + value);
            }
        }else
        {
            System.out.println("leader not found!");
        }
    }

    @Test
    public void getHealthService() {
        List<ServiceDiscovery.HostPort> healthServiceList = this.serviceDiscovery.getHealthServices(ServiceDiscovery.SERVICE_NAME);

        healthServiceList.stream().forEach(h -> System.out.printf("health service host: %s, port: %d\n", h.getHost(), h.getPort()));
    }

}
