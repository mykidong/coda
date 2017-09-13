package io.shunters.coda.discovery;

import io.shunters.coda.ConsumerOldClientTestSkip;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by mykidong on 2017-09-14.
 */
public class ConsulServiceDiscoveryTestSkip {

    @Test
    public void electLeader() throws Exception
    {
        ServiceDiscovery serviceDiscovery = ConsulServiceDiscovery.getConsulServiceDiscovery();

        String key = "service/coda/leader";

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

        for (int i = 0; i < 3; i++) {
            executor.execute(new ServiceDiscoveryTask(serviceDiscovery, key, i));
        }

        Thread.sleep(Long.MAX_VALUE);


        Map<String, String> leaderMap = serviceDiscovery.getKVValues(key);
        for(String k : leaderMap.keySet())
        {
            String value = leaderMap.get(k);
            System.out.println("leader: " + value);
        }
    }

    private static class ServiceDiscoveryTask implements Runnable
    {
        private ServiceDiscovery serviceDiscovery;
        private String key;
        private int index;

        public ServiceDiscoveryTask(ServiceDiscovery serviceDiscovery, String key, int index)
        {
            this.serviceDiscovery = serviceDiscovery;
            this.key = key;
            this.index = index;
        }

        @Override
        public void run() {
            String session = serviceDiscovery.createSession("coda", "node" + index);
            String nodeDescription = "node" + index + ":" + (9911 + index);
            boolean lockAcquired = serviceDiscovery.acquireLock(key, nodeDescription, session);

            System.out.printf("node desc: %s, lock acquired: %s\n", nodeDescription, String.valueOf(lockAcquired));
        }
    }

}
