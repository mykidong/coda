package io.shunters.coda.discovery;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

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
    public void getLeader() {
        // get leader.
        Map<String, String> leaderMap = serviceDiscovery.getLeader(key);
        if (leaderMap != null) {
            for (String k : leaderMap.keySet()) {
                String value = leaderMap.get(k);
                System.out.println("leader: " + value);
            }
        } else {
            System.out.println("leader not found!");
        }
    }

    @Test
    public void getHealthService() {
        List<ServiceDiscovery.HostPort> healthServiceList = this.serviceDiscovery.getHealthServices(ServiceDiscovery.SERVICE_NAME);

        healthServiceList.stream().forEach(h -> System.out.printf("health service host: %s, port: %d\n", h.getHost(), h.getPort()));
    }

}
