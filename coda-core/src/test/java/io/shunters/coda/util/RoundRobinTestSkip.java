package io.shunters.coda.util;

import io.shunters.coda.discovery.ServiceDiscovery;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by mykidong on 2017-09-28.
 */
public class RoundRobinTestSkip {

    @Test
    public void chooseReplicas() {
        int brokerId = 4;

        // partition replication factor.
        int partitionReplicationFactor = 3;

        List<ServiceDiscovery.ServiceNode> brokerList = new ArrayList<>();
        brokerList.add(new ServiceDiscovery.ServiceNode("3-localhost-9914", "localhost", 9914));
        brokerList.add(new ServiceDiscovery.ServiceNode("4-localhost-9915", "localhost", 9915));
        brokerList.add(new ServiceDiscovery.ServiceNode("0-localhost-9911", "localhost", 9911));
        brokerList.add(new ServiceDiscovery.ServiceNode("1-localhost-9912", "localhost", 9912));
        brokerList.add(new ServiceDiscovery.ServiceNode("2-localhost-9913", "localhost", 9913));


        List<RoundRobin.Robin> robinList = new ArrayList<>();
        for (ServiceDiscovery.ServiceNode broker : brokerList) {
            int tempBrokerId = broker.getBrokerId();
            robinList.add(new RoundRobin.Robin(tempBrokerId));
        }

        // sort broker list in ascending order.
        Collections.sort(robinList, (r1, r2) -> r1.call() - r2.call());

        RoundRobin roundRobin = new RoundRobin(robinList);

        boolean isFirstMatch = true;
        StringBuilder sb = new StringBuilder();
        int count = 0;
        while (true) {
            int tempBrokerId = roundRobin.next();
            if (isFirstMatch) {
                if (tempBrokerId == brokerId) {
                    sb.append(tempBrokerId);
                    isFirstMatch = false;
                } else {
                    continue;
                }
            } else {
                sb.append(tempBrokerId);
            }

            if (count != partitionReplicationFactor - 1) {
                sb.append(",");
                count++;
            } else {
                break;
            }
        }

        System.out.printf("replicas: [%s]\n", sb.toString());
    }
}
