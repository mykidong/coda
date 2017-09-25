package io.shunters.coda.meta;

import io.shunters.coda.config.ConfigHandler;
import io.shunters.coda.config.YamlConfigHandler;
import io.shunters.coda.discovery.ConsulServiceDiscovery;
import io.shunters.coda.discovery.ConsulSessionHolder;
import io.shunters.coda.discovery.ServiceDiscovery;
import io.shunters.coda.discovery.SessionHolder;
import io.shunters.coda.util.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by mykidong on 2017-09-25.
 */
public class BrokerController implements Controller, Runnable {

    private static Logger log = LoggerFactory.getLogger(BrokerController.class);

    private boolean isController;

    private ServiceDiscovery serviceDiscovery;

    private ConfigHandler configHandler;

    private static final Object lock = new Object();

    private static Controller controller;

    private int brokerId;

    private boolean shutdown = false;

    private int interval = 4;

    private int ttl = 10;

    private int numberOfPartitions;

    private SessionHolder controllerSessionHolder;

    public static Controller singleton(int port) {
        if (controller == null) {
            synchronized (lock) {
                if (controller == null) {
                    controller = new BrokerController(port);
                }
            }
        }
        return controller;
    }

    private BrokerController(int port) {
        configHandler = YamlConfigHandler.getConfigHandler();

        brokerId = (Integer) configHandler.get(ConfigHandler.CONFIG_BROKER_ID);

        numberOfPartitions = (Integer) configHandler.get(ConfigHandler.CONFIG_NUMBER_PARTITIONS);

        // register controller service onto consul.
        String hostName = NetworkUtils.getSimpleHostName();
        String hostPort = hostName + ":" + port;
        String serviceId = ServiceDiscovery.ServiceNode.describe(this.brokerId, hostName, port);

        this.serviceDiscovery = ConsulServiceDiscovery.getConsulServiceDiscovery();
        serviceDiscovery.createService(ServiceDiscovery.SERVICE_CONTROLLER, serviceId, null, hostName, port, null, hostPort, interval + "s", "1s");
        log.info("consul service [" + ServiceDiscovery.SERVICE_CONTROLLER + ":" + serviceId + "] registered.");

        // run consul session holder to elect controller leader.
        controllerSessionHolder = new ConsulSessionHolder(ServiceDiscovery.SESSION_LOCK_SERVICE_CONTROLLER, ServiceDiscovery.KEY_SERVICE_CONTROLLER_LEADER, brokerId, hostName, port, ttl);

        // run thread.
        Thread t = new Thread(this);
        t.start();

        log.info("broker controller started...");
    }


    @Override
    public boolean isController() {
        return this.isController;
    }

    @Override
    public List<ServiceDiscovery.ServiceNode> getBrokerList() {
        return this.serviceDiscovery.getHealthServices(ServiceDiscovery.SERVICE_CONTROLLER);
    }

    @Override
    public Metadata getMetadata() {

        // TODO:
        //      1. get broker list from consul.
        //      2. get partition meta data from consul.
        //      3. get default partition number.


        return null;
    }

    @Override
    public void run() {

        while (!shutdown)
        {
            // get controller leader.
            Map<String, String> leaderMap = serviceDiscovery.getLeader(ServiceDiscovery.KEY_SERVICE_CONTROLLER_LEADER);

            if (leaderMap != null) {
                for (String k : leaderMap.keySet()) {
                    String value = leaderMap.get(k);
                    if(value != null)
                    {
                        int brokerIdToken = Integer.valueOf(value.split("-")[0]);
                        log.info("current broker id: {}, controller leader broker id: {}", brokerId, brokerIdToken);

                        if(brokerIdToken == brokerId)
                        {
                            this.isController = true;
                        }
                        else
                        {
                            this.isController = false;
                        }

                        break;
                    }
                }
            }
            else {
                this.isController = false;
            }

            log.info("current broker is controller leader: {}", this.isController);


            try {
                Thread.sleep(interval / 2 * 1000);
            } catch (InterruptedException e)
            {
                e.printStackTrace();

                continue;
            }
        }

    }

    @Override
    public void shutdown()
    {
        this.controllerSessionHolder.shutdown();
        shutdown = true;
    }
}
