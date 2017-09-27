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

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

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

    private int interval = 10;

    private int ttl = 10;

    private SessionHolder controllerSessionHolder;

    private Metadata metadata;

    private int defaultNumberOfPartitions;

    private Set<String> leaderPartitions = new HashSet<>();

    private final ReentrantLock reentrantLock = new ReentrantLock();

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

        // load metadata.
        this.loadMetadata();
    }


    @Override
    public boolean isController() {
        return this.isController;
    }

    @Override
    public List<ServiceDiscovery.ServiceNode> getBrokerList() {
        return this.serviceDiscovery.getHealthServices(ServiceDiscovery.SERVICE_CONTROLLER);
    }

    public boolean isLeader(String topicName, int partition)
    {
        // check, if topic partition is new or not. if so, create metadata first.
        addMetadataIfNotExists(topicName, partition);

        // if current broker is the leader for the topic partition, produce / fetch request is acceptable,
        // but if not, Not Leader Exception thrown will be responded to the client.
        String leaderKey = ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_LEADER_SURFIX;

        return leaderPartitions.contains(leaderKey);
    }

    private void addMetadataIfNotExists(String topicName, int partition)  {
        String leaderKey = ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_LEADER_SURFIX;
        String isrKey = ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_ISR_SURFIX;
        String replicasKey = ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_REPLICAS_SURFIX;

        if(!leaderPartitions.contains(leaderKey))
        {
            // add topic with number of partitions to topic list in consul.
            String topicList = this.serviceDiscovery.getKVValue(ServiceDiscovery.KEY_TOPIC_LIST);
            if(topicList != null)
            {
                topicList += "," + topicName + "-" + this.defaultNumberOfPartitions;
            }
            else
            {
                topicList = topicName + "-" + this.defaultNumberOfPartitions;
            }
            this.serviceDiscovery.setKVValue(ServiceDiscovery.KEY_TOPIC_LIST, topicList);

            // TODO: add leader, isr, replicas for the topic partition into consul.
            // ...

            this.loadMetadata();
        }
    }

    public void reassignMetadata()
    {
        // TODO: if a broker failed, controller will reassign brokers for partition leader, isr, replicas.
    }

    private void loadMetadata()
    {
        reentrantLock.lock();
        try {
            // number of partitions.
            defaultNumberOfPartitions = (Integer) configHandler.get(ConfigHandler.CONFIG_NUMBER_PARTITIONS);

            // current broker list.
            List<ServiceDiscovery.ServiceNode> brokerList = getBrokerList();

            // get topic list.
            // topic list convention: [topic-name]-[number-of-partitions]
            // delimiter is comma(,)
            // example: user-2,event-2,item-2
            String topicList = this.serviceDiscovery.getKVValue(ServiceDiscovery.KEY_TOPIC_LIST);

            Map<String, Integer> topicMap = null;
            if (topicList != null) {
                topicMap = new HashMap<>();

                String[] topicTokens = topicList.split(",");

                for (String topicToken : topicTokens) {
                    String[] topicPartitions = topicToken.split("-");

                    String topicName = topicPartitions[0];
                    int partitions = Integer.valueOf(topicPartitions[1]);

                    topicMap.put(topicName, partitions);
                }
            } else {
                this.metadata = null;

                return;
            }

            List<TopicMetadata> topicMetadataList = new ArrayList<>();
            this.leaderPartitions = new HashSet<>();

            for (String topicName : topicMap.keySet()) {
                int numberOfPartitions = topicMap.get(topicName);

                List<PartitionMetadata> partitionMetadataList = new ArrayList<>();

                for (int partition = 0; partition < numberOfPartitions; partition++) {

                    String leaderKey = ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_LEADER_SURFIX;
                    String isrKey = ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_ISR_SURFIX;
                    String replicasKey = ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_REPLICAS_SURFIX;

                    int leader = Integer.valueOf(serviceDiscovery.getKVValue(leaderKey));

                    // if current broker is leader for this topic partition.
                    if(leader == brokerId)
                    {
                        leaderPartitions.add(leaderKey);
                    }


                    String isrStr = serviceDiscovery.getKVValue(isrKey);
                    List<Integer> isr = new ArrayList<>();
                    for (String isrMember : isrStr.split(",")) {
                        isr.add(Integer.valueOf(isrMember));
                    }

                    String replicaStr = serviceDiscovery.getKVValue(replicasKey);
                    List<Integer> replicas = new ArrayList<>();
                    for (String replicaMember : replicaStr.split(",")) {
                        replicas.add(Integer.valueOf(replicaMember));
                    }

                    PartitionMetadata partitionMetadata = new PartitionMetadata();
                    partitionMetadata.setPartition(partition);
                    partitionMetadata.setLeader(leader);
                    partitionMetadata.setIsr(isr);
                    partitionMetadata.setReplicas(replicas);

                    partitionMetadataList.add(partitionMetadata);
                }

                TopicMetadata topicMetadata = new TopicMetadata();
                topicMetadata.setTopicName(topicName);
                topicMetadata.setPartitionMetadataList(partitionMetadataList);

                topicMetadataList.add(topicMetadata);
            }

            metadata.setNumberOfPartitions(defaultNumberOfPartitions);
            metadata.setBrokerList(brokerList);
            metadata.setTopicMetadataList(topicMetadataList);
        }finally {
            reentrantLock.unlock();
        }
    }



    @Override
    public Metadata getMetadata() {
        return metadata;
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
