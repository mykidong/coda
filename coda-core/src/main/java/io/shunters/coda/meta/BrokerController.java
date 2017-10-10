package io.shunters.coda.meta;

import io.shunters.coda.config.ConfigHandler;
import io.shunters.coda.config.YamlConfigHandler;
import io.shunters.coda.discovery.ConsulServiceDiscovery;
import io.shunters.coda.discovery.ConsulSessionHolder;
import io.shunters.coda.discovery.ServiceDiscovery;
import io.shunters.coda.discovery.SessionHolder;
import io.shunters.coda.util.NetworkUtils;
import io.shunters.coda.util.RoundRobin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mykidong on 2017-09-25.
 */
public class BrokerController implements Controller {

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

    private SessionHolder controllerSessionHolder;

    private Metadata metadata;

    private List<Integer> currentBrokerIds;

    private List<Integer> lastBrokerIds;

    private int defaultNumberOfPartitions;

    private ConcurrentMap<String, Integer> topicMap = new ConcurrentHashMap<>();

    private ConcurrentMap<String, Integer> leaderPartitions = new ConcurrentHashMap<>();

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

        // run thread for electing controller.
        new Thread(this::electController).start();

        // run thread for updating metadata.
        new Thread(this::updateMetadata).start();
    }

    private void updateMetadata() {
        while (!shutdown) {
            this.loadMetadata();
            log.info("metadata updated...");

            this.reassignMetadata();
            log.info("metadata reassigned...");

            try {
                Thread.sleep(interval / 2 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();

                continue;
            }
        }
    }


    private void electController() {
        while (!shutdown) {
            // get controller leader.
            Map<String, String> leaderMap = serviceDiscovery.getLeader(ServiceDiscovery.KEY_SERVICE_CONTROLLER_LEADER);

            if (leaderMap != null) {
                for (String k : leaderMap.keySet()) {
                    String value = leaderMap.get(k);
                    if (value != null) {
                        int brokerIdToken = Integer.valueOf(value.split("-")[0]);
                        log.info("current broker id: {}, controller leader broker id: {}", brokerId, brokerIdToken);

                        if (brokerIdToken == brokerId) {
                            this.isController = true;
                        } else {
                            this.isController = false;
                        }

                        break;
                    }
                }
            } else {
                this.isController = false;
            }

            log.info("current broker is controller leader: {}", this.isController);

            try {
                Thread.sleep(interval / 2 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();

                continue;
            }
        }
    }


    @Override
    public boolean isController() {
        return this.isController;
    }

    @Override
    public List<ServiceDiscovery.ServiceNode> getBrokerList() {
        return this.serviceDiscovery.getHealthServices(ServiceDiscovery.SERVICE_CONTROLLER);
    }

    public boolean isLeader(String topicName, int partition) {
        // check, if topic partition is new or not. if so, create metadata first.
        addMetadataIfNotExists(topicName, partition);

        // if current broker is the leader for the topic partition, produce / fetch request is acceptable,
        // but if not, Not Leader Exception thrown will be responded to the client.
        String leaderKey = this.makeLeaderKey(topicName, partition);

        return leaderPartitions.containsKey(leaderKey);
    }

    private void addMetadataIfNotExists(String topicName, int partition) {
        String leaderKey = this.makeLeaderKey(topicName, partition);
        String isrKey = this.makeIsrKey(topicName, partition);
        String replicasKey = this.makeReplicasKey(topicName, partition);

        if (!leaderPartitions.containsKey(leaderKey)) {
            if (!topicMap.containsKey(topicName)) {
                // add topic with number of partitions to topic list in consul.
                String topicList = this.serviceDiscovery.getKVValue(ServiceDiscovery.KEY_TOPIC_LIST);
                if (topicList != null) {
                    topicList += "," + topicName + "-" + this.defaultNumberOfPartitions;
                } else {
                    topicList = topicName + "-" + this.defaultNumberOfPartitions;
                }
                this.serviceDiscovery.setKVValue(ServiceDiscovery.KEY_TOPIC_LIST, topicList);

                topicMap.put(topicName, this.defaultNumberOfPartitions);
            }

            // add leader.
            this.serviceDiscovery.setKVValue(leaderKey, String.valueOf(brokerId));


            // partition replication factor.
            int partitionReplicationFactor = (Integer) configHandler.get(ConfigHandler.CONFIG_PARTITION_REPLICATION_FACTOR);

            List<ServiceDiscovery.ServiceNode> brokerList = this.getBrokerList();

            // add replicas.
            String replicas = makeReplicas(brokerList, brokerId, partitionReplicationFactor);
            this.serviceDiscovery.setKVValue(replicasKey, replicas);

            // add isr.
            String isr = replicas; // first initial isr is the same as replicas.
            this.serviceDiscovery.setKVValue(isrKey, isr);


            // update metadata.
            this.loadMetadata();
        }
    }

    private RoundRobin makeBrokerListRoundRobin(List<ServiceDiscovery.ServiceNode> brokerList) {
        List<RoundRobin.Robin> robinList = new ArrayList<>();
        for (ServiceDiscovery.ServiceNode broker : brokerList) {
            int tempBrokerId = broker.getBrokerId();
            robinList.add(new RoundRobin.Robin(tempBrokerId));
        }

        // sort broker list in ascending order.
        Collections.sort(robinList, (r1, r2) -> r1.call() - r2.call());

        return new RoundRobin(robinList);
    }

    private int newLeader(List<Integer> isr, int oldLeader) {
        List<RoundRobin.Robin> robinList = new ArrayList<>();
        for (int tempBrokerId : isr) {
            robinList.add(new RoundRobin.Robin(tempBrokerId));
        }

        // sort broker list in ascending order.
        Collections.sort(robinList, (r1, r2) -> r1.call() - r2.call());

        RoundRobin roundRobin = new RoundRobin(robinList);

        int nextLeader = -1;

        while (true) {
            int tempBrokerId = roundRobin.next();
            if (tempBrokerId == oldLeader) {
                nextLeader = roundRobin.next();
                break;
            }
        }

        return nextLeader;
    }

    private String makeReplicas(List<ServiceDiscovery.ServiceNode> brokerList, int brokerId, int partitionReplicationFactor) {
        RoundRobin roundRobin = makeBrokerListRoundRobin(brokerList);

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

        return sb.toString();
    }

    public void reassignMetadata() {
        // if this broker is controller, reassign brokers for partition leader, isr, replicas.
        if (isController) {
            if (this.lastBrokerIds != null) {

                List<ServiceDiscovery.ServiceNode> brokerList = this.getBrokerList();

                int partitionReplicationFactor = (Integer) configHandler.get(ConfigHandler.CONFIG_PARTITION_REPLICATION_FACTOR);

                int lastBrokerSize = this.lastBrokerIds.size();
                int currentBrokerSize = this.currentBrokerIds.size();

                // if brokers fail.
                if (currentBrokerSize < lastBrokerSize) {

                    List<Integer> failedBrokerIds = new ArrayList<>();
                    for (int tempBrokerId : this.lastBrokerIds) {
                        if (!this.currentBrokerIds.contains(tempBrokerId)) {
                            failedBrokerIds.add(tempBrokerId);
                        }
                    }

                    // if a broker failed, controller will reassign brokers for partition leader, isr, replicas.

                    // refresh leader partition map first.
                    this.leaderPartitions = new ConcurrentHashMap<>();

                    // controller will reassign brokers for partition leader, isr, replicas.

                    // update metadata on local first.
                    this.metadata.setBrokerList(brokerList);

                    List<TopicMetadata> topicMetadataList = this.metadata.getTopicMetadataList();
                    for (TopicMetadata topicMetadata : topicMetadataList) {
                        String topicName = topicMetadata.getTopicName();


                        List<PartitionMetadata> partitionMetadataList = topicMetadata.getPartitionMetadataList();
                        for (PartitionMetadata partitionMetadata : partitionMetadataList) {
                            int partition = partitionMetadata.getPartition();
                            int oldLeader = partitionMetadata.getLeader();
                            List<Integer> oldIsr = partitionMetadata.getIsr();

                            int leader = oldLeader;
                            // if this old leader is a failed broker, elect new leader from the old isr list.
                            if (failedBrokerIds.contains(oldLeader)) {
                                leader = newLeader(oldIsr, oldLeader);
                            }

                            // replica string line.
                            String replicaStr = makeReplicas(brokerList, leader, partitionReplicationFactor);

                            List<Integer> isr = new ArrayList<>();
                            for (String replica : replicaStr.split(",")) {
                                isr.add(Integer.valueOf(replica));
                            }

                            // init. replicas is the same as isr.
                            List<Integer> replicas = isr;

                            partitionMetadata.setPartition(partition);
                            partitionMetadata.setLeader(leader);
                            partitionMetadata.setIsr(isr);
                            partitionMetadata.setReplicas(replicas);

                            // update metadata onto remote consul.
                            String leaderKey = this.makeLeaderKey(topicName, partition);
                            String isrKey = this.makeIsrKey(topicName, partition);
                            String replicasKey = this.makeReplicasKey(topicName, partition);

                            // update leader.
                            this.serviceDiscovery.setKVValue(leaderKey, String.valueOf(leader));

                            // update replicas.
                            this.serviceDiscovery.setKVValue(replicasKey, replicaStr);

                            // update isr.
                            this.serviceDiscovery.setKVValue(isrKey, replicaStr);


                            // if current broker is leader for this topic partition.
                            if (leader == brokerId) {
                                leaderPartitions.put(leaderKey, leader);
                            }
                        }
                    }

                }
                // if new brokers are added.
                else if (currentBrokerSize > lastBrokerSize) {
                    List<Integer> addedBrokerIds = new ArrayList<>();
                    for (int tempBrokerId : this.currentBrokerIds) {
                        if (!this.lastBrokerIds.contains(tempBrokerId)) {
                            addedBrokerIds.add(tempBrokerId);
                        }
                    }

                    // TODO: if new brokers are added.
                }
            }
        }
    }

    private String makeLeaderKey(String topicName, int partition) {
        return ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_LEADER_SURFIX;
    }

    private String makeIsrKey(String topicName, int partition) {
        return ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_ISR_SURFIX;
    }

    private String makeReplicasKey(String topicName, int partition) {
        return ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_PREFIX + topicName + "/" + partition + ServiceDiscovery.KEY_TOPIC_PARTITION_BROKER_REPLICAS_SURFIX;
    }

    private void loadMetadata() {
        reentrantLock.lock();
        try {
            // number of partitions.
            defaultNumberOfPartitions = (Integer) configHandler.get(ConfigHandler.CONFIG_NUMBER_PARTITIONS);

            // current broker list.
            List<ServiceDiscovery.ServiceNode> brokerList = getBrokerList();

            // update broker ids.
            // move the current broker id list to the old last broker id list.
            if (this.currentBrokerIds != null) {
                this.lastBrokerIds = new ArrayList<>();
                this.lastBrokerIds.addAll(this.currentBrokerIds);
            }

            // update current broker id list.
            this.currentBrokerIds = new ArrayList<>();
            for (ServiceDiscovery.ServiceNode broker : brokerList) {
                int tempBrokerId = broker.getBrokerId();
                currentBrokerIds.add(tempBrokerId);
            }

            // get topic list.
            // topic list convention: [topic-name]-[number-of-partitions]
            // delimiter is comma(,)
            // example: user-2,event-2,item-2
            String topicList = this.serviceDiscovery.getKVValue(ServiceDiscovery.KEY_TOPIC_LIST);

            this.topicMap = new ConcurrentHashMap<>();

            if (topicList != null) {
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
            this.leaderPartitions = new ConcurrentHashMap<>();

            for (String topicName : topicMap.keySet()) {
                int numberOfPartitions = topicMap.get(topicName);

                List<PartitionMetadata> partitionMetadataList = new ArrayList<>();

                for (int partition = 0; partition < numberOfPartitions; partition++) {

                    String leaderKey = this.makeLeaderKey(topicName, partition);
                    String isrKey = this.makeIsrKey(topicName, partition);
                    String replicasKey = this.makeReplicasKey(topicName, partition);

                    String leaderRet = serviceDiscovery.getKVValue(leaderKey);
                    // if leader for the current partition does not exist, continue to next partition.
                    if (leaderRet == null) {
                        continue;
                    }

                    int leader = Integer.valueOf(leaderRet);

                    // if current broker is leader for this topic partition.
                    if (leader == brokerId) {
                        leaderPartitions.put(leaderKey, leader);
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
        } finally {
            reentrantLock.unlock();
        }
    }


    @Override
    public Metadata getMetadata() {
        return metadata;
    }


    @Override
    public void shutdown() {
        this.controllerSessionHolder.shutdown();
        shutdown = true;
    }
}
