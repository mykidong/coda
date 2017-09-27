package io.shunters.coda.meta;

import io.shunters.coda.discovery.ServiceDiscovery;

import java.util.List;

/**
 * Created by mykidong on 2017-09-25.
 */
public interface Controller {

    boolean isController();

    boolean isLeader(String topicName, int partition);

    List<ServiceDiscovery.ServiceNode> getBrokerList();

    Metadata getMetadata();

    void shutdown();


    public static class Metadata
    {
        private List<ServiceDiscovery.ServiceNode> brokerList;

        private List<TopicMetadata> topicMetadataList;

        private int numberOfPartitions;

        public List<ServiceDiscovery.ServiceNode> getBrokerList() {
            return brokerList;
        }

        public void setBrokerList(List<ServiceDiscovery.ServiceNode> brokerList) {
            this.brokerList = brokerList;
        }

        public List<TopicMetadata> getTopicMetadataList() {
            return topicMetadataList;
        }

        public void setTopicMetadataList(List<TopicMetadata> topicMetadataList) {
            this.topicMetadataList = topicMetadataList;
        }

        public int getNumberOfPartitions() {
            return numberOfPartitions;
        }

        public void setNumberOfPartitions(int numberOfPartitions) {
            this.numberOfPartitions = numberOfPartitions;
        }
    }

    public static class TopicMetadata
    {
        private String topicName;

        private List<PartitionMetadata> partitionMetadataList;

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public List<PartitionMetadata> getPartitionMetadataList() {
            return partitionMetadataList;
        }

        public void setPartitionMetadataList(List<PartitionMetadata> partitionMetadataList) {
            this.partitionMetadataList = partitionMetadataList;
        }
    }

    public static class PartitionMetadata
    {
        private  int partition;

        private int leader;

        private List<Integer> replicas;

        private List<Integer> isr;

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public int getLeader() {
            return leader;
        }

        public void setLeader(int leader) {
            this.leader = leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }

        public void setReplicas(List<Integer> replicas) {
            this.replicas = replicas;
        }

        public List<Integer> getIsr() {
            return isr;
        }

        public void setIsr(List<Integer> isr) {
            this.isr = isr;
        }
    }
}
