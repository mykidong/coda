package io.shunters.coda.offset;

/**
 * Created by mykidong on 2016-09-01.
 */
public class TopicPartition {

    private String topic;

    private int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public int hashCode() {
        return topic.hashCode() + this.partition;
    }

    @Override
    public boolean equals(Object o) {
        TopicPartition topicPartition = (TopicPartition) o;

        return (this.topic.equals(topicPartition.getTopic())) && (this.partition == topicPartition.getPartition());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("topic: ").append(this.topic).append(", ").append("partition: ").append(this.partition);

        return sb.toString();
    }
}
