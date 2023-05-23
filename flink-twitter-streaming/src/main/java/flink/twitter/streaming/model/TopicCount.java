package flink.twitter.streaming.model;

import java.io.Serializable;

public class TopicCount implements Serializable {
    String topic;
    long count;

    public TopicCount(String topic, long count) {
        this.topic = topic;
        this.count = count;
    }

    @Override
    public String toString() {
        return "TopicCount{" +
                "topic='" + topic + '\'' +
                ", count=" + count +
                '}';
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
