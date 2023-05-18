package flink.twitter.streaming.model;

import java.io.Serializable;
import java.util.Objects;

public class TweetTopic implements Serializable {

    String topic;
    String id;
    long timestampMs;

    public TweetTopic(String topic, String id, long timestampMs) {
        this.topic = topic;
        this.id = id;
        this.timestampMs = timestampMs;
    }

    @Override
    public String toString() {
        return "TweetTopic{" +
                "topic='" + topic + '\'' +
                ", id='" + id + '\'' +
                ", timestampMs=" + timestampMs +
                '}';
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TweetTopic)) {
            return false;
        }
        TweetTopic that = (TweetTopic) other;
        return timestampMs == that.timestampMs &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(id, that.id);
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }
}
