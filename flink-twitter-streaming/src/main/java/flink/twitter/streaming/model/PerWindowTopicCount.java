package flink.twitter.streaming.model;

import java.io.Serializable;
import java.util.Objects;

public class PerWindowTopicCount implements Serializable {
    String topic;
    int count;
    int windowSizeMin;

    long watermarkTimeMs;

    public PerWindowTopicCount(String topic, int count, int windowSizeMin, long watermarkTimeMs) {

        this.topic = topic;
        this.count = count;
        this.windowSizeMin = windowSizeMin;
        this.watermarkTimeMs = watermarkTimeMs;
    }

    @Override
    public String toString() {
        return "TopicCountPerWindow{" +
                "topic='" + topic + '\'' +
                ", count=" + count +
                ", windowSizeMin=" + windowSizeMin +
                ", watermarkTimeMs=" + watermarkTimeMs +
                '}';
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        PerWindowTopicCount that = (PerWindowTopicCount) other;
        return count == that.count
                && windowSizeMin == that.windowSizeMin
                && watermarkTimeMs == that.watermarkTimeMs
                && Objects.equals(topic, that.topic);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getWindowSizeMin() {
        return windowSizeMin;
    }

    public void setWindowSizeMin(int windowSizeMin) {
        this.windowSizeMin = windowSizeMin;
    }

    public long getWatermarkTimeMs() {
        return watermarkTimeMs;
    }

    public void setWatermarkTimeMs(long watermarkTimeMs) {
        this.watermarkTimeMs = watermarkTimeMs;
    }
}
