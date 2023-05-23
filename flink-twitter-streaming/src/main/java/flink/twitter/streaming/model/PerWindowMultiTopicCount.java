package flink.twitter.streaming.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PerWindowMultiTopicCount implements Serializable {

    int windowSizeMin;

    long watermarkTimeMs;

    List<TopicCount> topicCounts = new ArrayList<>();

    static class TopicCount {
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
    }

    public static PerWindowMultiTopicCount fromSingleCount(PerWindowTopicCount singleCount) {
        PerWindowMultiTopicCount multiCnt = new PerWindowMultiTopicCount();
        multiCnt.watermarkTimeMs = singleCount.watermarkTimeMs;
        multiCnt.windowSizeMin = singleCount.windowSizeMin;
        multiCnt.topicCounts.add(new TopicCount(singleCount.topic, singleCount.count));
        return multiCnt;
    }

    public static PerWindowMultiTopicCount merge(PerWindowMultiTopicCount count1, PerWindowMultiTopicCount count2){

        PerWindowMultiTopicCount mergedCnt = new PerWindowMultiTopicCount();
        mergedCnt.watermarkTimeMs = count1.watermarkTimeMs;
        mergedCnt.windowSizeMin = count1.windowSizeMin;

        mergedCnt.topicCounts.addAll(count1.topicCounts);
        mergedCnt.topicCounts.addAll(count2.topicCounts);
        return mergedCnt;
    }

    public int getWindowSizeMin() {
        return windowSizeMin;
    }

    public long getWatermarkTimeMs() {
        return watermarkTimeMs;
    }

    public List<TopicCount> getTopicCounts() {
        return topicCounts;
    }

    @Override
    public String toString() {
        return "PerWindowMultiTopicCount{" +
                "windowSizeMin=" + windowSizeMin +
                ", watermarkTimeMs=" + watermarkTimeMs +
                ", topicCounts=" + topicCounts +
                '}';
    }
}
