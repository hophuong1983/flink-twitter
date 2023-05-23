package flink.twitter.streaming.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PerWindowMultiTopicCount implements Serializable {

    int windowSizeMin;

    long watermarkTimeMs;

    List<TopicCount> topicCounts = new ArrayList<>();

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

    @Override
    public String toString() {
        return "PerWindowMultiTopicCount{" +
                "windowSizeMin=" + windowSizeMin +
                ", watermarkTimeMs=" + watermarkTimeMs +
                ", topicCounts=" + topicCounts +
                '}';
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

    public List<TopicCount> getTopicCounts() {
        return topicCounts;
    }

    public void setTopicCounts(List<TopicCount> topicCounts) {
        this.topicCounts = topicCounts;
    }
}
