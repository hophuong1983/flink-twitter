package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import flink.twitter.streaming.model.PerWindowMultiTopicCount;
import flink.twitter.streaming.model.PerWindowTopicCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PerWindowMultiTopicCounter {

    int allowedLatenessSec;

    public PerWindowMultiTopicCounter(Config aggregationConfig) {
        this.allowedLatenessSec = aggregationConfig.getInt("allowed.lateness.sec");
    }

    public DataStream<PerWindowMultiTopicCount> generateCountPerWindow(DataStream<PerWindowTopicCount> countDataStream) {
        return countDataStream
                .map(singleCount -> PerWindowMultiTopicCount.fromSingleCount(singleCount))
                .keyBy(countEvent -> countEvent.getWatermarkTimeMs() + "-" + countEvent.getWindowSizeMin())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(allowedLatenessSec))
                .reduce((count1, count2) -> PerWindowMultiTopicCount.merge(count1, count2));
    }
}
