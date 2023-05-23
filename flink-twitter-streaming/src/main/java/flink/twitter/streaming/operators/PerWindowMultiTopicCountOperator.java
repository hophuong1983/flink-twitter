package flink.twitter.streaming.operators;

import flink.twitter.streaming.model.PerWindowMultiTopicCount;
import flink.twitter.streaming.model.PerWindowTopicCount;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PerWindowMultiTopicCountOperator {

    public DataStream<PerWindowMultiTopicCount> generateCountPerWindow(DataStream<PerWindowTopicCount> countDataStream) {
        return countDataStream
                .map(singleCount -> PerWindowMultiTopicCount.fromSingleCount(singleCount))
                .keyBy(countEvent -> countEvent.getWatermarkTimeMs() + "-" + countEvent.getWindowSizeMin())
                .reduce((count1, count2) -> PerWindowMultiTopicCount.merge(count1, count2));
    }
}
