package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import flink.twitter.streaming.model.PerWindowTopicCount;
import flink.twitter.streaming.model.TweetTopic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;


public class PerWindowTopicCounter {

    List<Integer> windowSizeMinList;
    int allowedLatenessSec;
    private int topicCount;

    public PerWindowTopicCounter(Config aggregationConfig, Config topicFilterConf) {
        this.windowSizeMinList = aggregationConfig.getIntList("windows.min");
        this.allowedLatenessSec = aggregationConfig.getInt("allowed.lateness.sec");
        this.topicCount = topicFilterConf.getStringList("topics").size();
    }

    public DataStream<PerWindowTopicCount> generateCountPerWindow(DataStream<TweetTopic> tweetStream,
                                                                  List<SinkFunction> sinks) {

        DataStream<PerWindowTopicCount> topicCntStream = tweetStream.map(
                tweet -> new PerWindowTopicCount(tweet.getTopic(), 1, -1, -1));

        for (int windowSizeMin : windowSizeMinList) {
            topicCntStream = topicCntStream
                    .keyBy(topicCnt -> topicCnt.getTopic())
                    .window(SlidingEventTimeWindows.of(Time.minutes(windowSizeMin), Time.minutes(1)))
                    .allowedLateness(Time.seconds(allowedLatenessSec))
                    .process(new ProcessWindowFunction<PerWindowTopicCount, PerWindowTopicCount, String, TimeWindow>() {
                        @Override
                        public void process(String topic,
                                            ProcessWindowFunction<PerWindowTopicCount, PerWindowTopicCount, String, TimeWindow>.Context context,
                                            Iterable<PerWindowTopicCount> iterable,
                                            Collector<PerWindowTopicCount> collector) throws Exception {

                            int cntSum = 0;
                            for (PerWindowTopicCount cur : iterable) {
                                cntSum += cur.getCount();
                            }
                            collector.collect(new PerWindowTopicCount(topic, cntSum, windowSizeMin, context.window().getEnd()));
                        }
                    })
                    .setParallelism(topicCount);

            for (SinkFunction sink: sinks) {
                topicCntStream
                        .addSink(sink)
                        .setParallelism(topicCount);
            }

        }
        return topicCntStream;
    }

}
