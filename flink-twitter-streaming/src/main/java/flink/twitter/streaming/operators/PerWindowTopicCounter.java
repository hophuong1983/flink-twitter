package flink.twitter.streaming.operators;

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
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class PerWindowTopicCounter {

    private DataStream<TweetTopic> assignWatermark(DataStream<TweetTopic> tweetStream) {
        return tweetStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TweetTopic>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestampMs())
        );
    }

    public DataStream<PerWindowTopicCount> generateCountPerWindow(DataStream<TweetTopic> tweetStream,
                                                                  List<Integer> windowSizeMinList,
                                                                  int allowedLatenessSec,
                                                                  List<SinkFunction> sinks) {

        DataStream<PerWindowTopicCount> topicCntStream =
                assignWatermark(tweetStream).map(
                        tweet -> new PerWindowTopicCount(tweet.getTopic(), 1, -1, -1));

        List<DataStream<PerWindowTopicCount>> streamList = new ArrayList<>();
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
                    .setParallelism(windowSizeMinList.size());

            for (SinkFunction sink: sinks) {
                topicCntStream
                        .addSink(sink)
                        .setParallelism(windowSizeMinList.size());
            }

            streamList.add(topicCntStream);
        }

        return streamList.stream().reduce((stream1, stream2) -> stream1.union(stream2)).get();
    }

}
