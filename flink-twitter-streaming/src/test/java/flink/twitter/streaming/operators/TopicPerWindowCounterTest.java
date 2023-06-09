package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import flink.twitter.streaming.model.PerWindowTopicCount;
import flink.twitter.streaming.model.TweetTopic;
import flink.twitter.streaming.utils.ListSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class TopicPerWindowCounterTest {

    static List<PerWindowTopicCount> oneMinCntExpected = Arrays.asList(
            //window ends at minute 0
            new PerWindowTopicCount("Pebbles", 1, 1, 1684100520000l),
            new PerWindowTopicCount("Fred", 1, 1, 1684100520000l),
            new PerWindowTopicCount("Wilma", 1, 1, 1684100520000l),

            //window ends at minute 2
            new PerWindowTopicCount("Fred", 1, 1, 1684100640000l),
            new PerWindowTopicCount("Wilma", 1, 1, 1684100640000l),

            //window ends at minute 6
            new PerWindowTopicCount("Fred", 1, 1, 1684100880000l)
    );

    static List<PerWindowTopicCount> fiveMinCntExpected = Arrays.asList(

            //window ends at minute 3
            new PerWindowTopicCount("Pebbles", 1, 5, 1684100700000l),
            new PerWindowTopicCount("Wilma", 2, 5, 1684100700000l),
            new PerWindowTopicCount("Fred", 2, 5, 1684100700000l),

            //window ends at minute 8
            new PerWindowTopicCount("Fred", 1, 5, 1684101000000l)
    );

    static List<PerWindowTopicCount> tenMinCntExpected = Arrays.asList(

            //window ends at minute 8
            new PerWindowTopicCount("Pebbles", 1, 10, 1684101000000l),
            new PerWindowTopicCount("Wilma", 2, 10, 1684101000000l),
            new PerWindowTopicCount("Fred", 3, 10, 1684101000000l)
    );

    static void generateCountPerMinuteWith(List<Integer> windowSizeMinList,
                                           List<SinkFunction> sinks,
                                           List<PerWindowTopicCount> expected) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TweetTopic> tweetDs = env.fromElements(
                // minute 0
                new TweetTopic("Pebbles","1", 1684100474000l),
                new TweetTopic("Fred","2", 1684100474001l),
                new TweetTopic("Wilma","3", 1684100474002l),
                // minute 2
                new TweetTopic("Fred","4", 1684100584000l),
                new TweetTopic("Wilma","5", 1684100584000l),
                // minute 6
                new TweetTopic("Fred", "4", 1684100834000l)
        );

        tweetDs = tweetDs.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TweetTopic>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestampMs())
        );

        Config aggregationConf = ConfigFactory.load()
                .withValue("windows.min", ConfigValueFactory.fromAnyRef(windowSizeMinList))
                .withValue("allowed.lateness.sec", ConfigValueFactory.fromAnyRef(1));
        Config filterConf = ConfigFactory.load()
                .withValue(
                        "topics",
                        ConfigValueFactory.fromAnyRef(Arrays.asList("1", "2")));
        PerWindowTopicCounter counter = new PerWindowTopicCounter(aggregationConf, filterConf);
        DataStream<PerWindowTopicCount> result = counter.generateCountPerWindow(tweetDs, sinks);

        if (sinks.isEmpty()) {
            ListSink listSink = new ListSink();
            listSink.outputList.clear();
            result.addSink(listSink);
        }

        env.execute();
        assertTrue(expected.containsAll(ListSink.outputList));
        assertTrue(ListSink.outputList.containsAll(expected));
    }
    @Test
    void generateCountPerMinute() throws Exception {
        // window following window generates same result as one window only
        generateCountPerMinuteWith(Arrays.asList(5), new ArrayList<>(), fiveMinCntExpected);
        generateCountPerMinuteWith(Arrays.asList(1), new ArrayList<>(), oneMinCntExpected);
        generateCountPerMinuteWith(Arrays.asList(10), new ArrayList<>(), tenMinCntExpected);
    }

    @Test
    void generateCountPerMinute2() throws Exception {
        // mix window results
        ListSink listSink = new ListSink();
        listSink.outputList.clear();
        List<PerWindowTopicCount> expected = new ArrayList<>();
        expected.addAll(oneMinCntExpected);
        expected.addAll(fiveMinCntExpected);

        generateCountPerMinuteWith(Arrays.asList(1, 5), Arrays.asList(listSink), expected);

    }

    @Test
    void generateCountPerMinute3() throws Exception {
        // mix window results
        ListSink listSink = new ListSink();
        listSink.outputList.clear();
        List<PerWindowTopicCount> expected = new ArrayList<>();
        expected.addAll(oneMinCntExpected);
        expected.addAll(fiveMinCntExpected);
        expected.addAll(tenMinCntExpected);

        generateCountPerMinuteWith(Arrays.asList(1, 5, 10), Arrays.asList(listSink), expected);

    }

}
