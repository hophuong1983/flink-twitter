package flink.twitter.streaming.operators;

import flink.twitter.streaming.model.PerWindowTopicCount;
import flink.twitter.streaming.model.TweetTopic;
import flink.twitter.streaming.utils.ListSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class TopicPerWindowCounterTest {

    static Iterable<PerWindowTopicCount> fiveMinCntExpected = Arrays.asList(
            //window ends at minute 0
            new PerWindowTopicCount("Pebbles", 1, 5, 1684100520000l),
            new PerWindowTopicCount("Fred", 1, 5, 1684100520000l),
            new PerWindowTopicCount("Wilma", 1, 5, 1684100520000l),

            //window ends at minute 1
            new PerWindowTopicCount("Fred", 1, 5, 1684100580000l),
            new PerWindowTopicCount("Pebbles", 1, 5, 1684100580000l),
            new PerWindowTopicCount("Wilma", 1, 5, 1684100580000l),

            //window ends at minute 2
            new PerWindowTopicCount("Fred", 2, 5, 1684100640000l),
            new PerWindowTopicCount("Pebbles", 1, 5, 1684100640000l),
            new PerWindowTopicCount("Wilma", 2, 5, 1684100640000l),

            //window ends at minute 3
            new PerWindowTopicCount("Pebbles", 1, 5, 1684100700000l),
            new PerWindowTopicCount("Wilma", 2, 5, 1684100700000l),
            new PerWindowTopicCount("Fred", 2, 5, 1684100700000l),

            //window ends at minute 4
            new PerWindowTopicCount("Pebbles", 1, 5, 1684100760000l),
            new PerWindowTopicCount("Wilma", 2, 5, 1684100760000l),
            new PerWindowTopicCount("Fred", 2, 5, 1684100760000l),

            //window ends at minute 5
            new PerWindowTopicCount("Fred", 1, 5, 1684100820000l),
            new PerWindowTopicCount("Wilma", 1, 5, 1684100820000l),

            //window ends at minute 6
            new PerWindowTopicCount("Fred", 2, 5, 1684100880000l),
            new PerWindowTopicCount("Wilma", 1, 5, 1684100880000l),

            //window ends at minute 7 after
            new PerWindowTopicCount("Fred", 1, 5, 1684100940000l),
            new PerWindowTopicCount("Fred", 1, 5, 1684101000000l),
            new PerWindowTopicCount("Fred", 1, 5, 1684101060000l),
            new PerWindowTopicCount("Fred", 1, 5, 1684101120000l)
    );

    static void generateCountPerMinuteWith(List<Integer> windowSizeMinList) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream tweetDs = env.fromElements(
                // minute 0
                new TweetTopic("Pebbles","1", 1684100474000l),
                new TweetTopic("Fred","2", 1684100474001l),
                new TweetTopic("Wilma","3", 1684100474002l),
                // minute 2
                new TweetTopic("Fred","4", 1684100474000l + 120000),
                new TweetTopic("Wilma","5", 1684100474000l + 120000),
                // minute 6
                new TweetTopic("Fred", "4", 1684100474000l + 360000)
        );

        PerWindowTopicCounter counter = new PerWindowTopicCounter();
        DataStream<TweetTopic> tweetTimeStream = counter.assignWatermark(tweetDs);
        DataStream<PerWindowTopicCount> result = counter.generateCountPerWindow(tweetTimeStream, Arrays.asList(5));

        ListSink listSink = new ListSink();
        listSink.outputList.clear();
        result.addSink(listSink);
        env.execute();

        assertIterableEquals(fiveMinCntExpected, listSink.outputList);
    }
    @Test
    void generateCountPerMinute() throws Exception {
        generateCountPerMinuteWith(Arrays.asList(5));
        generateCountPerMinuteWith(Arrays.asList(1, 5));
    }

}
