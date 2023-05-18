package flink.twitter.streaming.operators;

import flink.twitter.streaming.model.PerWindowTopicCount;
import flink.twitter.streaming.model.TweetTopic;
import flink.twitter.streaming.utils.ListSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicationOperatorTest {

    @Test
    void deduplicate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TweetTopic> tweetDs = env.fromElements(
                // minute 0
                new TweetTopic("Pebbles","1", 1684100474000l),
                new TweetTopic("Fred","2", 1684100474001l),
                new TweetTopic("Wilma","3", 1684100474002l),
                // minute 2
                new TweetTopic("Wilma","3", 1684100474002l),
                new TweetTopic("Fred","4", 1684100474000l + 120000),
                new TweetTopic("Wilma","5", 1684100474000l + 120000),
                // minute 6
                new TweetTopic("Fred", "4", 1684100474000l + 360000)
        );

        DeduplicationOperator deduplicationOperator = new DeduplicationOperator();
        DataStream<TweetTopic> result = deduplicationOperator.deduplicate(tweetDs, 3);

        ListSink listSink = new ListSink();
        listSink.outputList.clear();
        result.addSink(listSink);
        env.execute();

        List<TweetTopic> expected = Arrays.asList(
                new TweetTopic("Pebbles","1", 1684100474000l),
                new TweetTopic("Fred","2", 1684100474001l),
                new TweetTopic("Wilma","3", 1684100474002l),
                new TweetTopic("Fred","4", 1684100474000l + 120000),
                new TweetTopic("Wilma","5", 1684100474000l + 120000)
        );
        assertIterableEquals(expected, listSink.outputList);
    }
}