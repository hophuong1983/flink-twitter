package flink.twitter.streaming.operators;

import flink.twitter.streaming.model.TweetTopic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicationOperatorTest {

    @Test
    void deduplicate() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TweetTopic> tweetDs = env.fromElements(
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
    }
}