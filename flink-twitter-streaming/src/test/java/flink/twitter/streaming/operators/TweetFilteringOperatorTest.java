package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.model.TweetTopic;
import flink.twitter.streaming.utils.ListSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class TweetFilteringOperatorTest {

    TweetFilteringOperator operator = new TweetFilteringOperator(
            ConfigFactory.parseMap(Map.of("country.code", "es")),
            ConfigFactory.parseMap(
                    Map.of(
                            "class.name", "StrictTopicRule",
                            "topics", Arrays.asList("a b", "c d")))
    );

    @Test
    void filter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tweet> tweetStream = env.fromElements(
                // match text
                new Tweet("1", 1, "A B c", "D E F", "es", new String[]{"g h", "i k"}),
                new Tweet("2", 1, "A B c", "D E F", "fr", new String[]{"g h", "i k"}),

                // match userName
                new Tweet("3", 1, "A h c", "D c F", "es", new String[]{"g h", "i k"}),
                new Tweet("4", 1, "A h c", "D c F", "fr", new String[]{"g h", "i k"}),

                // match hash tags
                new Tweet("5", 1, "A h c", "D z F", "es", new String[]{"a b", "i k"}),
                new Tweet("6", 1, "A h c", "D z F", "fr", new String[]{"a b", "i k"}),

                // no country
                new Tweet("7", 1, "A B c", "D c F", null, new String[]{"a b", "i k"})
        );

        DataStream<TweetTopic> filteredTweetStream = operator.filter(tweetStream);

        ListSink sink = new ListSink();
        ListSink.outputList.clear();
        filteredTweetStream.addSink(sink);

        env.execute();
        List expected = Arrays.asList(
                // name matching
                new TweetTopic("a b", "1", 1),
                new TweetTopic("c d", "3", 1),
                new TweetTopic("a b", "5", 1),
                new TweetTopic("a b", "7", 1),
                new TweetTopic("c d", "7", 1)
        );

        assertIterableEquals(expected, ListSink.outputList);
    }

}