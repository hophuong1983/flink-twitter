package flink.twitter.streaming;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import flink.twitter.streaming.functions.PubNubSource;
import flink.twitter.streaming.model.PerWindowTopicCount;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.model.TweetTopic;
import flink.twitter.streaming.operators.DeduplicationOperator;
import flink.twitter.streaming.operators.PerWindowTopicCounter;
import flink.twitter.streaming.operators.TweetFilteringOperator;
import flink.twitter.streaming.utils.ConfigUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

public class TwitterTrendAnalyzerClient {

    private static final Logger LOG = Logger.getLogger(TwitterTrendAnalyzerClient.class);

    Config config;

    public TwitterTrendAnalyzerClient(String configFilePath) {

        LOG.info("Config file " + configFilePath);
        config = ConfigFactory.parseFile(new File(configFilePath));
        LOG.info("Config  " + config);
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Connect to PubNub
        Properties pubNubConf = ConfigUtils.propsFromConfig(config.getConfig("pubnub"));
        DataStream<Tweet> tweetStream = env.addSource(new PubNubSource(pubNubConf));

        // Do filtering
        Config trendsConfig = config.getConfig("twitter.filtering");
        TweetFilteringOperator filterOperator = new TweetFilteringOperator(trendsConfig);
        DataStream<TweetTopic> topicStream = filterOperator.filter(tweetStream);

        // Get configured config
        Config aggregationConfig = config.getConfig("aggregation");
        List<Integer> windows = aggregationConfig.getIntList("windows");

        // Deduplicate stream
        DeduplicationOperator deduplicationOperator = new DeduplicationOperator();
        DataStream<TweetTopic> deduplicatedTopicStream =
                deduplicationOperator.deduplicate(topicStream, Collections.max(windows));

        // For each topic, count messages per window
        List<SinkFunction> sinks = Arrays.asList(new PrintSinkFunction());
        PerWindowTopicCounter countOperator = new PerWindowTopicCounter();
        countOperator.generateCountPerWindow(deduplicatedTopicStream, windows, sinks);

        env.execute();
    }

    public static void main(String[] args) throws Exception {

        if (args.length == 0){
            LOG.error("Config file is missing");
            System.exit(1);
        }

        String configFilePath = args[0];
        TwitterTrendAnalyzerClient client = new TwitterTrendAnalyzerClient(configFilePath);
        client.run();
    }

}
