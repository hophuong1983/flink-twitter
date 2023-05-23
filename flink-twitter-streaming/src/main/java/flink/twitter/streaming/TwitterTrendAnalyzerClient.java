package flink.twitter.streaming;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import flink.twitter.streaming.functions.PerWindowTopicCountRedisMapper;
import flink.twitter.streaming.functions.PubNubSource;
import flink.twitter.streaming.model.PerWindowTopicCount;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.model.TweetTopic;
import flink.twitter.streaming.operators.DeduplicationOperator;
import flink.twitter.streaming.operators.PerWindowMultiTopicCountOperator;
import flink.twitter.streaming.operators.PerWindowTopicCounter;
import flink.twitter.streaming.operators.TweetFilteringOperator;
import flink.twitter.streaming.utils.ConfigUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TwitterTrendAnalyzerClient {

    private static final Logger LOG = Logger.getLogger(TwitterTrendAnalyzerClient.class);

    Config config;

    public TwitterTrendAnalyzerClient(String configFilePath) {

        LOG.info("Config file " + configFilePath);
        config = ConfigFactory.parseFile(new File(configFilePath));
        LOG.info("Config  " + config);
    }

    public void run() throws Exception {
        updateRedisMetaData();
        runStreaming();
    }

    private void updateRedisMetaData() throws JsonProcessingException {
        Config redisConfig = config.getConfig("redis");
        String redisHost = redisConfig.getString("host");
        int redisPort = redisConfig.getInt("port");
        String hashKey = redisConfig.getString("current.hash.key");

        Jedis jedis = new Jedis(redisHost, redisPort);

        // Insert window information to Redis
        Config aggregationConfig = config.getConfig("twitter.aggregation");
        List<Integer> windows = aggregationConfig.getIntList("windowsMin");
        jedis.hset(hashKey, "windows", windows.toString());

        // Insert topic information to Redis
        Config topicConfig = config.getConfig("twitter.filtering.topic.filter");
        List<String> topics = topicConfig.getStringList("topics");
        jedis.hset(hashKey, "topics", new ObjectMapper().writeValueAsString(topics));
    }

    private void runStreaming() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Connect to PubNub
        Properties pubNubConf = ConfigUtils.propsFromConfig(config.getConfig("pubnub"));
        DataStream<Tweet> tweetStream = env.addSource(new PubNubSource(pubNubConf));

        // Do filtering
        Config trendsConfig = config.getConfig("twitter.filtering");
        TweetFilteringOperator filterOperator = new TweetFilteringOperator(trendsConfig);
        DataStream<TweetTopic> topicStream = filterOperator.filter(tweetStream);

        // Deduplicate stream
        int seenWindowSec = config.getInt("twitter.deduplication.seenWindowSec");
        DeduplicationOperator deduplicationOperator = new DeduplicationOperator();
        DataStream<TweetTopic> deduplicatedTopicStream =
                deduplicationOperator.deduplicate(topicStream, seenWindowSec);

        // For each topic, count messages per window
        Config aggregationConfig = config.getConfig("twitter.aggregation");
        List<Integer> windows = aggregationConfig.getIntList("windowsMin");
        int allowedLatenessSec = aggregationConfig.getInt("allowedLatenessSec");
        PerWindowTopicCounter countOperator = new PerWindowTopicCounter();

        DataStream<PerWindowTopicCount> countStream = countOperator.generateCountPerWindow(
                deduplicatedTopicStream, windows, allowedLatenessSec);
        countStream.addSink(createRedisSink());

        // Do aggregation topic count per period - history of count
        PerWindowMultiTopicCountOperator multiTopicCountOperator = new PerWindowMultiTopicCountOperator();
        multiTopicCountOperator.generateCountPerWindow(countStream).print();

        env.execute();
    }

    private RedisSink<PerWindowTopicCount> createRedisSink() {
        Config redisConfig = config.getConfig("redis");
        FlinkJedisPoolConfig redisPoolConf =
                new FlinkJedisPoolConfig.Builder()
                        .setHost(redisConfig.getString("host"))
                        .setPort(redisConfig.getInt("port")).build();

        return new RedisSink<PerWindowTopicCount>(
                redisPoolConf,
                new PerWindowTopicCountRedisMapper(redisConfig.getString("current.hash.key")));
    }


    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            LOG.error("Config file is missing");
            System.exit(1);
        }

        String configFilePath = args[0];
        TwitterTrendAnalyzerClient client = new TwitterTrendAnalyzerClient(configFilePath);
        client.run();
    }

}
