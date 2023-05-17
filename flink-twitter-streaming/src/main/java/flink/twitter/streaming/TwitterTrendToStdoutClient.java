package flink.twitter.streaming;

import com.typesafe.config.Config;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.operators.TweetFilteringOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TwitterTrendToStdoutClient extends PubNubClient {
    public TwitterTrendToStdoutClient(String configFilePath) {
        super(configFilePath);
    }

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            logger.error("Config file is missing");
            System.exit(1);
        }

        String configFilePath = args[0];
        TwitterTrendToStdoutClient client = new TwitterTrendToStdoutClient(configFilePath);
        client.run();
    }

    @Override
    public void processTweetStream(DataStream<Tweet> tweetStream) {

        Config trendsConfig = config.getConfig("twitter.topic_filter");
        TweetFilteringOperator operator = new TweetFilteringOperator(trendsConfig);
        operator.filter(tweetStream).print();
    }
}