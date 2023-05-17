package flink.twitter.streaming;

import com.typesafe.config.Config;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.operators.TweetFilteringOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TopTrendTwitterToStdoutClient extends PubNubClient {
    public TopTrendTwitterToStdoutClient(String configFilePath) {
        super(configFilePath);
    }

    @Override
    public void processTweetStream(DataStream<Tweet> tweetStream) {

        Config trendsConfig = config.getConfig("twitter.topic_filter");
        TweetFilteringOperator operator = new TweetFilteringOperator(trendsConfig);
        operator.filter(tweetStream).print();
    }
}
