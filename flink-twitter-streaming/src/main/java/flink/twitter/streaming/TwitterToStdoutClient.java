package flink.twitter.streaming;

import flink.twitter.streaming.model.Tweet;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TwitterToStdoutClient extends PubNubClient {
    public TwitterToStdoutClient(String configFilePath) {
        super(configFilePath);
    }

    @Override
    public void processTweetStream(DataStream<Tweet> tweetStream) {
        tweetStream.print();
    }
}
