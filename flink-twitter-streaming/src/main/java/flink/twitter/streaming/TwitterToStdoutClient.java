package flink.twitter.streaming;

import flink.twitter.streaming.model.Tweet;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TwitterToStdoutClient extends PubNubClient {
    public TwitterToStdoutClient(String configFilePath) {
        super(configFilePath);
    }

    public static void main(String[] args) throws Exception {

        if (args.length == 0){
            logger.error("Config file is missing");
            System.exit(1);
        }

        String configFilePath = args[0];
        TwitterToStdoutClient client = new TwitterToStdoutClient(configFilePath);
        client.run();
    }

    @Override
    public void processTweetStream(DataStream<Tweet> tweetStream) {
        tweetStream.print();
    }
}
