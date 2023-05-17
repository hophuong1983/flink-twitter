package flink.twitter.streaming;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import flink.twitter.streaming.functions.PubNubSource;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.utils.ConfigUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Properties;

public abstract class PubNubClient {

    final static Logger logger = Logger.getLogger(PubNubClient.class);
    protected Config config;

    public PubNubClient(String configFilePath) {

        logger.info("Config file " + configFilePath);
        config = ConfigFactory.parseFile(new File(configFilePath));
        logger.info("Config  " + config);
    }

    public abstract void processTweetStream(DataStream<Tweet> tweetStream);

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Connect to PubNub
        Properties pubNubConf = ConfigUtils.propsFromConfig(config.getConfig("pubnub"));
        DataStream<Tweet> tweetStream = env.addSource(new PubNubSource(pubNubConf));
        processTweetStream(tweetStream);

        env.execute();
    }
}
