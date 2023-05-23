package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import flink.twitter.streaming.functions.DeduplicationFunction;
import flink.twitter.streaming.model.TweetTopic;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DeduplicationOperator {

    int deduplicateStateTtlMin;

    public DeduplicationOperator(int deduplicateStateTtlMin) {
        this.deduplicateStateTtlMin = deduplicateStateTtlMin;
    }

    public DeduplicationOperator(Config config){
        this(config.getInt("seen.window.sec"));
    }

    public DataStream<TweetTopic> deduplicate(DataStream<TweetTopic> tweetStream){
        // Deduplicate tweetStream by id
        return tweetStream
                .keyBy(topic -> topic.getId())
                .flatMap(new DeduplicationFunction(deduplicateStateTtlMin));
    }
}
