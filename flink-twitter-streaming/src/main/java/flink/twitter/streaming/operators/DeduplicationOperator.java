package flink.twitter.streaming.operators;

import flink.twitter.streaming.functions.DeduplicationFunction;
import flink.twitter.streaming.model.TweetTopic;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DeduplicationOperator {

    public DataStream<TweetTopic> deduplicate(DataStream<TweetTopic> tweetStream,
                                              int deduplicateStateTtlMin){
        return tweetStream
                .keyBy(topic -> topic.getId())
                .flatMap(new DeduplicationFunction(deduplicateStateTtlMin));
    }
}
