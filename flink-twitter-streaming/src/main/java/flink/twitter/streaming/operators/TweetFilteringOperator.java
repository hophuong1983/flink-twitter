package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import flink.twitter.streaming.filtering.CountryCodeRule;
import flink.twitter.streaming.filtering.TopicRule;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.model.TweetTopic;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class TweetFilteringOperator implements Serializable {

    CountryCodeRule countryCodeRule;
    List<TopicRule> topicRules;

    public TweetFilteringOperator(String countryCode, List<String> topicList) {
        this.countryCodeRule = new CountryCodeRule(countryCode);
        topicRules = topicList.stream().map(topic -> new TopicRule(topic)).collect(Collectors.toList());
    }

    public TweetFilteringOperator(Config trendsConfig) {
        this(trendsConfig.getString("country_code"), trendsConfig.getStringList("topics"));
    }

    public DataStream<TweetTopic> filter(DataStream<Tweet> tweetStream) {
        return tweetStream
                .filter(tweet -> countryCodeRule.apply(tweet))
                .flatMap(new FlatMapFunction<Tweet, TweetTopic>() {
                             @Override
                             public void flatMap(Tweet tweet, Collector<TweetTopic> collector) throws Exception {
                                 topicRules.forEach(rule -> {
                                     if (rule.apply(tweet)) {
                                         collector.collect(
                                                 new TweetTopic(rule.getTopic(), tweet.getId(), tweet.getTimestampMs())
                                         );
                                     }
                                 });
                             }
                         }
                );
    }

}
