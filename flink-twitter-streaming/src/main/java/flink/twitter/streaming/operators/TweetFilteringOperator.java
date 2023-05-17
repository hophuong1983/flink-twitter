package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import flink.twitter.streaming.filtering.CountryRule;
import flink.twitter.streaming.filtering.TopicRule;
import flink.twitter.streaming.model.Tweet;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class TweetFilteringOperator implements Serializable {

    CountryRule countryRule;
    List<TopicRule> topicRules;

    public TweetFilteringOperator(String country, List<String> topicList) {
        this.countryRule = new CountryRule(country);
        topicRules = topicList.stream().map(topic -> new TopicRule(topic)).collect(Collectors.toList());
    }

    public TweetFilteringOperator(Config trendsConfig) {
        this(trendsConfig.getString("country"), trendsConfig.getStringList("topics"));
    }

    public DataStream<Tweet> filter(DataStream<Tweet> tweetStream) {
        return tweetStream
                .filter(tweet -> filter(tweet));
    }

    private boolean filter(Tweet tweet) {
        return countryRule.apply(tweet) &&
                topicRules.stream().anyMatch(rule -> rule.apply(tweet));
    }
}
