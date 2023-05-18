package flink.twitter.streaming.operators;

import com.typesafe.config.Config;
import flink.twitter.streaming.filtering.Rule;
import flink.twitter.streaming.filtering.RuleFactory;
import flink.twitter.streaming.filtering.TopicRule;
import flink.twitter.streaming.model.Tweet;
import flink.twitter.streaming.model.TweetTopic;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

public class TweetFilteringOperator implements Serializable {

    Rule countryCodeRule;
    List<TopicRule> topicRules;

    public TweetFilteringOperator(Config countryCodeFilterConf, Config topicFilterConf) {
        this.countryCodeRule = RuleFactory.createCountryCodeRule(countryCodeFilterConf);
        topicRules = RuleFactory.createTopicRule(topicFilterConf);
    }

    public TweetFilteringOperator(Config trendsConfig) {
        this(trendsConfig.getConfig("country.filter"), trendsConfig.getConfig("topic.filter"));
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
