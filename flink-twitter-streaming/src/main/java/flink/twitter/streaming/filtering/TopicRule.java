package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;

import java.util.Arrays;

public abstract class TopicRule implements Rule {

    String topic;
    String[] keywords;

    public TopicRule(String topic) {
        this.topic = topic;
        keywords = topic.toLowerCase().split(" ");
    }

    @Override
    public boolean apply(Tweet tweet) {
        return containsAllKeywords(tweet.getText()) ||
                containsAllKeywords(tweet.getUserName()) ||
                Arrays.stream(tweet.getHashTags()).anyMatch(text -> containsAllKeywords(text));
    }

    public String getTopic() {
        return topic;
    }

    protected abstract boolean containsAllKeywords(String text);
}
