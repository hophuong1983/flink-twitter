package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;

import java.util.Arrays;
import java.util.function.IntFunction;

public class TopicRule implements Rule {

    String topic;
    String[] keywords;

    public TopicRule(String topic) {
        this.topic = topic;
        keywords = topic.toLowerCase().split(" ");
        keywords = Arrays.stream(keywords).map(keyword -> " " + keyword + " ").toArray(String[]::new);
    }

    @Override
    public boolean apply(Tweet tweet) {
        return containsAllKeywords(tweet.getText()) ||
                containsAllKeywords(tweet.getUserName()) ||
                Arrays.stream(tweet.getHashTags()).anyMatch(text -> containsAllKeywords(text));
    }

    private boolean containsAllKeywords(String text) {
        String lowerCasedText = " " + text.toLowerCase() + " ";
        return Arrays.stream(keywords).allMatch(keyword -> lowerCasedText.contains(keyword));
    }

    public String getTopic() {
        return topic;
    }
}