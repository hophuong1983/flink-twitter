package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;

import java.util.Arrays;

public class TopicRule implements Rule {

    String[] keywords;

    public TopicRule(String topic) {
        keywords = topic.toLowerCase().split(" ");
    }

    @Override
    public boolean apply(Tweet tweet) {
        return containsAllKeywords(tweet.getText()) ||
                containsAllKeywords(tweet.getUserName()) ||
                Arrays.stream(tweet.getHashTags()).anyMatch(text -> containsAllKeywords(text));
    }

    private boolean containsAllKeywords(String text) {
        String lowerCasedText = text.toLowerCase();
        return Arrays.stream(keywords).allMatch(keyword -> lowerCasedText.contains(keyword));
    }

}
