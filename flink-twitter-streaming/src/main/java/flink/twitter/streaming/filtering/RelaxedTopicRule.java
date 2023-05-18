package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;

import java.util.Arrays;

public class RelaxedTopicRule extends TopicRule {

    public RelaxedTopicRule(String topic) {
        super(topic);
    }

    @Override
    protected boolean containsAllKeywords(String text) {
        String lowerCasedText = " " + text.toLowerCase() + " ";
        return Arrays.stream(keywords).allMatch(keyword -> lowerCasedText.contains(keyword));
    }
}
