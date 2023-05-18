package flink.twitter.streaming.filtering;

import java.util.Arrays;

public class StrictTopicRule extends TopicRule {

    public StrictTopicRule(String topic) {
        super(topic);

        keywords = Arrays.stream(keywords).map(keyword -> " " + keyword + " ").toArray(String[]::new);
    }

    @Override
    protected boolean containsAllKeywords(String text) {
        String lowerCasedText = " " + text.toLowerCase() + " ";
        return Arrays.stream(keywords).allMatch(keyword -> lowerCasedText.contains(keyword));
    }
}

