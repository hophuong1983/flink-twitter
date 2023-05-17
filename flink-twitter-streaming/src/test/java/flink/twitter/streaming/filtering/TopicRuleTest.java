package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicRuleTest {

    @Test
    void apply() {

        // match text
        Rule rule = new TopicRule("A b");
        Tweet tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[]{"g h", "i k"});
        assertTrue(rule.apply(tweet));

        // match useName
        rule = new TopicRule("D E");
        tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[]{"g h", "i k"});
        assertTrue(rule.apply(tweet));

        // match hash tags
        rule = new TopicRule("g H");
        tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[]{"g h", "i k"});
        assertTrue(rule.apply(tweet));

    }

    @Test
    void apply2() {
        // match text
        Rule rule = new TopicRule("A z");
        Tweet tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[]{"g h", "i k"});
        assertFalse(rule.apply(tweet));

        // match useName
        rule = new TopicRule("D z");
        tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[]{"g h", "i k"});
        assertFalse(rule.apply(tweet));

        // match hash tags
        rule = new TopicRule("g z");
        tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[]{"g h", "i k"});
        assertFalse(rule.apply(tweet));
    }

    @Test
    void apply3() {

        // match text
        Rule rule = new TopicRule("A b");
        Tweet tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[0]);
        assertTrue(rule.apply(tweet));

        // match useName
        rule = new TopicRule("D E");
        tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[0]);
        assertTrue(rule.apply(tweet));

        // match hash tags
        rule = new TopicRule("g H");
        tweet = new Tweet("adg", 1, "A B c", "D E F", "es", new String[0]);
        assertFalse(rule.apply(tweet));

    }
}