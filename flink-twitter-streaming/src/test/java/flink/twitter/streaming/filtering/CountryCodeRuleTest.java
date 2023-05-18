package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CountryCodeRuleTest {

    @Test
    void apply() {
        // match text
        Rule rule = new CountryCodeRule("US");
        Tweet tweet = new Tweet("adg", 1, "A B c", "D E F", "us", new String[]{"g h", "i k"});
        assertTrue(rule.apply(tweet));

        // match useName
        rule = new CountryCodeRule("US");
        tweet = new Tweet("adg", 1, "A B c", "D E F", "FR", new String[]{"g h", "i k"});
        assertFalse(rule.apply(tweet));

        // match hash tags
        rule = new CountryCodeRule("US");
        tweet = new Tweet("adg", 1, "A B c", "D E F", null, new String[]{"g h", "i k"});
        assertTrue(rule.apply(tweet));
    }
}