package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;

public class CountryCodeRule implements Rule {

    String countryCode;

    public CountryCodeRule(String countryCode) {
        this.countryCode = countryCode.toLowerCase();
    }

    @Override
    public boolean apply(Tweet tweet) {
        return tweet.getCountryCode().toLowerCase().equals(countryCode);
    }
}
