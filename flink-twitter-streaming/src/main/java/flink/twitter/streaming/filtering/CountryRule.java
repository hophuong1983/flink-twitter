package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;

public class CountryRule implements Rule {

    String country;

    public CountryRule(String country) {
        this.country = country.toLowerCase();
    }

    @Override
    public boolean apply(Tweet tweet) {
        return tweet.getCountryCode().toLowerCase().equals(country);
    }
}
