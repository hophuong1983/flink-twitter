package flink.twitter.streaming.filtering;

import com.typesafe.config.Config;

import java.util.List;
import java.util.stream.Collectors;

public class RuleFactory {

    public static CountryCodeRule createCountryCodeRule(Config countryFilterConf) {
        return new CountryCodeRule(countryFilterConf.getString("country.code"));
    }

    public static List<TopicRule> createTopicRule(Config topicFilterConf) {
        List<String> topics = topicFilterConf.getStringList("topics");
        return topics.stream().map(topic -> {
            switch (topicFilterConf.getString("class.name")) {
                case "RelaxedTopicRule":
                    return new RelaxedTopicRule(topic);
                case "StrictTopicRule":
                    return new StrictTopicRule(topic);
                default:
                    throw new UnsupportedOperationException("Not support topic class " + topicFilterConf.getString("class.name"));
            }
        }).collect(Collectors.toList());
    }
}
