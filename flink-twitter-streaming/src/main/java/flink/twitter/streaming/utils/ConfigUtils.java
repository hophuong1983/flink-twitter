package flink.twitter.streaming.utils;

import com.typesafe.config.Config;

import java.util.Properties;

public class ConfigUtils {

    public static Properties propsFromConfig(Config config) {

        Properties props = new Properties();
        config.entrySet().forEach(entry -> {
            props.put(entry.getKey(), entry.getValue().unwrapped());
        });

        return props;
    }
}
