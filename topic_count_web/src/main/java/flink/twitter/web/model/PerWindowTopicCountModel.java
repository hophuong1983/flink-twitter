package flink.twitter.web.model;

import flink.twitter.web.bean.RedisConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class PerWindowTopicCountModel {
    @Autowired
    private RedisConfig redisConfig;

    @Autowired
    private RedisTemplate template;

    public String findAllTopics() {
        return (String) template.opsForHash().get(redisConfig.getRedisHashKey(), "topics");
    }

    public String getCountPerTopicWindow(String topic, int window) {
        String key = topic + "-" + window;
        Object count = template.opsForHash().get(redisConfig.getRedisHashKey(), key);
        if (template.opsForHash().get(redisConfig.getRedisHashKey(), key) == null) {
            return "Not exists or not ready yet";
        } else {
            return (String) count;
        }
    }
}
