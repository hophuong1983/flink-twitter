package flink.twitter.web.model;

import flink.twitter.web.bean.RedisConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class WindowModel {

    @Autowired
    private RedisConfig redisConfig;

    @Autowired
    private RedisTemplate template;

    public String findAllWindows() {
        return (String) template.opsForHash().get(redisConfig.getRedisHashKey(), "windows");
    }
}
