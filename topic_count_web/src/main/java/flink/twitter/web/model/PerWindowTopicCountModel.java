package flink.twitter.web.model;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class PerWindowTopicCountModel {
    @Autowired
    private RedisTemplate template;
}
