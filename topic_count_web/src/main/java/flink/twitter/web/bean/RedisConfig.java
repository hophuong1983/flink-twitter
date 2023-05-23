package flink.twitter.web.bean;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {
    @Value("${redis.host}")
    private String redisHost;

    @Value("${redis.port}")
    private int redisPort;

    @Value("${redis.hash.key}")
    private String redisHashKey;

    @Bean
    public LettuceConnectionFactory createConnectionFactory() {
        
        RedisStandaloneConfiguration conf = new RedisStandaloneConfiguration(redisHost, redisPort);
        LettuceConnectionFactory conFactory = new LettuceConnectionFactory(conf);

        conFactory.afterPropertiesSet();
        return conFactory;
    }

    @Bean
    @Primary
    public RedisTemplate<String, String> createRedisTemplate(RedisConnectionFactory connectionFactory) {

        RedisTemplate<String, String> template = new RedisTemplate();
        template.setConnectionFactory(connectionFactory);

        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());

        template.afterPropertiesSet();

        return template;
    }

    public String getRedisHashKey() {
        return redisHashKey;
    }
}