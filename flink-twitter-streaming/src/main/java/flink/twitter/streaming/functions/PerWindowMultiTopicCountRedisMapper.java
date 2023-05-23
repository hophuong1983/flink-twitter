package flink.twitter.streaming.functions;

import flink.twitter.streaming.model.PerWindowMultiTopicCount;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class PerWindowMultiTopicCountRedisMapper implements RedisMapper<PerWindowMultiTopicCount> {

    String hashKey;

    transient ObjectMapper objectMapper = new ObjectMapper();

    public PerWindowMultiTopicCountRedisMapper(String hashKey) {
        this.hashKey = hashKey;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, hashKey);
    }

    @Override
    public String getKeyFromData(PerWindowMultiTopicCount data) {
        return data.getWatermarkTimeMs() + "-" + data.getWindowSizeMin();
    }

    @Override
    public String getValueFromData(PerWindowMultiTopicCount data) {
        try {
            return objectMapper.writeValueAsString(data.getTopicCounts());
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
