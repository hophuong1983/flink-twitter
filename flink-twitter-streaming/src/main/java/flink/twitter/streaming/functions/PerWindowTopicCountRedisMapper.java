package flink.twitter.streaming.functions;

import flink.twitter.streaming.model.PerWindowTopicCount;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class PerWindowTopicCountRedisMapper implements RedisMapper<PerWindowTopicCount> {

    String hashKey;

    public PerWindowTopicCountRedisMapper(String hashKey) {
        this.hashKey = hashKey;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, hashKey);
    }

    @Override
    public String getKeyFromData(PerWindowTopicCount data) {
        return data.getTopic() + "-" + data.getWindowSizeMin();
    }

    @Override
    public String getValueFromData(PerWindowTopicCount data) {
        return Long.toString(data.getCount());
    }
}
