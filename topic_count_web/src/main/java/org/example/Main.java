package org.example;

import redis.clients.jedis.Jedis;

public class Main {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost");
        System.out.println("Stored string in redis:: "+ jedis.hget("tweet_topic_flink", "m"));
        System.out.println("Stored string in redis:: "+ jedis.hget("tweet_topic_flink", "n"));

    }
}