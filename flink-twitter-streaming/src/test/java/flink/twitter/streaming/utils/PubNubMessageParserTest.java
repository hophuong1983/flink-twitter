package flink.twitter.streaming.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.pubnub.api.models.consumer.pubsub.BasePubSubResult;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class PubNubMessageParserTest {

    @Test
    void convertToTweet() throws IOException {
        byte[] payloadBytes = this.getClass().getClassLoader()
                .getResourceAsStream("twitter_payload.json")
                .readAllBytes();
        String payload = new String(payloadBytes);

        JsonElement msg = new JsonParser().parseString(payload);
        System.out.println(PubNubMessageParser.convertToTweet(msg));
    }

}