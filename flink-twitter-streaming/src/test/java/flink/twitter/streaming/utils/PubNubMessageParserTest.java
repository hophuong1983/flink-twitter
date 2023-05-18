package flink.twitter.streaming.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import flink.twitter.streaming.model.Tweet;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PubNubMessageParserTest {

    @Test
    void convertToTweet() throws IOException {
        byte[] payloadBytes = this.getClass().getClassLoader()
                .getResourceAsStream("twitter_payload.json")
                .readAllBytes();
        String payload = new String(payloadBytes);

        JsonElement msg = new JsonParser().parseString(payload);
        Tweet expected = new Tweet(
                "1623677806713487361",
                1675949930252L,
                "INDUSTRY MIXER February 18th \uD83E\uDDE8 BE THERE #BGsWORLD @ Out Your League ENT https://t.co/6jNkGPB26x",
                "ATL BABY G",
                "US",
                new String[]{"BGsWORLD"}
        );

        assertEquals(expected, PubNubMessageParser.convertToTweet(msg));
    }

    @Test
    void convertToTweet2() throws IOException {
        String payload = "{\n" +
                "  \"created_at\": \"Thu Feb 09 13:38:50 +0000 2023\",\n" +
                "  \"id\": 1623677806713487400,\n" +
                "  \"id_str\": \"1623677806713487361\",\n" +
                "  \"text\": \"INDUSTRY MIXER February 18th\",\n" +
                "  \"source\": \"<a href=\\\"http://instagram.com\\\" rel=\\\"nofollow\\\">Instagram</a>\",\n" +
                "  \"user\": {\n" +
                "    \"id\": 1002973047081656300,\n" +
                "    \"id_str\": \"1002973047081656321\",\n" +
                "    \"name\": \"ATL BABY G\"" +
                "  },\n" +
                "  \"place\": {\n" +
                "    \"country_code\": \"US\",\n" +
                "    \"country\": \"United States\"" +
                "  },\n" +
                "  \"entities\": {\n" +
                "    \"hashtags\": []" +
                "  },\n" +
                "  \"timestamp_ms\": \"1675949930252\"\n" +
                "}";

        JsonElement msg = new JsonParser().parseString(payload);
        Tweet expected = new Tweet(
                "1623677806713487361",
                1675949930252L,
                "INDUSTRY MIXER February 18th",
                "ATL BABY G",
                "US",
                new String[0]
        );

        assertEquals(expected, PubNubMessageParser.convertToTweet(msg));
    }

}