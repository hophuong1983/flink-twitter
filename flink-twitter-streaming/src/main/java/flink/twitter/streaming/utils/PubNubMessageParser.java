package flink.twitter.streaming.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import flink.twitter.streaming.model.Tweet;

import java.util.ArrayList;
import java.util.List;

public class PubNubMessageParser {

    public static Tweet convertToTweet(PNMessageResult messageResult) {
        return convertToTweet(messageResult.getMessage());
    }

    static Tweet convertToTweet(JsonElement message) {
        JsonObject msg = message.getAsJsonObject();
        String country = null;
        if (msg.get("place") != null) {
            country = msg.get("place").getAsJsonObject().get("country").getAsString();
        }

        long timestampMs = msg.get("timestamp_ms").getAsLong();
        String text = msg.get("text").getAsString();
        String id = msg.get("id_str").getAsString();
        String userName = msg.get("user").getAsJsonObject().get("name").getAsString();

        List<String> hashTags = new ArrayList();
        if (msg.get("entities") != null) {
            JsonArray hashTagsArr = msg.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray();
            for (JsonElement ele : hashTagsArr) {
                hashTags.add(ele.getAsJsonObject().get("text").getAsString());
            }
        }

        return new Tweet(id, timestampMs, text, userName, country, hashTags.toArray(new String[0]));
    }
}
