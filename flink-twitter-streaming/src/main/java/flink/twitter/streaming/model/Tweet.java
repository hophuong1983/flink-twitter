package flink.twitter.streaming.model;

import java.io.Serializable;
import java.util.Arrays;

public class Tweet implements Serializable {

    String id;
    long timestampMs;
    String text;
    String userName;
    String country;
    String[] hashTags;

    public Tweet(String id, long timestampMs, String text, String userName, String country, String[] hashTags) {
        this.id = id;
        this.timestampMs = timestampMs;
        this.text = text;
        this.userName = userName;
        this.country = country;
        this.hashTags = hashTags;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "id='" + id + '\'' +
                ", timestampMs=" + timestampMs +
                ", text='" + text + '\'' +
                ", userName='" + userName + '\'' +
                ", country='" + country + '\'' +
                ", hashTags=" + Arrays.toString(hashTags) +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String[] getHashTags() {
        return hashTags;
    }

    public void setHashTags(String[] hashTags) {
        this.hashTags = hashTags;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }
}
