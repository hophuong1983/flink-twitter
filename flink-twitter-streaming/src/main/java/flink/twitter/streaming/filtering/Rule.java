package flink.twitter.streaming.filtering;

import flink.twitter.streaming.model.Tweet;

import java.io.Serializable;

public interface Rule extends Serializable {

    boolean apply(Tweet tweet);
}
