#!/usr/bin/env bash

dir_name=`dirname -- "$0"`
parent_dir_name="$dir_name/.."
$FLINK_HOME/bin/flink run -c flink.twitter.streaming.TwitterTrendAnalyzerClient \
                          -p 10 \
                          ${parent_dir_name}/target/flink-twitter-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar \
                          ${parent_dir_name}/conf/dev/twitter_trend_analyzer.conf
