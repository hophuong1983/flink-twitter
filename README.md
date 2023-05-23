# flink-twitter
## Introduction
The objective of this project is to many messages per period the top trends subjects at Twitter. Since Twitter closed its free API, we will get Twitter stream via PubNub.

## Prerequisites
### Download and run Flink
1. Download flink version 1.16.1
2. Set up following environment variables in file `~/.profile_bash`.
```
export FLINK_HOME=[Path to Flink folder]
```
3. Start Flink cluster
```
cd $FLINK_HOME
bin/start-cluster.sh
```
### Download source code
Clone the project
```
git clone https://github.com/hophuong1983/flink-twitter.git
```
Build jar file for streaming client
```
cd [Path to flink-twitter]
cd flink-twitter-streaming
mvn install
```
## Deliveries
### Phase 1
#### Objective
Create a client that connects with PubNub.
#### Implementation
I created a demo client that consume tweets and print out all of them. 
#### How to run
Check out the tag and build the jar file
```
cd [Path to flink-twitter]
git checkout phase_1
cd flink-twitter-streaming/
mvn install
```
Run the Flink client
```
bash cmd/run_twitter_trend_analyzer_client.sh 
```
Check the tweets output on the TaskManager logs on Flink dashboard.
```
8> Tweet{id='1623721370809933824', timestampMs=1675960316742, text='@haluklevent  abi siyaseti sevmiyorsun biliyorum amma seçim için hazineden aldıkları paranın birazını sosyal soruml… https://t.co/g7S50HvCtV', userName='Ramazan AZAK', countryCode='TR', hashTags=[]}
9> Tweet{id='1623721371195912195', timestampMs=1675960316834, text='Hoy es MJ 
Bakán', userName='Pasas al Ron ₪ ø lll ·o.', countryCode='CL', hashTags=[]}
10> Tweet{id='1623721371439185929', timestampMs=1675960316892, text='Memories 💔', userName='يُمنىٰ', countryCode='EG', hashTags=[]}
```
### Phase 2
#### Objective
Implement a client able to consume the 3 (default) top trends in the Netherlands (default)
#### Implementation
Since PubNub doesn't provide the service to get top trends in Twitter or the one to consume a specific topic, 
I have to get the top trend from internet and do filtering in the code.
I created a demo client that consume tweets, filter to get top trend
tweets and print out all of them. 
#### How to run
Check out the tag and build the jar file
```
cd [Path to flink-twitter]
git checkout phase_2
cd flink-twitter-streaming/
mvn install
```
Run the Flink client
```
bash cmd/run_twitter_trend_analyzer_client.sh
```
Check the tweets output on the TaskManager logs on Flink dashboard. <br>
Note that using PubNub tweets are consumed from all around the world with low throughput,
there may be no output with the real top trends in some period.
We could change topics in file `conf/dev/twitter_trend_analyzer.conf`
to have some output.

```agsl
...
twitter {
    filtering {
        ...
        topic.filter {
            class.name = "RelaxedTopicRule"
            topics = ["UtrSpa", "m", "n"]
        }
    }
}
```
Output examples:
```
5> TweetTopic{topic='n', id='1623653554786279426', timestampMs=1675944148142}
4> TweetTopic{topic='n', id='1623661633401679872', timestampMs=1675946074234}
4> TweetTopic{topic='m', id='1623661633401679872', timestampMs=1675946074234}
```
### Phase 3
#### Objective
Implement a client that connects with Twitter stream and calculates (on your consumer) how many tweets we have every 1, 5 and 10 minutes for a trend topic. 
Build Rest API to get those information and make topic name and window sizes configurable.
#### How to run
##### Streaming application
Check out the tag and build the jar file
```
cd [Path to flink-twitter]
git checkout phase_3
cd flink-twitter-streaming/
mvn install
```
Run redis server / cluster. <br>
Example: On MacOS
```agsl
brew services start redis
```
Run the Flink client
```
bash cmd/run_twitter_trend_analyzer_client.sh
```
##### Web application
Open a new terminal, build the jar file for the web application:
```
cd [Path to flink-twitter]
git checkout phase_3
cd topic_count_web/
mvn install
```
Run the web application
```agsl
java -jar target/topic_count_web-1.0-SNAPSHOT.jar
```
##### Check REST API output
Open a new terminal, check API output
```agsl
curl http://localhost:8082/api/topics
["UtrSpa","m","n"]
curl http://localhost:8082/api/windows
[1, 5, 10]
curl http://localhost:8082/api/topics/m/1
6
```
### Phase 4
#### Objective
Sync these aggregations on a key-value storage using the time period + the time leap as a key
#### How to run
Check out the tag and build the jar file
```
cd [Path to flink-twitter]
git checkout phase_4
cd flink-twitter-streaming/
mvn install
```
Run redis server / cluster. <br>
Example: On MacOS
```agsl
brew services start redis
```
Run the Flink client
```
bash cmd/run_twitter_trend_analyzer_client.sh
```
Check output on redis client:
```
redis-cli
127.0.0.1:6379> hgetall twitter_topic_flink_multi_topic
 1) "1675960260000-1"
 2) "[{\"topic\":\"m\",\"count\":6}]"
 3) "1675960320000-1"
 4) "[{\"topic\":\"m\",\"count\":5}]"
 5) "1675960380000-1"
 6) "[{\"topic\":\"m\",\"count\":5},{\"topic\":\"n\",\"count\":1}]"
 7) "1675960440000-1"
 8) "[{\"topic\":\"n\",\"count\":1},{\"topic\":\"m\",\"count\":5}]"
 9) "1675960500000-1"
10) "[{\"topic\":\"m\",\"count\":4}]"
11) "1675960500000-5"
12) "[{\"topic\":\"n\",\"count\":2},{\"topic\":\"m\",\"count\":25}]"
```

