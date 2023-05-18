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
I created a demo client that consume tweets and print out all of them: `TwitterToStdoutClient`. 
#### How to run
Check out the tag and build the jar file
```
cd [Path to flink-twitter]
git checkout phase_1
cd flink-twitter-streaming/
mvn install
```
Run `TwitterToStdoutClient`
```
bash cmd/run_twitter_trend_analyzer_client.sh 
```
Check the tweets output on the TaskManager logs on Flink dashboard.
```
8> Tweet{id='1623721370809933824', timestampMs=1675960316742, text='@haluklevent  abi siyaseti sevmiyorsun biliyorum amma seçim için hazineden aldıkları paranın birazını sosyal soruml… https://t.co/g7S50HvCtV', userName='Ramazan AZAK', countryCode='TR', hashTags=[]}
9> Tweet{id='1623721371195912195', timestampMs=1675960316834, text='Hoy es MJ 
Bakán', userName='Pasas al Ron ₪ ø lll ·o.', countryCode='CL', hashTags=[]}
10> Tweet{id='1623721371439185929', timestampMs=1675960316892, text='Memories 💔', userName='يُمنىٰ', countryCode='EG', hashTags=[]}
1```