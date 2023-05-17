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
Run `TwitterToStdoutClient`
```
cd [Path to flink-twitter]
bash cmd/run_twitter_to_stdout_client.sh 
```
Check the tweets output on the TaskManager logs on Flink dashboard.
```
6> Tweet{id='1623646635409526785', timestampMs=1675942498434, text='@KimberlyGuima10 Verdade linda, concordo', userName='luazinha', country='Brasil', hashTags=[]}
7> Tweet{id='1623646635963154439', timestampMs=1675942498566, text='@vaboiredujuusla WWWWAAAAAAAA', userName='lagrange (安らかに眠るw)', country='France', hashTags=[]}
8> Tweet{id='1623646636139286529', timestampMs=1675942498608, text='@marianaspinelIi Isso sim.', userName='Dias melhores.', country='Brasil', hashTags=[]}
```