package flink.twitter.streaming.functions;

import flink.twitter.streaming.model.TweetTopic;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class DeduplicationFunction extends RichFlatMapFunction<TweetTopic, TweetTopic> {

    ValueState<Boolean> seen;

    @Override
    public void open(Configuration conf) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
        desc.enableTimeToLive(ttlConfig);
        seen = getRuntimeContext().getState(desc);
    }
    @Override
    public void flatMap(TweetTopic tweetTopic, Collector<TweetTopic> collector) throws Exception {
        if (seen.value() == null) {
            collector.collect(tweetTopic);
            seen.update(true);
        }
    }
}
