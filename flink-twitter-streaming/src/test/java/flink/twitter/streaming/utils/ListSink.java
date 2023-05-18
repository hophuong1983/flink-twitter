package flink.twitter.streaming.utils;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListSink<T> implements SinkFunction<T> {

    public static final List outputList = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(T value, Context context) throws Exception {
        outputList.add(value);
    }
}
