package org.example.simple;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleSourceEntry {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.disableOperatorChaining();

        DataStream<Integer> dataStream = env.addSource(new SimpleSource());

        dataStream.print();

        env.execute();

    }
}
