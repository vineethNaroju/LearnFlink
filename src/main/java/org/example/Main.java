package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


// nc -lk 9999
public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        System.out.println(env.getConfig());

        // DataStream<String> text = env.fromElements("hello", "world", "123", "456");


        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String word: sentence.split(" ")) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        // System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());

        env.execute();
    }
}