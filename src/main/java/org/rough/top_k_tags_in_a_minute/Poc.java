package org.rough.top_k_tags_in_a_minute;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.*;

public class Poc {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        Random random = new Random(new Date().getTime());

        DataGeneratorSource<Integer> gen = new DataGeneratorSource<>(new GeneratorFunction<Long, Integer>() {
            @Override
            public Integer map(Long aLong) throws Exception {
                return random.nextInt(1000);
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(100), Types.INT);

        env.fromSource(gen, WatermarkStrategy.forMonotonousTimestamps(), "random-ints")
                .map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                        return Tuple2.of(integer, 1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, Integer> val) throws Exception {
                        return val.f0;
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) throws Exception {
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                })
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
//                .aggregate(new AggregateFunction<Tuple2<Integer, Integer>, Map<Integer, Integer>, List<Integer>>() {
//                    @Override
//                    public Map<Integer, Integer> createAccumulator() {
//                        return new HashMap<>();
//                    }
//
//                    @Override
//                    public Map<Integer, Integer> add(Tuple2<Integer, Integer> a, Map<Integer, Integer> acc) {
//
//                        acc.computeIfPresent(a.f0, (k, v) -> a.f1 + v);
//                        acc.computeIfAbsent(a.f0, v -> a.f1);
//
//                        return acc;
//                    }
//
//                    @Override
//                    public List<Integer> getResult(Map<Integer, Integer> acc) {
//                        return new ArrayList<>(acc.keySet());
//                    }
//
//                    @Override
//                    public Map<Integer, Integer> merge(Map<Integer, Integer> a, Map<Integer, Integer> b) {
//
//                        for(Map.Entry<Integer, Integer> entry : b.entrySet()) {
//                            a.computeIfPresent(entry.getKey(), (k, v) -> v + entry.getValue());
//                            a.computeIfAbsent(entry.getKey(), k -> entry.getValue());
//                        }
//
//                        return a;
//                    }
//                })
                .print();

        env.execute("poc");
    }
}
