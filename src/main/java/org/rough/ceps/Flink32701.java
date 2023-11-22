package org.rough.ceps;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Flink32701 {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.enableCheckpointing(120_000); // 2 minutes
        env.disableOperatorChaining();

        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "a";
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(10), Types.STRING);

        DataStream<String> inputStream = env.fromSource(generatorSource, WatermarkStrategy.forMonotonousTimestamps(), "strings");

        Pattern<String, ?> pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(val -> val.equals("a")))
                .notFollowedBy("next")
                .where(SimpleCondition.of(val -> val.equals("b")))
                .within(Time.of(2, TimeUnit.MINUTES));

        PatternStream<String> resultStream = CEP.pattern(inputStream, pattern).inProcessingTime();

        resultStream.process(new PatternProcessFunction<String, String>() {
            @Override
            public void processMatch(Map<String, List<String>> map, Context context, Collector<String> collector) throws Exception {

                String str = "MAP CONTENTS \n";

                for(Map.Entry<String, List<String>> entry : map.entrySet()) {
                    str += entry.getKey();
                    str += " => ";
                    for(String temp : entry.getValue()) {
                        str += temp;
                        str += " , ";
                    }
                    str += "\n";
                }

                collector.collect(str);
            }
        }).print().name("sink");

        env.execute("testing");
    }
}
