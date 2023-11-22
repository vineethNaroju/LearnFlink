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
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SimpleCep {

    static Logger logger = LoggerFactory.getLogger(SimpleCep.class);

    public static void main(String[] args) throws Exception {

        final Configuration config = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);


        env.disableOperatorChaining();

        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "a";
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.STRING);

        DataStream<String> inputStream = env.fromSource(generatorSource, WatermarkStrategy.forMonotonousTimestamps(), "strings").forward();

//        DataStream<String> inputStream = env.fromElements("a", "b", "a", "c", "a");

        Pattern<String, ?> startPattern = Pattern.<String>begin("start").where(SimpleCondition.of(val -> val.equals("a")));

        PatternStream<String> patternStream = CEP.pattern(inputStream, startPattern).inProcessingTime();

        DataStream<List<String>> resultStream = patternStream.process(new PatternProcessFunction<String, List<String>>() {
            @Override
            public void processMatch(Map<String, List<String>> map, Context context, Collector<List<String>> collector) throws Exception {
                // logger.info(map.keySet().toString());
                collector.collect(map.get("start"));
            }
        });

        resultStream.print();

        env.execute("simple pattern match [a]");
    }
}
