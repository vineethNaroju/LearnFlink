package org.example.file;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class WordCount {
    public static void main(String[] args) throws Exception {


        final String inputDir = "s3://narojv-emr-dev/autoscaler/input"; // "/Users/narojv/input";

        final String outputDir = "s3://narojv-emr-dev/autoscaler/output"; // "/Users/narojv/output;

        final FileSource<String> fileSource = FileSource.
                <String>forRecordStreamFormat(new TextLineInputFormat(), new Path(inputDir))
                .monitorContinuously(Duration.ofSeconds(1L))
                .build();

        final FileSink<Tuple2<String, Integer>> fileSink = FileSink
                .<Tuple2<String, Integer>>forRowFormat(new Path(outputDir), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(MemorySize.ofMebiBytes(100))
                        .build())
                .build();

        Configuration config = new Configuration();

        config.set(StreamFormat.FETCH_IO_SIZE, MemorySize.ofMebiBytes(1L));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.disableOperatorChaining();

        final DataStream<Tuple2<String, Integer>> stream = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "File Source")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] tokens = sentence.toLowerCase().split("\\W+");

                        for (String word: tokens) {
                            if(word.length() > 0)
                                collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        stream.sinkTo(fileSink);

        env.execute();
    }
}
