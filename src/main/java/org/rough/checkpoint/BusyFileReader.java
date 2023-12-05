package org.rough.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Random;


/*
* https://github.com/apache/flink/blob/d351c5bd9c1f28a3e5ffe98fb549c1b94618485b/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.java#L1667
*
* taskmanager.network.detailed-metrics
* taskmanager.network.memory.buffer-debloat.enabled
* taskmanager.network.memory.buffers-per-channel
* taskmanager.network.memory.floating-buffers-per-gate
* taskmanager.network.memory.max-buffers-per-channel
* taskmanager.network.memory.max-overdraft-buffers-per-gate
* taskmanager.network.netty.sendReceiveBufferSize
*
*
*
* */

public class BusyFileReader {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        Path dir = new Path("/Users/narojv/rough/data");

        Random r = new Random();

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(),  dir)
                .monitorContinuously(Duration.of(1, ChronoUnit.SECONDS))
                .build();

        env.disableOperatorChaining();
        env.enableCheckpointing(120_000);

        env.fromSource(fileSource, WatermarkStrategy.forMonotonousTimestamps(), "FileSource")
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String s) throws Exception {
                        Thread.sleep(10000);
                        return r.nextInt(1000);
                    }
                })
                .addSink(new DiscardingSink<>());
        env.execute("repro");
    }
}
