package org.rough.file;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class FileSourceEntry {

    public static void main(String[] args) throws Exception {


        Path directory = new Path("/Users/narojv/input");


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        final FileSource<String> fileSource = FileSource.
                <String>forRecordStreamFormat(new TextLineInputFormat(), directory)
                .monitorContinuously(Duration.ofSeconds(1L))
                .build();

        final DataStream<String> stream = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "FileSource");


        stream.print();

        env.execute();
    }
}
