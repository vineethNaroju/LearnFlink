package org.rough.rough;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

public class S3io {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSystem fs = FlinkS3FileSystem.get(URI.create("s3://narojv-emr-dev/interpreter-list"));




        DataStream<String> ingress = env.readTextFile("s3://narojv-emr-dev/interpreter-list");

        ingress.print();

        env.execute();


    }
}
