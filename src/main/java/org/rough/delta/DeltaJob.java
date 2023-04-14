package org.rough.delta;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.util.Collections;

public class DeltaJob {

    public static void main(String[] args) {
        DeltaJob dj = new DeltaJob();

        try {
            dj.play();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void play() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path("hdfs://localhost:9000/delta"),
                        new Configuration(),
                        new RowType(Collections.singletonList(
                                new RowType.RowField("val", new IntType())
                        ))
                )
                .build();

        env.addSource(new DeltaSourceFunction())
                .sinkTo(deltaSink)
                .name("numbers_sink");

        env.execute();
    }
}

/*
 * The current version only supports Flink Datastream API.
 * Support for Flink Table API / SQL, along with Flink Catalog's implementation
 * for storing Delta table's metadata in an external metastore, are planned
 * to be added in a future release.
 *
 * https://docs.delta.io/latest/delta-storage.html#hdfs
 *
 * https://github.com/delta-io/connectors/blob/branch-0.5/examples/flink-example
 *
 * */

