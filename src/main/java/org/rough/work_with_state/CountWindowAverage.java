package org.rough.work_with_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CountWindowAverage
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {

    }
}
