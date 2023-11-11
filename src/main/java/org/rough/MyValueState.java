package org.rough;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Random;


//
public class MyValueState {

    public static void main(String[] args) throws Exception {

        long chkMilliSeconds = 1000;
        long delayMilliSeconds = 1000;

        for(String key : args) {
            if(key.startsWith("chk=")) {
                chkMilliSeconds = Long.parseLong(key.substring(4));
            } else if (key.startsWith("dly=")) {
                delayMilliSeconds = Long.parseLong(key.substring(4));
            }
        }

        final Configuration config = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.disableOperatorChaining();
        env.enableCheckpointing(60000);

        long finalDelayMilliSeconds = delayMilliSeconds;
        GeneratorFunction<Long, Integer> generatorFunction = new GeneratorFunction<Long, Integer>() {
            final Random random = new Random();

            @Override
            public Integer map(Long aLong) throws Exception {
                Thread.sleep(finalDelayMilliSeconds);
                return random.nextInt(1000);
            }
        };

        DataGeneratorSource<Integer> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                Types.INT
        );

        env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "RandomUnder1000")
            .keyBy(val -> val)
            .flatMap(new WindowAverage(chkMilliSeconds))
            .print();

        env.execute("MyValueState");
    }


}

class WindowAverage extends RichFlatMapFunction<Integer, Integer>  implements CheckpointedFunction {
    private transient ValueState<Integer> sum;
    private transient ValueState<Integer> count;

    private final long chkMilliSeconds;

    public WindowAverage(long chkMilliSeconds) {
        this.chkMilliSeconds = chkMilliSeconds;
    }

    // sleeptime,value,repeat
    @Override
    public void flatMap(Integer input, Collector<Integer> collector) throws Exception {
        
        // Thread.sleep(input.sleepTime)

        if(sum.value() == null) {
            sum.update(input);
        } else {
            sum.update(sum.value() + input);
        }

        if(count.value() == null) {
            count.update(1);
        } else {
            count.update(count.value() + 1);
        }

        // for loop repeat times {collector}
        collector.collect(sum.value() / count.value());
    }

    @Override
    public void open(Configuration parameters)  {
        sum = getRuntimeContext()
            .getState(new ValueStateDescriptor<>("sum", Integer.class));
        count = getRuntimeContext()
            .getState(new ValueStateDescriptor<>("count", Integer.class));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        Thread.sleep(chkMilliSeconds);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
