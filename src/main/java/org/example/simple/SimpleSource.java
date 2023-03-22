package org.example.simple;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSource implements SourceFunction<Integer> {
    private volatile boolean isRunning = true;
    private int start = 0;
    private ListState<Integer> state;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (isRunning) {
            for(int i=0; i<10000; i++) {
                ++start;
                if(start == Integer.MAX_VALUE) {
                    start = 0;
                }

                ctx.collect(start);
            }

            ctx.markAsTemporarilyIdle();

            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

    }

}
