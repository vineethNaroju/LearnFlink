package org.rough.sensors;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;
    int N = 1000;


    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random random = new Random();

        int taskId = this.getRuntimeContext().getIndexOfThisSubtask();

       while(running) {
           long currentTime = Calendar.getInstance().getTimeInMillis();

           for(int i=0; i<N; i++) {

               String sensorId = "sensor_" + taskId + "_" + i;
               double temperature = 65 + (random.nextGaussian() * 20);

               sourceContext.collect(new SensorReading(sensorId, currentTime, temperature));
           }

           // sourceContext.markAsTemporarilyIdle();

           Thread.sleep(10);
       }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
