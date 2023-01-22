package org.example.sensors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageSensorReadings {

    public static void main(String[] args) throws  Exception {


        Configuration config = new Configuration();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // happens by default post 1.12
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> sensorStream = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());



        DataStream<SensorReading> avgTemp = sensorStream
                .map(val -> new SensorReading(val.id, val.timestamp, (val.temperature - 32) * (5/9)))
                .keyBy(val -> val.id)
                .timeWindow(Time.seconds(1))
                .apply(new AverageTemperature());


        avgTemp.print();

        env.execute("compute average sensor temperature");
    }


    public static class AverageTemperature implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        @Override
        public void apply(String sensorId, TimeWindow timeWindow, Iterable<SensorReading> iterable,
                          Collector<SensorReading> collector) throws Exception {
            System.out.println("timeWindow:" + timeWindow);

            int cnt = 0;
            double sum = 0.0;


            for(SensorReading sensorReading : iterable) {
                ++cnt;
                sum += sensorReading.temperature;
            }

            double averageTemp = sum / cnt;

            collector.collect(new SensorReading(sensorId, timeWindow.getEnd(), averageTemp));
        }
    }
}
