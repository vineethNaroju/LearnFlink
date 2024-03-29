package org.rough.sensors;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

    public SensorTimeAssigner() {
        super(Time.seconds(5));
    }

    @Override
    public long extractTimestamp(SensorReading sensorReading) {
        return sensorReading.timestamp;
    }
}
