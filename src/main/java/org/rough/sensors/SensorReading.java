package org.rough.sensors;

public class SensorReading {
    public String id;
    public long timestamp;
    public double temperature;

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "[" + id + "," + timestamp + "," + temperature + "]";
    }
}
