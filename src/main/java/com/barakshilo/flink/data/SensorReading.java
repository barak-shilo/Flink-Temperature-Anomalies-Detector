package com.barakshilo.flink.data;

public final class SensorReading {
    private final Sensor sensor;
    private final long timestamp;
    private final double value;

    public static SensorReading createReading(Sensor sensor, long timestamp, Double temp) {
        return new SensorReading(sensor, timestamp, temp);
    }

    private SensorReading(Sensor sensor, long timestamp, double value) {
        this.sensor = sensor;
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public String getSensorId() {
        return sensor.getId();
    }

    @Override
    public String toString() {
        return "sensor=" + sensor +
                ", timestamp=" + timestamp +
                ", value=" + value;
    }
}
