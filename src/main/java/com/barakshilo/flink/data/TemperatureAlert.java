package com.barakshilo.flink.data;

public final class TemperatureAlert {
    private final String deviceId;
    private final String measurement;
    private final long time;

    public static TemperatureAlert generateAlert(String id, long time, Double temp) {
        return new TemperatureAlert(id, String.format("%.2f C", temp), time);
    }

    private TemperatureAlert(String sensor, String measurement, long time) {
        this.deviceId = sensor;
        this.measurement = measurement;
        this.time = time;
    }

    @Override
    public String toString() {
        return "deviceId=" + deviceId +
                ", measurement=" + measurement +
                ", time=" + time
                ;
    }
}
