package com.barakshilo.flink.data;

public final class Sensor {
    private final String id;

    public static Sensor newSensor(String id) {
        return new Sensor(id);
    }

    private Sensor(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }
}
