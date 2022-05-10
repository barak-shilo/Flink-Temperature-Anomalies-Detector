/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.barakshilo.utils;

import com.barakshilo.flink.data.Sensor;
import com.barakshilo.flink.data.SensorReading;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * SourceFunction that generates a data stream of readings for multiple sensors.
 *
 * <p>The stream is generated in order.
 */
public class SensorReadingsGenerator implements SourceFunction<SensorReading> {

    private static final Logger logger = LoggerFactory.getLogger(SensorReadingsGenerator.class);

    private volatile boolean running;
    private static final int DEFAULT_READINGS_PER_INTERVAL = 100;
    private static final int DEFAULT_NUM_OF_SENSORS = 2000;
    private static final long DEFAULT_INTERVAL = TimeUnit.MINUTES.toMillis(1);
    public static final int SENSOR_ID_LENGTH = 10;
    private static final double BASE_TEMP = 23.0d;

    private final int totalSensors;
    private final long durationBetweenReadings;

    private Map<Sensor, Double> sensors = new HashMap<>();

//    private ParameterTool config;

    public SensorReadingsGenerator(ParameterTool config) {
        running = true;
        int readingsPerInterval = config.getInt("readings_per_interval", DEFAULT_READINGS_PER_INTERVAL);
        totalSensors = config.getInt("total_sensors", DEFAULT_NUM_OF_SENSORS);
        long samplingWindowInterval = config.getLong("sampling_interval", DEFAULT_INTERVAL);
        durationBetweenReadings = samplingWindowInterval / readingsPerInterval;
        logger.debug("Initializing generator with params: totalSensors={}, readingsPerInterval={}", totalSensors, readingsPerInterval);
    }

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        sensors = createSensors();
        while (running) {
            sensors.keySet().forEach(sensor -> {
                double temp = getCurrentTemp(sensor);
                sensors.put(sensor, temp); //update temperature for sensor
                SensorReading reading = SensorReading.createReading(sensor, System.currentTimeMillis(), temp);
                ctx.collect(reading);
            });

            Thread.sleep(durationBetweenReadings);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private Map<Sensor, Double> createSensors() {
        for (int i = 0; i < totalSensors; i++) {
            Sensor s = Sensor.newSensor(generateSensorId(SENSOR_ID_LENGTH));
            sensors.put(s, BASE_TEMP); //Initialize sensors with base temperature
        }
        return sensors;
    }

    private double getCurrentTemp(Sensor sensor) {
        return sensors.get(sensor) + (delta() / 10);
    }

    private double delta() {
        return ThreadLocalRandom.current().nextInt() > 0 ?
                ThreadLocalRandom.current().nextGaussian() :
                -ThreadLocalRandom.current().nextGaussian();
    }

    private String generateSensorId(int length) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'

        return ThreadLocalRandom.current().ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
