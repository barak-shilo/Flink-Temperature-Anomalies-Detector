# Flink Temperature Anomalies Detector

A flink job that detects anomalies in the temperature readings of an imaginary IOT device.

## Description

The job gets the temperature measurements from a SourceFunction that generates ~100  temperature measurements in 1 minute.

The system gets data from 2000 devices.

An abnormal measurement is defined as one that is 3 standard deviations away from the 1 minute average.

Detected anomalies are printed to the console.

Example: deviceId=7US9lSCQs6, measurement=22.27 C, time=1652202059736

## Getting Started

### Build/Run

#### Run locally

```
gradle run
```

#### Submit to a Flink cluster

* Build:

```
gradle shadowJar
```

* Submit job:
```
flink run build/libs/Flink-Temperature-Anomalies-Detector-1.0-fat.jar