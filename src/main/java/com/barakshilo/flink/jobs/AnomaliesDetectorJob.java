package com.barakshilo.flink.jobs;

import com.barakshilo.flink.data.SensorReading;
import com.barakshilo.flink.data.TemperatureAlert;
import com.barakshilo.utils.SensorReadingsGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AnomaliesDetectorJob {

    private static final Logger logger = LoggerFactory.getLogger(AnomaliesDetectorJob.class);

    private final SourceFunction<SensorReading> source;
    private final SinkFunction<TemperatureAlert> sink;

    private final Time windowTime;

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        ParameterTool config = getConfiguration(args);
        AnomaliesDetectorJob job = new AnomaliesDetectorJob(new SensorReadingsGenerator(config)
                , new PrintSinkFunction<>("temperature_alerts", false), config);
        job.execute();
    }

    /**
     * Creates a job using the source and sink provided.
     */
    public AnomaliesDetectorJob(
            SourceFunction<SensorReading> source, SinkFunction<TemperatureAlert> sink
            , ParameterTool config) {
        this.source = source;
        this.sink = sink;

        long samplingInterval = config.getLong("sampling_interval", TimeUnit.MINUTES.toMillis(1));
        this.windowTime = Time.milliseconds(samplingInterval);
    }

    /**
     * Create and execute the temperature anomalies detector pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the readings generator and set up watermarking
        DataStream<SensorReading> readings =
                env.addSource(source)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (reading, t) -> reading.getTimestamp()))
                        .name("Sensor readings");

        // group readings into a window (by sensorId) and filter for abnormal readings
        DataStream<SensorReading> abnormalReadings =
                readings.keyBy(SensorReading::getSensorId)
                        .window(TumblingEventTimeWindows.of(windowTime))
                        .process(new FilterReadings())
                        .name("Abnormal sensor readings");

        // generate alerts and set up the sink pipeline
        abnormalReadings
                .map(new GenerateAlerts())
                .addSink(sink);

        // execute the pipeline
        String jobName = "Temperature anomalies";
        logger.info("Executing job: {}", jobName);
        return env.execute(jobName);
    }

    private static class FilterReadings
            extends ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow> {
        @Override
        public void process(String key, Context context,
                            Iterable<SensorReading> readings, Collector<SensorReading> out) {
            List<SensorReading> abnormalReadings = filterReadings(readings);
            for (SensorReading r : abnormalReadings)
                out.collect(r);
        }
    }

    private static class GenerateAlerts implements MapFunction<SensorReading, TemperatureAlert> {
        @Override
        public TemperatureAlert map(SensorReading r) {
            return TemperatureAlert.generateAlert(r.getSensorId(), r.getTimestamp(), r.getValue());
        }
    }

    /**
     * Filter out all readings that are withing the normal bounds (using three-sigma)
     *
     * @param iterable all readings
     * @return List of abnormal readings
     */
    private static List<SensorReading> filterReadings(Iterable<SensorReading> iterable) {
        List<SensorReading> readings = new ArrayList<>();
        iterable.forEach(readings::add);

        double mean = readings.stream().mapToDouble(SensorReading::getValue).average().orElse(0);
        double variance = readings.stream().mapToDouble(r -> Math.pow((r.getValue() - mean), 2)).sum() / readings.size();

        Tuple2<Double, Double> bounds = calcThreeSigmaBounds(mean, variance);

        return readings.stream()
                .filter(r -> isAbnormalReading(r, bounds.f0, bounds.f1))
                .collect(Collectors.toList());
    }

    private static Tuple2<Double, Double> calcThreeSigmaBounds(double mean, double variance) {
        double deviation = Math.sqrt(variance);
        double threeSigma = 3 * deviation;

        double lowerBound = mean - threeSigma;
        double upperBound = mean + threeSigma;

        return Tuple2.of(lowerBound, upperBound);
    }

    private static boolean isAbnormalReading(SensorReading r, double lowerBound, double upperBound) {
        return r.getValue() > upperBound || r.getValue() < lowerBound;
    }

    private static ParameterTool getConfiguration(String[] args) throws IOException {
        ParameterTool paramsFromArgs = ParameterTool.fromArgs(args);
        ParameterTool config;
        if (paramsFromArgs.has("config-path"))
            config = ParameterTool.fromPropertiesFile(paramsFromArgs.get("config-path"));
        else //No config-path provided - use default
            config = ParameterTool.fromPropertiesFile(AnomaliesDetectorJob.class.getClassLoader().getResourceAsStream("conf/jobConfig.properties"));
        return config;
    }
}