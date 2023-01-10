package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.timestream.TimestreamStorageDriver;
import com.anonymous.test.common.TrajectoryPoint;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author anonymous
 * @create 2022-06-07 2:30 PM
 **/
@Deprecated
public class TimestreamBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        String filename = "/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample.csv";

        public String DATABASE_NAME = "benchmark";

        private long HT_TTL_HOURS = 1;

        private long CT_TTL_DAYS = 1;

        public String TABLE_NAME = "trajectoryForInsertion";

        PortoTaxiRealData portoTaxiRealData;

        @Setup(Level.Invocation)
        public void setupData() {
            TimestreamStorageDriver.createTable(DATABASE_NAME, TABLE_NAME, HT_TTL_HOURS, CT_TTL_DAYS);
            portoTaxiRealData = new PortoTaxiRealData(filename);
        }

        @TearDown(Level.Invocation)
        public void close() {
            if (portoTaxiRealData != null) {
                portoTaxiRealData.close();
            }
            TimestreamStorageDriver.deleteTable(DATABASE_NAME, TABLE_NAME);
        }

    }

    @Fork(1)
    @Warmup(iterations = 0)
    @Benchmark
    @Measurement(time = 20, iterations = 3)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insert(Blackhole blackhole, BenchmarkState state) {
        PortoTaxiRealData data = state.portoTaxiRealData;
        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        while ((point = data.nextPointFromPortoTaxis()) != null) {
            dataBatch.add(point);
            if (dataBatch.size() == 10000) {
                TimestreamStorageDriver.writeMultiMeasureRecords(dataBatch, state.DATABASE_NAME, state.TABLE_NAME);
                dataBatch.clear();
            }
            blackhole.consume(dataBatch);
        }
        TimestreamStorageDriver.writeMultiMeasureRecords(dataBatch, state.DATABASE_NAME, state.TABLE_NAME);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TimestreamBenchmark.class.getSimpleName())
                //.output("/home/anonymous/IdeaProjects/trajectory-index/benchmark-log/headchunk/semi/semi-insertion.log")
                .build();

        new Runner(opt).run();
    }
}
