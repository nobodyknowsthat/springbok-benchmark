package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.springbok.SpringbokDriver;
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
 * @create 2022-06-28 1:37 PM
 **/
public class SpringbokBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        String dataFile = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_100w.csv";

        // This is a different PortoTaxiRealData from springbok jar
        PortoTaxiRealData portoTaxiRealData;

        @Setup(Level.Invocation)
        public void setupData() {
            portoTaxiRealData = new PortoTaxiRealData(dataFile);
        }

        @TearDown(Level.Invocation)
        public void close() {
            portoTaxiRealData.close();
        }
    }

    @Fork(1)
    @Warmup(iterations = 0)
    @Benchmark
    @Measurement(time = 20, iterations = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void insert(Blackhole blackhole, BenchmarkState state) {
        PortoTaxiRealData data = state.portoTaxiRealData;
        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        int count = 0;
        while ((point = data.nextPointFromPortoTaxis()) != null) {
            count++;
            dataBatch.add(point);
            if (dataBatch.size() == 2000) {
                SpringbokDriver.insertData(dataBatch);
                dataBatch.clear();
            }
            blackhole.consume(dataBatch);
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
            }
        }
        SpringbokDriver.insertData(dataBatch);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SpringbokBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }


}
