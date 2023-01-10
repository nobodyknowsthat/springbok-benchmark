package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.dynamodb.IdTemporalStorageDriver;
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
 * @create 2022-06-08 2:17 PM
 **/
public class DynamodbIdTemporalBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        String filename = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_10w.csv";

        String tableName = "idTemporalTableInsertion";

        String partitionKeyName = "myOid";

        String sortKeyName = "myTimestamp";

        PortoTaxiRealData portoTaxiRealData;

        @Setup(Level.Invocation)
        public void setupData() {
            portoTaxiRealData = new PortoTaxiRealData(filename);
            IdTemporalStorageDriver.createTable(tableName, partitionKeyName, sortKeyName);
        }

        @TearDown(Level.Invocation)
        public void close() {
            if (portoTaxiRealData != null) {
                portoTaxiRealData.close();
            }
            IdTemporalStorageDriver.deleteTable(tableName);
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
            if (dataBatch.size() == 10000) {
                IdTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, state.tableName, state.partitionKeyName, state.sortKeyName);
                dataBatch.clear();
            }
            blackhole.consume(dataBatch);
        }
        if (count % 1000000 == 0) {
            System.out.println(point);
            System.out.println("count: " + count);
        }

        IdTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, state.tableName, state.partitionKeyName, state.sortKeyName);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DynamodbIdTemporalBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

}
