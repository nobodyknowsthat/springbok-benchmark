package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.benchmark.predicate.StatisticUtil;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.anonymous.test.benchmark.StatusRecorder;
import com.anonymous.test.benchmark.influxdb.InfluxdbStorageNewDriver;
/**
 * @author anonymous
 * @create 2022-10-05 3:25 PM
 **/
public class InfluxdbIdTemporalQueryBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"1h", "6h", "24h", "7d"})
        public String timeLength;

        String queryFilenamePrefix = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_id_";

        List<IdTemporalQueryPredicate> predicateList;

        List<String> queryResultList = new ArrayList<>();

        @Setup(Level.Trial)
        public void setup() {
            // set query
            String queryFilename = queryFilenamePrefix + timeLength + ".query";
            predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile(queryFilename);
        }

        @TearDown(Level.Trial)
        public void saveLog() {
            for (String log : queryResultList) {
                StatusRecorder.recordStatus("influxdb-spatialtag-idtemporal-detail.log", log);
            }
            queryResultList.clear();
            StatusRecorder.recordStatus("influxdb-spatialtag-idtemporal-detail.log", "\n\n\n");
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void idTemporalQuery(Blackhole blackhole, BenchmarkState state) {
        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = InfluxdbStorageNewDriver.idTemporalQuery(predicate);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
    }

    public static void main(String[] args) throws RunnerException {
        System.out.println("This is id temporal query benchmark for influxdb");

        Options opt = new OptionsBuilder()
                .include(InfluxdbIdTemporalQueryBenchmark.class.getSimpleName())
                .output("influxdb-spatialtag-id-temporal.log")
                .build();

        new Runner(opt).run();
    }

}
