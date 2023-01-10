package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.benchmark.predicate.StatisticUtil;
import com.anonymous.test.benchmark.springbok.SpringbokDriver;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
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
/**
 * @author anonymous
 * @create 2022-06-28 2:32 PM
 **/
public class SpringbokSpatialTemporalBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        //@Param({"7d"})
        @Param({"24h"})
        //@Param({"1h", "6h", "24h", "7d"})
        public String timeLength;

        @Param({"001", "01", "1"})
        //@Param({"01"})
        public String spatialWidth;

        String queryFilenamePrefix = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_";

        List<SpatialTemporalRangeQueryPredicate> predicateList;

        List<String> queryResultList = new ArrayList<>();

        @Setup(Level.Trial)
        public void setup() {
            // set query
            String queryFilename = queryFilenamePrefix + timeLength + "_" + spatialWidth + ".query";
            predicateList = QueryGenerator.getSpatialTemporalRangeQueriesFromQueryFile(queryFilename);
        }

        @TearDown(Level.Trial)
        public void saveLog() {
            for (String log : queryResultList) {
                StatusRecorder.recordStatus("springbok-spatiotemporal-5x-fixtemporal-again-detail.log", log);
            }
            queryResultList.clear();
            StatusRecorder.recordStatus("springbok-spatiotemporal-5x-fixtemporal-again-detail.log", "\n\n\n");
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery(Blackhole blackhole, BenchmarkState state) {
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = SpringbokDriver.spatialTemporalQuery(predicate);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SpringbokSpatialTemporalBenchmark.class.getSimpleName())
                .output("springbok-spatiotemporal-5x-fixtemporal-again.log")
                .build();

        new Runner(opt).run();
    }

}
