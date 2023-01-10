package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.benchmark.predicate.StatisticUtil;
import com.anonymous.test.benchmark.timestream.TimestreamStorageDriver;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author anonymous
 * @create 2022-06-07 4:48 PM
 **/
public class TimestreamSpatialTemporalBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"24h"})
        public String timeLength;

        @Param({"01"})
        public String spatialWidth;

        String queryFilenamePrefix = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_";

        List<SpatialTemporalRangeQueryPredicate> predicateList;

        @Setup(Level.Trial)
        public void setup() {

            long timestampOffset = 0;
            try {
                timestampOffset = TimestreamQueryTableCreator.getTimestampOffset(TimestreamQueryTableCreator.CURRENT_DATE);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // set query
            String queryFilename = queryFilenamePrefix + timeLength + "_" + spatialWidth + ".query";
            predicateList = QueryGenerator.getSpatialTemporalRangeQueriesForTimestream(queryFilename, timestampOffset);
        }

    }

    @Fork(value = 1)
    @Warmup(iterations = 0, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(50)    // the number of queries
    @Measurement(time = 5, iterations = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalQuery(Blackhole blackhole, BenchmarkState state) {
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            String SQL = QueryGenerator.assembleSQLStringForSpatioTemporalQueryForTimestream(predicate, TimestreamQueryTableCreator.DATABASE_NAME, TimestreamQueryTableCreator.TABLE_NAME);
            int count = TimestreamStorageDriver.runQuery(SQL);
            resultCountList.add(count);
            System.out.println("result count: " + count);
        }
        System.out.println("average result list: " + StatisticUtil.calculateAverage(resultCountList));
    }

    public static void main(String[] args) throws RunnerException {
        System.out.println("This is spatial temporal query benchmark");

        Options opt = new OptionsBuilder()
                .include(TimestreamSpatialTemporalBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();

    }

}
