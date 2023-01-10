package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.benchmark.predicate.StatisticUtil;
import com.anonymous.test.benchmark.timestream.TimestreamStorageDriver;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
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
 * @create 2022-06-07 2:54 PM
 **/
public class TimestreamIdTemporalBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        //@Param({"1h", "6h", "24h", "7d"})
        @Param({"24h"})
        public String timeLength;  // unit is hour

        String queryFilenamePrefix = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_id_";

        List<IdTemporalQueryPredicate> predicateList;

        @Setup(Level.Trial)
        public void setup() {
            long timestampOffset = 0;
            try {
                timestampOffset = TimestreamQueryTableCreator.getTimestampOffset(TimestreamQueryTableCreator.CURRENT_DATE);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            // set query
            String queryFilename = queryFilenamePrefix + timeLength + ".query";
            predicateList = QueryGenerator.getIdTemporalQueriesForTimestream(queryFilename, timestampOffset);
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 0, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(50)    // the number of queries
    @Measurement(time = 5, iterations = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void idTemporalQuery(Blackhole blackhole, BenchmarkState state) {
        List<Integer> countResultList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            String SQL = QueryGenerator.assembleSQLStringForIdTemporalQueryForTimestream(predicate, TimestreamQueryTableCreator.DATABASE_NAME, TimestreamQueryTableCreator.TABLE_NAME);
            int count = TimestreamStorageDriver.runQuery(SQL);
            System.out.println("result count: " + count);
            blackhole.consume(SQL);
            countResultList.add(count);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(countResultList));
    }


    public static void main(String[] args) throws RunnerException {
        System.out.println("This is id temporal query benchmark for timestream");

        Options opt = new OptionsBuilder()
                .include(TimestreamIdTemporalBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }


}
