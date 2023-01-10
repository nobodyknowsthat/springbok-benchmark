package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.dynamodb.IdTemporalStorageDriver;
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

/**
 * @author anonymous
 * @create 2022-06-08 3:21 PM
 **/
public class DynamodbIdTemporalQueryBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        //@Param({"1h", "6h", "24h", "7d"})
        @Param({"7d"})
        public String timeLength;

        String queryFilenamePrefix = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_id_";

        List<IdTemporalQueryPredicate> predicateList;

        @Setup(Level.Trial)
        public void setup() {
            // set query
            String queryFilename = queryFilenamePrefix + timeLength + ".query";
            predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile(queryFilename);
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
        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            int count = IdTemporalStorageDriver.idTemporalQuery(predicate, DynamodbIdTemporalQueryTableCreator.TABLE_NAME,
                    DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME,
                    DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME);
            System.out.println("result count: " + count);
            resultCountList.add(count);
        }
        // dynamodb does not allow duplicated key, and if dataset has some duplicated records the count may be different from other systems (support duplicated records)
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
    }


    public static void main(String[] args) throws RunnerException {
        System.out.println("This is id temporal query benchmark for dynamodb");

        Options opt = new OptionsBuilder()
                .include(DynamodbIdTemporalQueryBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }


}
