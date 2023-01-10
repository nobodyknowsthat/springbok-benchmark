package com.anonymous.test.benchmark.hbase;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * row key = myOid + myTimestamp
 * value = data
 *
 * @author anonymous
 * @create 2022-10-03 11:19 AM
 **/
public class HBaseIdTemporalStorageDriver {

    private static String columnFamilyName = "cf";

    private static String qualifierName = "";

    private static HBaseDriver driver = new HBaseDriver("172.31.85.41");

    public static void main(String[] args) {
        testIdTemporal();
    }

    private static void testIdTemporal() {
        String testTable = "test_idtemporal";
        createTable(testTable);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample_small.csv");
        List<TrajectoryPoint> pointList = new ArrayList<>();
        TrajectoryPoint point;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            pointList.add(point);
        }
        System.out.println("data list size: " + pointList.size());
        //batchPutTrajectoryPoints(pointList, testTable);

        IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(1372636853000L, 1372636881000L, "20000380");

        int count = idTemporalQuery(predicate, testTable);
        System.out.println("result count: " + count);
    }

    public static void createTable(String tableName) {
        try {
            driver.createTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("create hbase table failed\n");
        }
    }

    public static void batchPutTrajectoryPoints(List<TrajectoryPoint> pointList, String tableName) {
        List<Put> putList = new ArrayList<>();
        for (TrajectoryPoint point : pointList) {
            String rowKey = point.getOid() + "." + point.getTimestamp();
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifierName), Bytes.toBytes(point.getPayload()));
            putList.add(put);
        }
        try {
            driver.batchPut(tableName, putList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int idTemporalQuery(IdTemporalQueryPredicate predicate, String tableName) {
        String startKey = predicate.getDeviceId() + "." + predicate.getStartTimestamp();
        String stopKey = predicate.getDeviceId() + "." + predicate.getStopTimestamp();
        int count = 0;
        try {
            List<Result> resultList = driver.scan(tableName, Bytes.toBytes(startKey), Bytes.toBytes(stopKey));
            for (Result result : resultList) {
                result.getRow();
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }
}
