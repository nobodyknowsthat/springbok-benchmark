package com.anonymous.test.benchmark.hbase;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.util.ZCurve;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-10-03 12:10 PM
 **/
public class HBaseSpatioTemporalStorageDriver {

    private static String columnFamilyName = "cf";

    private static String qualifierName = "";

    private static HBaseDriver driver = new HBaseDriver("172.31.85.41");

    private static double spatialWidth = 0.01;

    private static ZCurve zCurve = new ZCurve();

    public static void createTable(String tableName) {
        try {
            driver.createTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("create hbase table failed\n");
        }
    }

    public static void main(String[] args) {
        testSpatioTemporal();
    }

    public static void testSpatioTemporal() {
        String testTable = "test_spatiotemporal";
        /*createTable(testTable);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample_small.csv");
        List<TrajectoryPoint> pointList = new ArrayList<>();
        TrajectoryPoint point;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            pointList.add(point);
        }
        System.out.println("data list size: " + pointList.size());
        batchPutTrajectoryPoints(pointList, testTable);*/

        SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(1372636853000L, 1372636883000L, new Point(-8.610309, 41.140746), new Point(-8.610291, 41.14089));
        int count = spatioTemporalRangeQuery(predicate, testTable);
        System.out.println("result count: " + count);

    }

    public static void batchPutTrajectoryPoints(List<TrajectoryPoint> pointList, String tableName) {
        List<Put> putList = new ArrayList<>();
        for (TrajectoryPoint point : pointList) {
            int latitudeId = (int) Math.floor(point.getLatitude() / spatialWidth);
            int longitudeId = (int) Math.floor(point.getLongitude() / spatialWidth);
            long partitionIdValue = zCurve.getCurveValue(longitudeId, latitudeId);

            String partitionId = String.valueOf(partitionIdValue);
            String timestampOid = String.format("%d.%s", point.getTimestamp(), point.getOid());

            String rowKey = partitionId + "." + timestampOid;
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

    public static int spatioTemporalRangeQuery(SpatialTemporalRangeQueryPredicate predicate, String tableName) {
        Point lowerLeftPoint = predicate.getLowerLeft();
        Point upperRightPoint = predicate.getUpperRight();

        int lonIndexLow = (int) Math.floor(lowerLeftPoint.getLongitude() / spatialWidth);
        int lonIndexHigh = (int) Math.floor(upperRightPoint.getLongitude() / spatialWidth);
        int latIndexLow = (int) Math.floor(lowerLeftPoint.getLatitude() / spatialWidth);
        int latIndexHigh = (int) Math.floor(upperRightPoint.getLatitude() / spatialWidth);

        List<String> partitionIdList = new ArrayList<>();
        for (int i = lonIndexLow; i <= lonIndexHigh; i++) {
            for (int j = latIndexLow; j <= latIndexHigh; j++) {
                partitionIdList.add(String.valueOf(zCurve.getCurveValue(i, j)));
            }
        }

        String myTimestampOidLow = String.format("%d.%s", predicate.getStartTimestamp(), "00000000");
        String myTimestampOidHigh = String.format("%d.%s", predicate.getStopTimestamp(), "99999999");

        int count = 0;
        try {
            for (String partitionId : partitionIdList) {
                String startKey = partitionId + "." + myTimestampOidLow;
                String stopKey = partitionId + "." + myTimestampOidHigh;
                List<Result> resultList = driver.scan(tableName, Bytes.toBytes(startKey), Bytes.toBytes(stopKey));

                for (Result result : resultList) {
                    for (Cell cell : result.listCells()) {
                        // System.out.println("value offset: " + cell.getValueOffset());
                        // System.out.println("value length: " + cell.getValueLength());
                        // System.out.println(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                        String dataValue = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                        String[] items = dataValue.split(",");
                        if (items.length == 10) {
                            double longitude = Double.parseDouble(items[8]);
                            double latitude = Double.parseDouble(items[9]);
                            long timestamp = Long.parseLong(items[5]) * 1000;
                            if (longitude >= predicate.getLowerLeft().getLongitude() && longitude <= predicate.getUpperRight().getLongitude()
                                    && latitude >= predicate.getLowerLeft().getLatitude() && latitude <= predicate.getUpperRight().getLatitude()
                                    && timestamp >= predicate.getStartTimestamp() && timestamp <= predicate.getStopTimestamp()) {
                                count++;
                                //System.out.println(item);
                                //System.out.println(dataValue);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return count;
    }

}
