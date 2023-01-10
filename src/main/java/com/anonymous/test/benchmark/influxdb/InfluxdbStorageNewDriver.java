package com.anonymous.test.benchmark.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.util.ZCurve;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-10-10 8:10 PM
 **/
public class InfluxdbStorageNewDriver {

    private static char[] token = "4ch-HKJtyMnqAxqq50S87On4Nr4ruvjKUG1qLpV9NhauFXiz0ipcNMT1ieVlPO4Iei7M9uz-AKSuxeIReXu8xA==".toCharArray();
    private static String org = "cuhk";
    private static String bucket = "myportodatapartitioninsert";

    private static double spatialWidth = 0.01;

    private static ZCurve zCurve = new ZCurve();

    private static InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://172.31.85.41:8086", token, org, bucket);

    public static void main(String[] args) {
        testIdTemporal();
    }

    private static void testWrite() {
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData("/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample_small.csv");
        List<TrajectoryPoint> pointList = new ArrayList<>();
        TrajectoryPoint point;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            pointList.add(point);
        }
        System.out.println("data list size: " + pointList.size());

        writeTrajectoryPoints(pointList);
        influxDBClient.close();
    }

    private static void testIdTemporal() {
        IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(1372636853000L, 1372636881000L, "20000380");
        int count = idTemporalQuery(predicate);
        System.out.println("result count: " + count);
    }

    private static void testSpatioTemporal() {
        SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(1372636853000L, 1372636884000L, new Point(-8.610309, 41.140746), new Point(-8.610291, 41.14089));
        int count = spatioTemporalQuery(predicate);
        System.out.println("result count: " + count);

    }

    public static void writeTrajectoryPoints(List<TrajectoryPoint> pointList) {
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        List<String> lineList = new ArrayList<>();
        for (TrajectoryPoint point : pointList) {
            int latitudeId = (int) Math.floor(point.getLatitude() / spatialWidth);
            int longitudeId = (int) Math.floor(point.getLongitude() / spatialWidth);
            long partitionIdValue = zCurve.getCurveValue(longitudeId, latitudeId);
            String partitionId = String.valueOf(partitionIdValue);

            String lineProtocolString = String.format("trajectories,oid=%s,partition=%s longitude=%f,latitude=%f,payload=\"%s\" %d", point.getOid(), partitionId, point.getLongitude(), point.getLatitude(), point.getPayload(), point.getTimestamp());
            lineList.add(lineProtocolString);
        }


        writeApi.writeRecords(WritePrecision.MS, lineList);
    }

    public static int idTemporalQuery(IdTemporalQueryPredicate predicate) {
        // the time unit used in time() should be second unit for influxdb
        String fluxString = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d) |> filter(fn: (r) => r._measurement == \"trajectories\" and r.oid == \"%s\")", bucket, predicate.getStartTimestamp()/1000, predicate.getStopTimestamp()/1000, predicate.getDeviceId());

        System.out.println("flux: " + fluxString);

        QueryApi queryApi = influxDBClient.getQueryApi();

        List<FluxTable> tables = queryApi.query(fluxString);
        int count = 0;
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                count++;
                //System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
            }
        }
        return count;
    }

    public static int spatioTemporalQuery(SpatialTemporalRangeQueryPredicate predicate) {
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

        int count = 0;
        for (String partitionId : partitionIdList) {
            String fluxString = String.format("from(bucket:\"%s\") |> range(start: %d, stop: %d) |> filter(fn: (r) => r._measurement == \"trajectories\" and r.partition == \"%s\")", bucket,
                    predicate.getStartTimestamp() / 1000, predicate.getStopTimestamp() / 1000, partitionId);
            System.out.println("flux: " + fluxString);

            QueryApi queryApi = influxDBClient.getQueryApi();

            List<FluxTable> tables = queryApi.query(fluxString);
            for (FluxTable fluxTable : tables) {
                List<FluxRecord> records = fluxTable.getRecords();
                for (FluxRecord fluxRecord : records) {
                    // influxdb does not support a index for field values, so we do the filtering in the client side

                    if ("payload".equals(fluxRecord.getField())) {
                        String[] items = ((String) fluxRecord.getValueByKey("_value")).split(",");
                        double longitude = Double.parseDouble(items[8]);
                        double latitude = Double.parseDouble(items[9]);
                        if (longitude >= predicate.getLowerLeft().getLongitude() && longitude <= predicate.getUpperRight().getLongitude()
                                && latitude >= predicate.getLowerLeft().getLatitude() && latitude <= predicate.getUpperRight().getLatitude()) {
                            //System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getField() + ": "+  fluxRecord.getValueByKey("_value"));
                            count++;
                        }
                    }


                }
            }
        }
        return count;
    }
}
