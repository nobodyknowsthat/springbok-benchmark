package com.anonymous.test.benchmark.dynamodb;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.benchmark.DynamodbSpatialTemporalQueryTableCreator;
import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.util.ZCurve;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author anonymous
 * @create 2022-06-01 1:27 PM
 **/
public class SpatialTemporalStorageDriver {

    private static DynamoDbClient client = DynamoDBDriver.createClientForLocal();

    private static String tableName = "spatialTemporalTable";

    private static String partitionKeyName = "myPartitionId";

    private static String sortKeyName = "myTimestampOid";

    private static double spatialWidth = 0.01;

    private static ZCurve zCurve = new ZCurve();

    public static void main(String[] args) {

        /*String v1 = "1234.001";
        String v2 = "1234.003";
        String v3 = "1678.001";
        String v4 = "1888.004";
        List<String> valueList = new ArrayList<>();
        valueList.add(v2);
        valueList.add(v1);
        valueList.add(v4);
        valueList.add(v3);
        Collections.sort(valueList);
        System.out.println(valueList);*/

        //List<TrajectoryPoint> pointList = PortoTaxiRealData.generateFullPointsFromPortoTaxis("/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample.csv");


        //createTable();
        //batchPutTrajectoryPoints(pointList);

        List<SpatialTemporalRangeQueryPredicate> predicateList = QueryGenerator.getSpatialTemporalRangeQueriesFromQueryFile("/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_24h_01.query");
        long start = System.currentTimeMillis();
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            System.out.println(predicate);
            int count = spatialTemporalRangeQueryWithRefinement(predicate, DynamodbSpatialTemporalQueryTableCreator.TABLE_NAME, DynamodbSpatialTemporalQueryTableCreator.PARTITION_KEY_NAME, DynamodbSpatialTemporalQueryTableCreator.SORT_KEY_NAME);
            System.out.println("count: " + count);
        }
        long stop = System.currentTimeMillis();
        System.out.println("50 queries time: " + (stop - start) + " ms");

        // 1398174986,1398779786,-8.611344,-8.601344,41.147343,41.157343
        Point point = new Point(-8.611344, 41.147343);
        Point point1 = new Point(-8.601344, 41.157343);
        SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(1398174986000L, 1398779786000L, point, point1);
        int count = spatialTemporalRangeQueryWithRefinement(predicate, DynamodbSpatialTemporalQueryTableCreator.TABLE_NAME, DynamodbSpatialTemporalQueryTableCreator.PARTITION_KEY_NAME, DynamodbSpatialTemporalQueryTableCreator.SORT_KEY_NAME);
        System.out.println("count: " + count);

        //1398229601,1398834401,-8.636013,-8.626013,41.206293,41.216293
        Point point2 = new Point(-8.636013, 41.206293);
        Point point3 = new Point(-8.626013, 41.216293);
        SpatialTemporalRangeQueryPredicate predicate2 = new SpatialTemporalRangeQueryPredicate(1398229601000L, 1398834401000L, point2, point3);
        int count1 = spatialTemporalRangeQueryWithRefinement(predicate2, DynamodbSpatialTemporalQueryTableCreator.TABLE_NAME, DynamodbSpatialTemporalQueryTableCreator.PARTITION_KEY_NAME, DynamodbSpatialTemporalQueryTableCreator.SORT_KEY_NAME);
        System.out.println("count: " + count1);
    }

    public static String createTable() {
        return DynamoDBDriver.createTableComKeyWithOnDemand(client, tableName, partitionKeyName, ScalarAttributeType.S, sortKeyName, ScalarAttributeType.S);
    }

    public static String createTable(String tableName, String partitionKeyName, String sortKeyName) {
        return DynamoDBDriver.createTableComKeyWithOnDemand(client, tableName, partitionKeyName, ScalarAttributeType.S, sortKeyName, ScalarAttributeType.S);
    }

    public static void deleteTable(String tableName) {
        DynamoDBDriver.deleteDynamoDBTable(client, tableName);
    }

    public static void batchPutTrajectoryPoints(List<TrajectoryPoint> pointList) {
        String sqlStatement = "INSERT INTO spatialTemporalTable VALUE {'myPartitionId':?, 'myTimestampOid':?, 'myData':?}";

        try {
            List<BatchStatementRequest> batchStatementRequestList = new ArrayList<>();
            for (TrajectoryPoint point : pointList) {
                int latitudeId = (int) Math.floor(point.getLatitude() / spatialWidth);
                int longitudeId = (int) Math.floor(point.getLongitude() / spatialWidth);
                long partitionIdValue = zCurve.getCurveValue(longitudeId, latitudeId);

                String partitionId = String.valueOf(partitionIdValue);
                String timestampOid = String.format("%d.%s", point.getTimestamp(), point.getOid());
                //System.out.println(timestampOid);
                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(partitionId)
                        .build();
                AttributeValue att2 = AttributeValue.builder()
                        .s(timestampOid)
                        .build();
                AttributeValue att3 = AttributeValue.builder()
                        .s(point.getPayload())
                        .build();
                parameters.add(att1);
                parameters.add(att2);
                parameters.add(att3);

                BatchStatementRequest request = BatchStatementRequest.builder()
                        .statement(sqlStatement)
                        .parameters(parameters)
                        .build();
                batchStatementRequestList.add(request);

                if (batchStatementRequestList.size() == 25) {  // max batch write limit is 25
                    BatchExecuteStatementRequest batchRequest = BatchExecuteStatementRequest.builder()
                            .statements(batchStatementRequestList).build();

                    BatchExecuteStatementResponse response = client.batchExecuteStatement(batchRequest);
                    System.out.println("ExecuteStatement successful: "+ response.toString());
                    System.out.println("Added new records using a batch command.");
                    batchStatementRequestList.clear();
                }

            }

            if (batchStatementRequestList.size() > 0) {
                BatchExecuteStatementRequest batchRequest = BatchExecuteStatementRequest.builder()
                        .statements(batchStatementRequestList).build();

                BatchExecuteStatementResponse response = client.batchExecuteStatement(batchRequest);
                System.out.println("ExecuteStatement successful: " + response.toString());
                System.out.println("Added new movies using a batch command.");
            }

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

    }

    public static void batchPutTrajectoryPoints(List<TrajectoryPoint> pointList, String tableName, String partitionKeyName, String sortKeyName) {
        String sqlStatement = String.format("INSERT INTO %s VALUE {'%s':?, '%s':?, 'myData':?}", tableName, partitionKeyName, sortKeyName);


            List<BatchStatementRequest> batchStatementRequestList = new ArrayList<>();
            for (TrajectoryPoint point : pointList) {
                //System.out.println(point);
                int latitudeId = (int) Math.floor(point.getLatitude() / spatialWidth);
                int longitudeId = (int) Math.floor(point.getLongitude() / spatialWidth);
                long partitionIdValue = zCurve.getCurveValue(longitudeId, latitudeId);

                String partitionId = String.valueOf(partitionIdValue);
                String timestampOid = String.format("%d.%s", point.getTimestamp(), point.getOid());
                System.out.println(timestampOid);
                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(partitionId)
                        .build();
                AttributeValue att2 = AttributeValue.builder()
                        .s(timestampOid)
                        .build();
                AttributeValue att3 = AttributeValue.builder()
                        .s(point.getPayload())
                        .build();
                parameters.add(att1);
                parameters.add(att2);
                parameters.add(att3);

                BatchStatementRequest request = BatchStatementRequest.builder()
                        .statement(sqlStatement)
                        .parameters(parameters)
                        .build();
                batchStatementRequestList.add(request);

                if (batchStatementRequestList.size() == 25) {  // max batch write limit is 25
                    BatchExecuteStatementRequest batchRequest = BatchExecuteStatementRequest.builder()
                            .statements(batchStatementRequestList).build();

                    try {
                        BatchExecuteStatementResponse response = client.batchExecuteStatement(batchRequest);
                        System.out.println("ExecuteStatement successful: "+ response.toString());
                    } catch (DynamoDbException e) {
                        System.err.println(e.getMessage());
                    }
                    //System.out.println("Added new records using a batch command.");
                    batchStatementRequestList.clear();
                }

            }

            if (batchStatementRequestList.size() > 0) {
                BatchExecuteStatementRequest batchRequest = BatchExecuteStatementRequest.builder()
                        .statements(batchStatementRequestList).build();

                try {
                    BatchExecuteStatementResponse response = client.batchExecuteStatement(batchRequest);
                    System.out.println("ExecuteStatement successful: " + response.toString());
                } catch (DynamoDbException e) {
                    System.err.println(e.getMessage());
                }
            }

    }

    public static void spatialTemporalRangeQuery(SpatialTemporalRangeQueryPredicate predicate) {
        String sqlStatement = "SELECT * FROM spatialTemporalTable WHERE myPartitionId = ? AND myTimestampOid BETWEEN ? AND ?";

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

        //Only single item select is supported

        for (String partitionId : partitionIdList) {

            try {

                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(partitionId).build();
                parameters.add(att1);
                AttributeValue att2 = AttributeValue.builder()
                        .s(myTimestampOidLow).build();
                parameters.add(att2);
                AttributeValue att3 = AttributeValue.builder()
                        .s(myTimestampOidHigh).build();
                parameters.add(att3);

                ExecuteStatementResponse response = executeStatementRequest(client, sqlStatement, parameters);
                /*System.out.println("ExecuteStatement successful: " + response.toString());*/
                for (Map<String, AttributeValue> item : response.items()) {
                    // do nothing
                    item.keySet();
                }

            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }

    }

    public static int spatialTemporalRangeQuery(SpatialTemporalRangeQueryPredicate predicate, String tableName, String partitionKeyName, String sortKeyName) {
        String sqlStatement = String.format("SELECT * FROM %s WHERE %s = ? AND %s BETWEEN ? AND ?", tableName, partitionKeyName, sortKeyName);

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

        //Only single item select is supported
        int count = 0;
        for (String partitionId : partitionIdList) {

            try {

                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(partitionId).build();
                parameters.add(att1);
                AttributeValue att2 = AttributeValue.builder()
                        .s(myTimestampOidLow).build();
                parameters.add(att2);
                AttributeValue att3 = AttributeValue.builder()
                        .s(myTimestampOidHigh).build();
                parameters.add(att3);

                ExecuteStatementResponse response = executeStatementRequest(client, sqlStatement, parameters);

                //System.out.println("ExecuteStatement successful: " + response.toString());
                for (Map<String, AttributeValue> item : response.items()) {
                    System.out.println(item);
                    // do nothing
                    count++;
                    for (String key : item.keySet()) {
                        item.get(key);
                    }
                }

            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
        return count;

    }

    public static int spatialTemporalRangeQueryWithRefinement(SpatialTemporalRangeQueryPredicate predicate, String tableName, String partitionKeyName, String sortKeyName) {
        String sqlStatement = String.format("SELECT * FROM %s WHERE %s = ? AND %s BETWEEN ? AND ?", tableName, partitionKeyName, sortKeyName);

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

        System.out.println(myTimestampOidLow);
        System.out.println(myTimestampOidHigh);
        //Only single item select is supported
        int count = 0;
        for (String partitionId : partitionIdList) {

            try {

                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(partitionId).build();
                parameters.add(att1);
                AttributeValue att2 = AttributeValue.builder()
                        .s(myTimestampOidLow).build();
                parameters.add(att2);
                AttributeValue att3 = AttributeValue.builder()
                        .s(myTimestampOidHigh).build();
                parameters.add(att3);

                String nextToken = null;
                do {
                    ExecuteStatementResponse response;
                    if (nextToken == null) {
                        response = executeStatementRequest(client, sqlStatement, parameters);
                    } else {
                        response = executeStatementRequestWithNextToken(client, sqlStatement, parameters, nextToken);
                    }
                    //System.out.println("ExecuteStatement successful: " + response.toString());
                    for (Map<String, AttributeValue> item : response.items()) {
                        // do nothing
                        //count++;
                        for (String key : item.keySet()) {
                            if ("myData".equals(key)) {
                                String dataString = item.get(key).s();
                                if (dataString != null) {
                                    String[] items = dataString.split(",");
                                    if (items.length == 10) {
                                        double longitude = Double.parseDouble(items[8]);
                                        double latitude = Double.parseDouble(items[9]);
                                        long timestamp = Long.parseLong(items[5]) * 1000;
                                        if (longitude >= predicate.getLowerLeft().getLongitude() && longitude <= predicate.getUpperRight().getLongitude()
                                                && latitude >= predicate.getLowerLeft().getLatitude() && latitude <= predicate.getUpperRight().getLatitude()
                                                && timestamp >= predicate.getStartTimestamp() && timestamp <= predicate.getStopTimestamp()) {
                                            count++;
                                            //System.out.println(item);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    nextToken = response.nextToken();
                } while (nextToken !=null);

            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
        return count;

    }

    private static ExecuteStatementResponse executeStatementRequest(DynamoDbClient ddb, String statement, List<AttributeValue> parameters) {
        ExecuteStatementRequest request = ExecuteStatementRequest.builder()
                .statement(statement)
                .parameters(parameters)
                .build();

        return ddb.executeStatement(request);
    }

    private static ExecuteStatementResponse executeStatementRequestWithNextToken(DynamoDbClient ddb, String statement, List<AttributeValue> parameters, String nextToken) {
        ExecuteStatementRequest request = ExecuteStatementRequest.builder()
                .statement(statement)
                .nextToken(nextToken)
                .parameters(parameters)
                .build();

        return ddb.executeStatement(request);
    }

}
