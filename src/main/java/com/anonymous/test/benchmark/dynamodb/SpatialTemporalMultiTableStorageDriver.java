package com.anonymous.test.benchmark.dynamodb;

import com.anonymous.test.common.Point;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.util.ZCurve;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;

/**
 * @author anonymous
 * @create 2022-06-14 7:50 PM
 **/
@Deprecated
public class SpatialTemporalMultiTableStorageDriver {

    private static DynamoDbClient client = DynamoDBDriver.createClientForWeb(Region.US_EAST_1);

    private static String partitionKeyName = "myTimePartition";

    private static String sortKeyName = "myTimestampOid";

    private static double spatialWidth = 0.01;

    private static long timeWidth = 60 * 60;    // 1 hour (unit is s)

    private static ZCurve zCurve = new ZCurve();

    private static Set<String> existedSpaceIds = new HashSet<>();

    public static void batchPutTrajectoryPoints(List<TrajectoryPoint> pointList) {

        Map<String, List<TrajectoryPoint>> pointMap = new HashMap<>();
        for (TrajectoryPoint point : pointList) {
            int latitudeId = (int) Math.floor(point.getLatitude() / spatialWidth);
            int longitudeId = (int) Math.floor(point.getLongitude() / spatialWidth);
            long partitionIdValue = zCurve.getCurveValue(longitudeId, latitudeId);
            String spaceId = String.valueOf(partitionIdValue);

            if (pointMap.containsKey(spaceId)) {
                pointMap.get(spaceId).add(point);
            } else {
                List<TrajectoryPoint> individualPointList = new ArrayList<>();
                individualPointList.add(point);
                pointMap.put(spaceId, individualPointList);
            }
        }

        for (String spaceId : pointMap.keySet()) {
            String tableName = String.format("SpatialTemporalFor%s", spaceId);
            if (!existedSpaceIds.contains(spaceId)) {
                // this is a new object, we create table for it
                SpatialTemporalStorageDriver.createTable(tableName, partitionKeyName, sortKeyName);
                existedSpaceIds.add(spaceId);
            }
            batchPutForSpacePartition(pointMap.get(spaceId), tableName, partitionKeyName, sortKeyName);
        }
    }

    private static void batchPutForSpacePartition(List<TrajectoryPoint> pointList, String tableName, String partitionKeyName, String sortKeyName) {
        String sqlStatement = String.format("INSERT INTO %s VALUE {'%s':?, '%s':?, 'myData':?}", tableName, partitionKeyName, sortKeyName);

        try {
            List<BatchStatementRequest> batchStatementRequestList = new ArrayList<>();
            for (TrajectoryPoint point : pointList) {
                //System.out.println(point);
                long timePartitionIdValue = point.getTimestamp() / timeWidth;

                String timePartitionId = String.valueOf(timePartitionIdValue);
                String timestampOid = String.format("%d.%s", point.getTimestamp(), point.getOid());
                //System.out.println(timestampOid);
                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(timePartitionId)
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
                    //System.out.println("Added new records using a batch command.");
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
            //System.exit(1);
        }


    }

    public static Set<String> getTableIds() {
        return existedSpaceIds;
    }

    public static void spatialTemporalRangeQuery(SpatialTemporalRangeQueryPredicate predicate) {

        Point lowerLeftPoint = predicate.getLowerLeft();
        Point upperRightPoint = predicate.getUpperRight();

        int lonIndexLow = (int) Math.floor(lowerLeftPoint.getLongitude() / spatialWidth);
        int lonIndexHigh = (int) Math.floor(upperRightPoint.getLongitude() / spatialWidth);
        int latIndexLow = (int) Math.floor(lowerLeftPoint.getLatitude() / spatialWidth);
        int latIndexHigh = (int) Math.floor(upperRightPoint.getLatitude() / spatialWidth);

        List<String> spacePartitionIdList = new ArrayList<>();
        for (int i = lonIndexLow; i <= lonIndexHigh; i++) {
            for (int j = latIndexLow; j <= latIndexHigh; j++) {
                spacePartitionIdList.add(String.valueOf(zCurve.getCurveValue(i, j)));
            }
        }

        for (String spacePartitionId : spacePartitionIdList) {
            String tableName = String.format("SpatialTemporalFor%s", spacePartitionId);
            queryForSpace(predicate, tableName, partitionKeyName, sortKeyName);
        }

    }

    private static void queryForSpace(SpatialTemporalRangeQueryPredicate predicate, String tableName, String partitionKeyName, String sortKeyName) {
        String sqlStatement = String.format("SELECT * FROM %s WHERE %s = ? AND %s BETWEEN ? AND ?", tableName, partitionKeyName, sortKeyName);

        List<String> timePartitionList = new ArrayList<>();
        long indexLow = predicate.getStartTimestamp() / timeWidth;
        long indexHigh = predicate.getStopTimestamp() / timeWidth;
        for (long i = indexLow; i < indexHigh; i++) {
            timePartitionList.add(String.valueOf(i));
        }


        String myTimestampOidLow = String.format("%d.%s", predicate.getStartTimestamp(), "00000000");
        String myTimestampOidHigh = String.format("%d.%s", predicate.getStopTimestamp(), "99999999");

        for (String timePartitionId : timePartitionList) {
            try {

                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(timePartitionId).build();
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
                    // do nothing
                    item.keySet();
                }

            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }

    }

    private static ExecuteStatementResponse executeStatementRequest(DynamoDbClient ddb, String statement, List<AttributeValue> parameters) {
        ExecuteStatementRequest request = ExecuteStatementRequest.builder()
                .statement(statement)
                .parameters(parameters)
                .build();

        return ddb.executeStatement(request);
    }

    public static void deleteTables(Set<String> tableIds) {
        for (String id : tableIds) {
            String tableName = String.format("SpatialTemporalFor%s", id);
            SpatialTemporalStorageDriver.deleteTable(tableName);
        }
    }
}
