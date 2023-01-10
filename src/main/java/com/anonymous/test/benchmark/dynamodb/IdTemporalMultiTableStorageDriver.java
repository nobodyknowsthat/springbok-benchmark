package com.anonymous.test.benchmark.dynamodb;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;

/**
 * @author anonymous
 * @create 2022-06-14 7:15 PM
 **/
@Deprecated
public class IdTemporalMultiTableStorageDriver {

    private static DynamoDbClient client = DynamoDBDriver.createClientForLocal();

    private static String partitionKeyName = "myTimePartition";  // used object id of vehicle as the partition key

    private static String sortKeyName = "myTimestamp";  // use timestamp as the sort key

    private static long timeWidth = 60 * 60;    // 1 hour (unit is s)

    private static Set<String> existedOids = new HashSet<>();

    public static void main(String[] args) {
        String filename = "/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample.csv";
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(filename);
        List<TrajectoryPoint> pointList = PortoTaxiRealData.generateFullPointsFromPortoTaxis(filename);
        batchPutTrajectoryPoints(pointList);
    }

    public static void batchPutTrajectoryPoints(List<TrajectoryPoint> pointList) {
        Map<String, List<TrajectoryPoint>> pointMap = new HashMap<>();
        for (TrajectoryPoint point : pointList) {
            String oid = point.getOid();
            if (pointMap.containsKey(oid)) {
                pointMap.get(oid).add(point);
            } else {
                List<TrajectoryPoint> individualPointList = new ArrayList<>();
                individualPointList.add(point);
                pointMap.put(oid, individualPointList);
            }
        }

        for (String oid : pointMap.keySet()) {
            String tableName = String.format("IdTemporalFor%s", oid);
            System.out.println(tableName);
            if (!existedOids.contains(oid)) {
                // this is a new object, we create table for it
                IdTemporalStorageDriver.createTable(tableName, partitionKeyName, sortKeyName);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                existedOids.add(oid);
            }
            batchPutForObject(pointMap.get(oid), tableName, partitionKeyName, sortKeyName);
        }
    }

    private static void batchPutForObject(List<TrajectoryPoint> pointList, String tableName, String partitionKeyName, String sortKeyName) {
        String sqlStatement = String.format("INSERT INTO %s VALUE {'%s':?, '%s':?, 'myData':?}", tableName, partitionKeyName, sortKeyName);

        try {
            List<BatchStatementRequest> batchStatementRequestList = new ArrayList<>();
            for (TrajectoryPoint point : pointList) {
                long timePartitionIdValue = point.getTimestamp() / timeWidth;
                String timePartitionId = String.valueOf(timePartitionIdValue);

                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(timePartitionId)
                        .build();
                AttributeValue att2 = AttributeValue.builder()
                        .n(String.valueOf(point.getTimestamp()))
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
            }

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            //System.exit(1);
        }

    }

    public static Set<String> getTableIds() {
        return existedOids;
    }

    public static void idTemporalQuery(IdTemporalQueryPredicate predicate) {

        String tableName = String.format("IdTemporalFor%s", predicate.getDeviceId());
        queryForObject(predicate, tableName, partitionKeyName, sortKeyName);

    }

    private static void queryForObject(IdTemporalQueryPredicate predicate, String tableName, String partitionKeyName, String sortKeyName) {
        String sqlStatement = String.format("SELECT * FROM %s where %s = ? AND %s BETWEEN ? AND ?", tableName, partitionKeyName, sortKeyName);

        List<String> timePartitionList = new ArrayList<>();
        long indexLow = predicate.getStartTimestamp() / timeWidth;
        long indexHigh = predicate.getStopTimestamp() / timeWidth;
        for (long i = indexLow; i < indexHigh; i++) {
            timePartitionList.add(String.valueOf(i));
        }

        for (String timePartitionId : timePartitionList) {
            try {

                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(timePartitionId).build();
                parameters.add(att1);
                AttributeValue att2 = AttributeValue.builder()
                        .n(String.valueOf(predicate.getStartTimestamp())).build();
                parameters.add(att2);
                AttributeValue att3 = AttributeValue.builder()
                        .n(String.valueOf(predicate.getStopTimestamp())).build();
                parameters.add(att3);

                ExecuteStatementResponse response = executeStatementRequest(client, sqlStatement, parameters);

                for (Map<String, AttributeValue> item : response.items()) {
                    // do nothing
                    item.keySet();
                }

            /*String result = response.toString();
            System.out.println("ExecuteStatement successful: "+ response.toString());*/

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
            String tableName = String.format("IdTemporalFor%s", id);
            IdTemporalStorageDriver.deleteTable(tableName);
        }
    }

}
