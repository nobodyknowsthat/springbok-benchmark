package com.anonymous.test.benchmark.dynamodb;

import com.anonymous.test.benchmark.benchmark.DynamodbIdTemporalQueryTableCreator;
import com.anonymous.test.common.PortoTaxiPoint;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;

/**
 * Each item has three attribute: myOid (partition key), myTimestamp (sort key) and myData
 *
 * @author anonymous
 * @create 2022-05-31 7:41 PM
 **/
public class IdTemporalStorageDriver {

    private static DynamoDbClient client = DynamoDBDriver.createClientForLocal();

/*    private static DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(client)
            .build();*/

    private static String tableName = "idTemporalTable";

    private static String partitionKeyName = "myOid";  // used object id of vehicle as the partition key

    private static String sortKeyName = "myTimestamp";  // use timestamp as the sort key

    public static void main(String[] args) {

        //List<TrajectoryPoint> pointList = PortoTaxiRealData.generateFullPointsFromPortoTaxis("/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample.csv");


        //createTable();

        /*List<TrajectoryPoint> pointList = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            TrajectoryPoint point = new TrajectoryPoint("1001", i, 4, 5, generateString(new Random(), "abcde", 1024));
            pointList.add(point);
        }

        batchPutTrajectoryPoints(pointList);
        System.out.println("finish writing");
        long start = System.currentTimeMillis();
        idTemporalQuery(new IdTemporalQueryPredicate(1, 1000, "1001"));
        long stop = System.currentTimeMillis();
        System.out.println("time: " + (stop - start));
        //System.out.println(createTable());*/


        /*List<IdTemporalQueryPredicate> predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile("/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_id_7d.query");
        long start = System.currentTimeMillis();
        for (IdTemporalQueryPredicate predicate : predicateList) {
            System.out.println(predicate);
            int count = idTemporalQuery(predicate, DynamodbIdTemporalQueryTableCreator.TABLE_NAME,
                    DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME,
                    DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME);
            System.out.println("count: " + count);
        }
        long stop = System.currentTimeMillis();
        System.out.println("50 queries time: " + (stop - start) + " ms");*/

        /*IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(1372638215000L,1373243015000L,"20000337");
        Set<Long> dynamodbSet = (idTemporalQueryForCheck(predicate, DynamodbIdTemporalQueryTableCreator.TABLE_NAME,
                DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME,
                DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME));

        List<TrajectoryPoint> resultFromSpringbok = SpringbokDriver.idTemporalQueryForCheck(predicate);
        Set<Long> springbokSet = new HashSet<>();
        for (TrajectoryPoint point : resultFromSpringbok) {
            springbokSet.add(point.getTimestamp());
        }

        int count = 0;
        for (long value : springbokSet) {
            if (!dynamodbSet.contains(value)) {
                System.out.println(value);
            } else {
                count++;
            }
        }

        System.out.println(count);
        *//*insertPoint(DynamodbIdTemporalQueryTableCreator.TABLE_NAME,
                DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME,
                DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME,
                "20000460", 1372669529000L, "test");
        pointQuery(DynamodbIdTemporalQueryTableCreator.TABLE_NAME,
                DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME,
                DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME,
                "20000460", 1372669529000L);*//*

        pointQuery(DynamodbIdTemporalQueryTableCreator.TABLE_NAME,
                DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME,
                DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME,
                "20000337", 1372668562000L);*/


        //testDuplicatedWrite();
        pointQuery(DynamodbIdTemporalQueryTableCreator.TABLE_NAME,
                DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME,
                DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME,
                "testoid", 30);
    }

    public static void testDuplicatedWrite() {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        for (int i = 10; i < 34; i++) {
            TrajectoryPoint point = new TrajectoryPoint("testoid", i, 11, 12, "data");
            pointList.add(point);
        }
        pointList.add(new TrajectoryPoint("testoid", 23, 11, 12, "data"));
        batchPutTrajectoryPoints(pointList, DynamodbIdTemporalQueryTableCreator.TABLE_NAME, DynamodbIdTemporalQueryTableCreator.PARTITION_KEY_NAME, DynamodbIdTemporalQueryTableCreator.SORT_KEY_NAME);
    }

    public static String generateString(Random rng, String characters, int length)
    {
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

    public static String createTable() {
        return DynamoDBDriver.createTableComKeyWithOnDemand(client, tableName, partitionKeyName, ScalarAttributeType.S, sortKeyName, ScalarAttributeType.N);
    }


    public static String createTable(String tableName, String partitionKeyName, String sortKeyName) {
        return DynamoDBDriver.createTableComKeyWithOnDemand(client, tableName, partitionKeyName, ScalarAttributeType.S, sortKeyName, ScalarAttributeType.N);
    }

    public static void deleteTable(String tableName) {
        DynamoDBDriver.deleteDynamoDBTable(client, tableName);
    }

    public static void batchPutTrajectoryPoints(List<TrajectoryPoint> pointList) {

        String sqlStatement = "INSERT INTO idTemporalTable VALUE {'myOid':?, 'myTimestamp':?, 'myData':?}";

        try {
            List<BatchStatementRequest> batchStatementRequestList = new ArrayList<>();
            for (TrajectoryPoint point : pointList) {
                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(point.getOid())
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
                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(point.getOid())
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

                        try {
                            BatchExecuteStatementResponse response = client.batchExecuteStatement(batchRequest);
                            System.out.println("ExecuteStatement successful: " + response.toString());
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
                    System.out.println("Added new movies using a batch command.");
                }



    }


    public static void idTemporalQuery(IdTemporalQueryPredicate predicate) {

        String sqlStatement = "SELECT * FROM idTemporalTable where myOid = ? AND myTimestamp BETWEEN ? AND ?";

        try {

            List<AttributeValue> parameters = new ArrayList<>();
            AttributeValue att1 = AttributeValue.builder()
                    .s(predicate.getDeviceId()).build();
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

    public static void insertPoint(String tableName, String partitionKeyName, String sortKeyName, String myOid, long myTimestamp, String payload) {
        String sqlStatement = String.format("INSERT INTO %s VALUE {'%s':?, '%s':?, 'myData':?}", tableName, partitionKeyName, sortKeyName);

        List<TrajectoryPoint> pointList = new ArrayList<>();
        TrajectoryPoint pointData = new TrajectoryPoint(myOid, myTimestamp, 0, 0, payload);
        pointList.add(pointData);

        try {
            List<BatchStatementRequest> batchStatementRequestList = new ArrayList<>();
            for (TrajectoryPoint point : pointList) {
                List<AttributeValue> parameters = new ArrayList<>();
                AttributeValue att1 = AttributeValue.builder()
                        .s(point.getOid())
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

                if (batchStatementRequestList.size() == 1) {  // max batch write limit is 25
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

    public static void pointQuery(String tableName, String partitionKeyName, String sortKeyName, String myOid, long myTimestamp) {
        String sqlStatement = String.format("SELECT * FROM %s WHERE %s = ? and %s = ?", tableName, partitionKeyName, sortKeyName);

        try {

            List<AttributeValue> parameters = new ArrayList<>();
            AttributeValue att1 = AttributeValue.builder()
                    .s(myOid).build();
            parameters.add(att1);
            AttributeValue att2 = AttributeValue.builder()
                    .n(String.valueOf(myTimestamp)).build();
            parameters.add(att2);


            ExecuteStatementResponse response = executeStatementRequest(client, sqlStatement, parameters);

            int count = 0;
            System.out.println(response.toString());
            for (Map<String, AttributeValue> item : response.items()) {
                // do nothing
                count++;
                System.out.println(item);
                for (String key : item.keySet()) {
                    item.get(key);
                }

            }
            /*String result = response.toString();
            System.out.println("ExecuteStatement successful: "+ response.toString());*/

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }


    }

    public static int idTemporalQuery(IdTemporalQueryPredicate predicate, String tableName, String partitionKeyName, String sortKeyName) {

        String sqlStatement = String.format("SELECT * FROM %s where %s = ? AND %s BETWEEN ? AND ?", tableName, partitionKeyName, sortKeyName);

        try {

            List<AttributeValue> parameters = new ArrayList<>();
            AttributeValue att1 = AttributeValue.builder()
                    .s(predicate.getDeviceId()).build();
            parameters.add(att1);
            AttributeValue att2 = AttributeValue.builder()
                    .n(String.valueOf(predicate.getStartTimestamp())).build();
            parameters.add(att2);
            AttributeValue att3 = AttributeValue.builder()
                    .n(String.valueOf(predicate.getStopTimestamp())).build();
            parameters.add(att3);

            String nextToken = null;
            int count = 0;
            do {

                ExecuteStatementResponse response;
                if (nextToken == null) {
                    response = executeStatementRequest(client, sqlStatement, parameters);
                } else {
                    response = executeStatementRequestWithNextToken(client, sqlStatement, parameters, nextToken);
                }
                //System.out.println(response.toString());
                for (Map<String, AttributeValue> item : response.items()) {
                    // do nothing
                    count++;
                    //System.out.println(item);
                    for (String key : item.keySet()) {
                        item.get(key).toString();
                    }

                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            return count;
            /*String result = response.toString();
            System.out.println("ExecuteStatement successful: "+ response.toString());*/

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return -1;
    }

    public static Set<Long> idTemporalQueryForCheck(IdTemporalQueryPredicate predicate, String tableName, String partitionKeyName, String sortKeyName) {

        String sqlStatement = String.format("SELECT * FROM %s where %s = ? AND %s BETWEEN ? AND ?", tableName, partitionKeyName, sortKeyName);

        Set<Long> hashSet = new HashSet<>();
        try {

            List<AttributeValue> parameters = new ArrayList<>();
            AttributeValue att1 = AttributeValue.builder()
                    .s(predicate.getDeviceId()).build();
            parameters.add(att1);
            AttributeValue att2 = AttributeValue.builder()
                    .n(String.valueOf(predicate.getStartTimestamp())).build();
            parameters.add(att2);
            AttributeValue att3 = AttributeValue.builder()
                    .n(String.valueOf(predicate.getStopTimestamp())).build();
            parameters.add(att3);

            ExecuteStatementResponse response = executeStatementRequest(client, sqlStatement, parameters);

            int count = 0;
            //System.out.println(response.toString());
            for (Map<String, AttributeValue> item : response.items()) {
                // do nothing
                count++;
                System.out.println(item);
                for (String key : item.keySet()) {
                    if (key.equals("myTimestamp")) {
                        hashSet.add(Long.valueOf(item.get(key).n()));
                    }
                }

            }
            return hashSet;
            /*String result = response.toString();
            System.out.println("ExecuteStatement successful: "+ response.toString());*/

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return hashSet;
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
