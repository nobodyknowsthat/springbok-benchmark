package com.anonymous.test.benchmark.timestream;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.predicate.QueryGenerator;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.*;
import software.amazon.awssdk.services.timestreamquery.model.ConflictException;
import software.amazon.awssdk.services.timestreamquery.paginators.QueryIterable;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.*;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author anonymous
 * @create 2022-06-01 2:49 PM
 **/
public class TimestreamStorageDriver {

    public static String DATABASE_NAME = "benchmark";

    private static long HT_TTL_HOURS = 24;

    private static long CT_TTL_DAYS = 7;

    public static String TABLE_NAME = "trajectory";

    private static long ONE_GB_IN_BYTES = 1024 * 1024 * 1024;

    private static TimestreamWriteClient timestreamWriteClient = buildWriteClient();

    private static TimestreamQueryClient timestreamQueryClient = buildQueryClient();

    public static void main(String[] args) {
        //createDatabase();
        //createTable();

        PortoTaxiRealData data = new PortoTaxiRealData("/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample.csv");
        List<TrajectoryPoint> pointList = PortoTaxiRealData.generateFullPointsFromPortoTaxis("/home/anonymous/IdeaProjects/springbok-benchmark/src/main/resources/dataset/porto_data_v1_sample.csv");
        //writeMultiMeasureRecords(pointList);

        String queryString = "SELECT * FROM " + DATABASE_NAME + "." + TABLE_NAME + " WHERE oid = '20000458' and latitude between 41.146119 and 42";
        String SQL = QueryGenerator.assembleSQLStringForIdTemporalQuery(new IdTemporalQueryPredicate(1372642546000L, 1372642576000L, "20000458"), DATABASE_NAME, TABLE_NAME);
        String SQL2 = QueryGenerator.assembleSQLStringForSpatioTemporalQuery(new SpatialTemporalRangeQueryPredicate(1372642546000L, 1372642576000L, new Point(-8.597556, 41.14613), new Point(-8.597484, 41.1462)), DATABASE_NAME, TABLE_NAME);
        System.out.println(SQL2);
        runQuery(SQL2);
    }

    public static void createDatabase() {
        System.out.println("Creating database");
        CreateDatabaseRequest request = CreateDatabaseRequest.builder().databaseName(DATABASE_NAME).build();
        try {
            timestreamWriteClient.createDatabase(request);
            System.out.println("Database [" + DATABASE_NAME + "] created successfully");
        } catch (ConflictException e) {
            System.out.println("Database [" + DATABASE_NAME + "] exists. Skipping database creation");
        }
    }

    public static void createDatabase(String databaseName) {
        System.out.println("Creating database");
        CreateDatabaseRequest request = CreateDatabaseRequest.builder().databaseName(databaseName).build();
        try {
            timestreamWriteClient.createDatabase(request);
            System.out.println("Database [" + databaseName + "] created successfully");
        } catch (ConflictException e) {
            System.out.println("Database [" + databaseName + "] exists. Skipping database creation");
        }
    }

    public static void createTable() {
        System.out.println("Creating table");

        final RetentionProperties retentionProperties = RetentionProperties.builder()
                .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
                .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS).build();
        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).retentionProperties(retentionProperties).build();

        try {
            timestreamWriteClient.createTable(createTableRequest);
            System.out.println("Table [" + TABLE_NAME + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + TABLE_NAME + "] exists on database [" + DATABASE_NAME + "] . Skipping database creation");
        }
    }

    public static void createTable(String databaseName, String tableName, long hotTTLHours, long coldTTLDays) {
        System.out.println("Creating table");

        final RetentionProperties retentionProperties = RetentionProperties.builder()
                .memoryStoreRetentionPeriodInHours(hotTTLHours)
                .magneticStoreRetentionPeriodInDays(coldTTLDays).build();
        final CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).retentionProperties(retentionProperties).build();

        try {
            timestreamWriteClient.createTable(createTableRequest);
            System.out.println("Table [" + tableName + "] successfully created.");
        } catch (ConflictException e) {
            System.out.println("Table [" + tableName + "] exists on database [" + databaseName + "] . Skipping database creation");
        }
    }

    public static void describeTable() {
        System.out.println("Describing table");
        final DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).build();
        try {
            DescribeTableResponse response = timestreamWriteClient.describeTable(describeTableRequest);
            String tableId = response.table().arn();
            System.out.println("Table " + TABLE_NAME + " has id " + tableId);
        } catch (final Exception e) {
            System.out.println("Table " + TABLE_NAME + " doesn't exist = " + e);
            throw e;
        }
    }

    public static void describeTable(String databaseName, String tableName) {
        System.out.println("Describing table");
        final DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).build();
        try {
            DescribeTableResponse response = timestreamWriteClient.describeTable(describeTableRequest);
            String tableId = response.table().arn();
            System.out.println("Table " + tableName + " has id " + tableId);
        } catch (final Exception e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            throw e;
        }
    }


    public static void deleteTable() {
        System.out.println("Deleting table");
        final DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).build();
        try {
            DeleteTableResponse response =
                    timestreamWriteClient.deleteTable(deleteTableRequest);
            System.out.println("Delete table status: " + response.sdkHttpResponse().statusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Table " + TABLE_NAME + " doesn't exist = " + e);
            throw e;
        } catch (final Exception e) {
            System.out.println("Could not delete table " + TABLE_NAME + " = " + e);
            throw e;
        }
    }

    public static void deleteTable(String databaseName, String tableName) {
        System.out.println("Deleting table");
        final DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                .databaseName(databaseName).tableName(tableName).build();
        try {
            DeleteTableResponse response =
                    timestreamWriteClient.deleteTable(deleteTableRequest);
            System.out.println("Delete table status: " + response.sdkHttpResponse().statusCode());
        } catch (final ResourceNotFoundException e) {
            System.out.println("Table " + tableName + " doesn't exist = " + e);
            throw e;
        } catch (final Exception e) {
            System.out.println("Could not delete table " + tableName + " = " + e);
            throw e;
        }
    }


    public static void writeMultiMeasureRecords(List<TrajectoryPoint> pointList) {
        System.out.println("Writing records");
        // Specify repeated values for all records

        final long currentTime = System.currentTimeMillis();
        List<Record> records = new ArrayList<>();
        int counter = 0;

        for (TrajectoryPoint point : pointList) {
            List<Dimension> dimensions = new ArrayList<>();
            final Dimension oid = Dimension.builder().name("oid").value(point.getOid()).build();

            dimensions.add(oid);

            final long recordTime = currentTime - counter * 50;
            Record record = Record.builder()
                    .dimensions(dimensions)
                    .measureValueType(MeasureValueType.MULTI)
                    .measureName("trajectory")
                    .measureValues(
                            MeasureValue.builder()
                                    .name("longitude")
                                    .value(String.valueOf(point.getLongitude()))
                                    .type(MeasureValueType.DOUBLE)
                                    .build(),
                            MeasureValue.builder()
                                    .name("latitude")
                                    .value(String.valueOf(point.getLatitude()))
                                    .type(MeasureValueType.DOUBLE)
                                    .build(),
                            MeasureValue.builder()
                                    .name("datatime")
                                    .value(String.valueOf(point.getTimestamp()))
                                    .type(MeasureValueType.BIGINT)
                                    .build(),
                            MeasureValue.builder()
                                    .name("payload")
                                    .value(String.valueOf(point.getPayload()))
                                    .type(MeasureValueType.VARCHAR)
                                    .build()
                    )
                    .time(String.valueOf(recordTime))
                    .build();

            records.add(record);
            counter++;

            //System.out.println(record.toString());

            if (records.size() == 100) {
                submitBatch(records, counter);
                records.clear();
            }

        }
    }

    public static void writeMultiMeasureRecords(List<TrajectoryPoint> pointList, String databaseName, String tableName) {
        System.out.println("Writing records");
        // Specify repeated values for all records

        final long currentTime = System.currentTimeMillis();
        List<Record> records = new ArrayList<>();
        int counter = 0;

        for (TrajectoryPoint point : pointList) {
            List<Dimension> dimensions = new ArrayList<>();
            final Dimension oid = Dimension.builder().name("oid").value(point.getOid()).build();

            dimensions.add(oid);

            final long recordTime = currentTime - counter * 50;
            //System.out.println(recordTime);
            Record record = Record.builder()
                    .dimensions(dimensions)
                    .measureValueType(MeasureValueType.MULTI)
                    .measureName("trajectory")
                    .measureValues(
                            MeasureValue.builder()
                                    .name("longitude")
                                    .value(String.valueOf(point.getLongitude()))
                                    .type(MeasureValueType.DOUBLE)
                                    .build(),
                            MeasureValue.builder()
                                    .name("latitude")
                                    .value(String.valueOf(point.getLatitude()))
                                    .type(MeasureValueType.DOUBLE)
                                    .build(),
                            MeasureValue.builder()
                                    .name("datatime")
                                    .value(String.valueOf(point.getTimestamp()))
                                    .type(MeasureValueType.BIGINT)
                                    .build(),
                            MeasureValue.builder()
                                    .name("payload")
                                    .value(String.valueOf(point.getPayload()))
                                    .type(MeasureValueType.VARCHAR)
                                    .build()
                    )
                    .time(String.valueOf(point.getTimestamp()))
                    .build();

            records.add(record);
            counter++;

            //System.out.println(record.toString());

            if (records.size() == 100) {
                submitBatch(records, counter, databaseName, tableName);
                records.clear();
            }

        }
    }

    public static void writeMultiMeasureRecordsHistorical(List<TrajectoryPoint> pointList, String databaseName, String tableName, long timestampOffset) {
        System.out.println("Writing records");
        // Specify repeated values for all records

        final long currentTime = System.currentTimeMillis();
        List<Record> records = new ArrayList<>();
        int counter = 0;

        for (TrajectoryPoint point : pointList) {
            List<Dimension> dimensions = new ArrayList<>();
            final Dimension oid = Dimension.builder().name("oid").value(point.getOid()).build();

            dimensions.add(oid);

            //System.out.println(recordTime);
            Record record = Record.builder()
                    .dimensions(dimensions)
                    .measureValueType(MeasureValueType.MULTI)
                    .measureName("trajectory")
                    .measureValues(
                            MeasureValue.builder()
                                    .name("longitude")
                                    .value(String.valueOf(point.getLongitude()))
                                    .type(MeasureValueType.DOUBLE)
                                    .build(),
                            MeasureValue.builder()
                                    .name("latitude")
                                    .value(String.valueOf(point.getLatitude()))
                                    .type(MeasureValueType.DOUBLE)
                                    .build(),
                            MeasureValue.builder()
                                    .name("datatime")
                                    .value(String.valueOf(point.getTimestamp()))
                                    .type(MeasureValueType.BIGINT)
                                    .build(),
                            MeasureValue.builder()
                                    .name("payload")
                                    .value(String.valueOf(point.getPayload()))
                                    .type(MeasureValueType.VARCHAR)
                                    .build()
                    )
                    .time(String.valueOf(point.getTimestamp() + timestampOffset))
                    .build();

            records.add(record);
            counter++;

            //System.out.println(record.toString());

            if (records.size() == 100) {
                submitBatch(records, counter, databaseName, tableName);
                records.clear();
            }

        }
    }

    private static void submitBatch(List<Record> records, int counter) {
        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("Processed " + counter + " records. WriteRecords Status: " +
                    writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            System.out.println("RejectedRecords: " + e);
            for (RejectedRecord rejectedRecord : e.rejectedRecords()) {
                System.out.println("Rejected Index " + rejectedRecord.recordIndex() + ": "
                        + rejectedRecord.reason());
            }
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    private static void submitBatch(List<Record> records, int counter, String databaseName, String tableName) {
        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(databaseName).tableName(tableName).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("Processed " + counter + " records. WriteRecords Status: " +
                    writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            System.out.println("RejectedRecords: " + e);
            for (RejectedRecord rejectedRecord : e.rejectedRecords()) {
                System.out.println("Rejected Index " + rejectedRecord.recordIndex() + ": "
                        + rejectedRecord.reason());
            }
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }


    public static int runQuery(String queryString) {
        int count = 0;
        try {
            QueryRequest queryRequest = QueryRequest.builder().queryString(queryString).build();
            final QueryIterable queryResponseIterator = timestreamQueryClient.queryPaginator(queryRequest);
            for(QueryResponse queryResponse : queryResponseIterator) {
                count = count + parseQueryResult(queryResponse);
            }
        } catch (Exception e) {
            // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
            e.printStackTrace();
            count = -1;
        }
        return count;
    }

    private static int parseQueryResult(QueryResponse response) {
        List<ColumnInfo> columnInfo = response.columnInfo();
        List<Row> rows = response.rows();

        //System.out.println("Metadata: " + columnInfo);
        //System.out.println("Data: ");
        int count = 0;
        // iterate every row
        for (Row row : rows) {
            //System.out.println(parseRow(columnInfo, row));
            parseRow(columnInfo, row);
            count++;
        }
        return count;
    }

    private static String parseRow(List<ColumnInfo> columnInfo, Row row) {
        List<Datum> data = row.data();
        List<String> rowOutput = new ArrayList<>();
        // iterate every column per row
        for (int j = 0; j < data.size(); j++) {
            ColumnInfo info = columnInfo.get(j);
            Datum datum = data.get(j);
            rowOutput.add(parseDatum(info, datum));
        }
        return String.format("{%s}", rowOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private static String parseDatum(ColumnInfo info, Datum datum) {
        if (datum.nullValue() != null && datum.nullValue()) {
            return info.name() + "=" + "NULL";
        }
        Type columnType = info.type();
        // If the column is of TimeSeries Type
        if (columnType.timeSeriesMeasureValueColumnInfo() != null) {
            return parseTimeSeries(info, datum);
        }
        // If the column is of Array Type
        else if (columnType.arrayColumnInfo() != null) {
            List<Datum> arrayValues = datum.arrayValue();
            return info.name() + "=" + parseArray(info.type().arrayColumnInfo(), arrayValues);
        }
        // If the column is of Row Type
        else if (columnType.rowColumnInfo() != null && columnType.rowColumnInfo().size() > 0) {
            List<ColumnInfo> rowColumnInfo = info.type().rowColumnInfo();
            Row rowValues = datum.rowValue();
            return parseRow(rowColumnInfo, rowValues);
        }
        // If the column is of Scalar Type
        else {
            return parseScalarType(info, datum);
        }
    }

    private static String parseTimeSeries(ColumnInfo info, Datum datum) {
        List<String> timeSeriesOutput = new ArrayList<>();
        for (TimeSeriesDataPoint dataPoint : datum.timeSeriesValue()) {
            timeSeriesOutput.add("{time=" + dataPoint.time() + ", value=" +
                    parseDatum(info.type().timeSeriesMeasureValueColumnInfo(), dataPoint.value()) + "}");
        }
        return String.format("[%s]", timeSeriesOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private static String parseScalarType(ColumnInfo info, Datum datum) {
        return parseColumnName(info) + datum.scalarValue();
    }

    private static String parseColumnName(ColumnInfo info) {
        return info.name() == null ? "" : info.name() + "=";
    }

    private static String parseArray(ColumnInfo arrayColumnInfo, List<Datum> arrayValues) {
        List<String> arrayOutput = new ArrayList<>();
        for (Datum datum : arrayValues) {
            arrayOutput.add(parseDatum(arrayColumnInfo, datum));
        }
        return String.format("[%s]", arrayOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private static TimestreamWriteClient buildWriteClient() {
        ApacheHttpClient.Builder httpClientBuilder =
                ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy =
                RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig =
                ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig.build())
                .region(Region.US_EAST_1)
                .build();
    }

    private static TimestreamQueryClient buildQueryClient() {
        return TimestreamQueryClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }

}
