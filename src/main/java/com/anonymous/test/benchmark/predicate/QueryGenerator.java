package com.anonymous.test.benchmark.predicate;



import com.anonymous.test.common.Point;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-05-31 6:38 PM
 **/
public class QueryGenerator {

    /**
     * the format of record in the file is (time_min, time_max, lon_min, lon_max, lat_min, lat_max), and the time unit is second
     * @param queryFile
     * @return
     */
    public static List<SpatialTemporalRangeQueryPredicate> getSpatialTemporalRangeQueriesFromQueryFile(String queryFile) {
        List<SpatialTemporalRangeQueryPredicate> predicateList = new ArrayList<>();

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile), 1024 * 1024 * 4);
            String currentLine;

            while ((currentLine = bufferedReader.readLine()) != null) {
                String[] items = currentLine.split(",");
                long time_min = Long.parseLong(items[0]) * 1000;
                long time_max = Long.parseLong(items[1]) * 1000;
                double lon_min = Double.parseDouble(items[2]);
                double lon_max = Double.parseDouble(items[3]);
                double lat_min = Double.parseDouble(items[4]);
                double lat_max = Double.parseDouble(items[5]);
                SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(time_min, time_max, new Point(lon_min, lat_min), new Point(lon_max, lat_max));
                predicateList.add(predicate);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return predicateList;
    }

    public static List<SpatialTemporalRangeQueryPredicate> getSpatialTemporalRangeQueriesForTimestream(String queryFile, long timestampOffset) {
        List<SpatialTemporalRangeQueryPredicate> predicateList = new ArrayList<>();

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile), 1024 * 1024 * 4);
            String currentLine;

            while ((currentLine = bufferedReader.readLine()) != null) {
                String[] items = currentLine.split(",");
                long time_min = Long.parseLong(items[0]) * 1000 + timestampOffset;
                long time_max = Long.parseLong(items[1]) * 1000 + timestampOffset;
                double lon_min = Double.parseDouble(items[2]);
                double lon_max = Double.parseDouble(items[3]);
                double lat_min = Double.parseDouble(items[4]);
                double lat_max = Double.parseDouble(items[5]);
                SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(time_min, time_max, new Point(lon_min, lat_min), new Point(lon_max, lat_max));
                predicateList.add(predicate);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return predicateList;
    }

    /**
     * thr format of records in the file is (oid, time_min, time_max), and the time unit is second
     * @param queryFile
     * @return
     */
    public static List<IdTemporalQueryPredicate> getIdTemporalQueriesFromQueryFile(String queryFile) {
        List<IdTemporalQueryPredicate> predicateList = new ArrayList<>();

        try {

            BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile), 1024 * 1024 * 4);
            String currentLine;

            while ((currentLine = bufferedReader.readLine()) != null) {
                String[] items = currentLine.split(",");
                String oid = items[0];
                long time_min = Long.parseLong(items[1]) * 1000;
                long time_max = Long.parseLong(items[2]) * 1000;

                IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(time_min, time_max, oid);
                predicateList.add(predicate);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return predicateList;
    }

    public static List<IdTemporalQueryPredicate> getIdTemporalQueriesForTimestream(String queryFile, long timestampOffset) {
        List<IdTemporalQueryPredicate> predicateList = new ArrayList<>();

        try {

            BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile), 1024 * 1024 * 4);
            String currentLine;

            while ((currentLine = bufferedReader.readLine()) != null) {
                String[] items = currentLine.split(",");
                String oid = items[0];
                long time_min = Long.parseLong(items[1]) * 1000 + timestampOffset;
                long time_max = Long.parseLong(items[2]) * 1000 + timestampOffset;

                IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(time_min, time_max, oid);
                predicateList.add(predicate);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return predicateList;
    }

    public static String assembleSQLStringForIdTemporalQuery(IdTemporalQueryPredicate predicate, String databaseName, String tableName) {

        String queryTableName = databaseName + "." + tableName;
        String SQLString = String.format("SELECT * FROM %s WHERE oid = '%s' AND datatime BETWEEN %s AND %s", queryTableName, predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());

        return SQLString;
    }

    public static String assembleSQLStringForIdTemporalQueryForTimestream(IdTemporalQueryPredicate predicate, String databaseName, String tableName) {

        String queryTableName = databaseName + "." + tableName;
        String SQLString = String.format("SELECT * FROM %s WHERE oid = '%s' AND time BETWEEN from_milliseconds(%d) AND from_milliseconds(%d)", queryTableName, predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());

        return SQLString;
    }

    public static String assembleSQLStringForSpatioTemporalQuery(SpatialTemporalRangeQueryPredicate predicate, String databaseName, String tableName) {

        String queryTableName = databaseName + "." + tableName;
        String SQLString = String.format("SELECT * FROM %s WHERE longitude BETWEEN %f AND %f AND latitude BETWEEN %f AND %f", queryTableName
                , predicate.getLowerLeft().getLongitude(), predicate.getUpperRight().getLongitude()
                , predicate.getLowerLeft().getLatitude(), predicate.getUpperRight().getLatitude());

        return SQLString;
    }

    public static String assembleSQLStringForSpatioTemporalQueryForTimestream(SpatialTemporalRangeQueryPredicate predicate, String databaseName, String tableName) {

        String queryTableName = databaseName + "." + tableName;
        String SQLString = String.format("SELECT * FROM %s WHERE time BETWEEN from_milliseconds(%d) AND from_milliseconds(%d) AND longitude BETWEEN %f AND %f AND latitude BETWEEN %f AND %f", queryTableName
                , predicate.getStartTimestamp(), predicate.getStopTimestamp()
                , predicate.getLowerLeft().getLongitude(), predicate.getUpperRight().getLongitude()
                , predicate.getLowerLeft().getLatitude(), predicate.getUpperRight().getLatitude());

        return SQLString;
    }

}
