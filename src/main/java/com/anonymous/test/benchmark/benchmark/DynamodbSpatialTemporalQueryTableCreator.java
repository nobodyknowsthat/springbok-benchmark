package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.dynamodb.SpatialTemporalStorageDriver;
import com.anonymous.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-06-08 8:59 PM
 **/
public class DynamodbSpatialTemporalQueryTableCreator {

    public static String TABLE_NAME = "SpatialTemporalTableQuery2";

    public static String PARTITION_KEY_NAME = "myPartitionId";

    public static String SORT_KEY_NAME = "myTimestampOid";

    public static String FILENAME = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_10w.csv";

    public static void createTableAndInsert() {
        //SpatialTemporalStorageDriver.deleteTable(TABLE_NAME);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(FILENAME);
        SpatialTemporalStorageDriver.createTable(TABLE_NAME, PARTITION_KEY_NAME, SORT_KEY_NAME);

        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            dataBatch.add(point);
            if (dataBatch.size() == 10000) {
                SpatialTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, TABLE_NAME, PARTITION_KEY_NAME, SORT_KEY_NAME);
                dataBatch.clear();
                System.out.println("count: " + count);
            }

        }
        SpatialTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, TABLE_NAME, PARTITION_KEY_NAME, SORT_KEY_NAME);


    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        createTableAndInsert();
        long stop = System.currentTimeMillis();

        System.out.println("insertion time: " + (stop -start) + " ms");
    }

}
