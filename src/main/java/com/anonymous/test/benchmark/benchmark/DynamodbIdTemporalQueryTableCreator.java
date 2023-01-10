package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.dynamodb.IdTemporalStorageDriver;
import com.anonymous.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-06-08 8:52 PM
 **/
public class DynamodbIdTemporalQueryTableCreator {

    public static String TABLE_NAME = "IdTemporalTableQuery";

    public static String PARTITION_KEY_NAME = "myOid";

    public static String SORT_KEY_NAME = "myTimestamp";

    public static String FILENAME = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_10w.csv";

    public static void createTableAndInsert() {
        //IdTemporalStorageDriver.deleteTable(TABLE_NAME);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(FILENAME);
        IdTemporalStorageDriver.createTable(TABLE_NAME, PARTITION_KEY_NAME, SORT_KEY_NAME);

        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            dataBatch.add(point);
            System.out.println(point.getTimestamp() + ", " + point.getOid());
            if (dataBatch.size() == 4000) {
                IdTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, TABLE_NAME, PARTITION_KEY_NAME, SORT_KEY_NAME);
                dataBatch.clear();
            }
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
            }

        }
        IdTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, TABLE_NAME, PARTITION_KEY_NAME, SORT_KEY_NAME);


    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        createTableAndInsert();
        long stop = System.currentTimeMillis();
        System.out.println("insertion time: " + (stop - start) + " ms");
    }

}
