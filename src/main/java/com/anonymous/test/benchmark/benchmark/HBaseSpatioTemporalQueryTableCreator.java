package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.geomesa.client.TimeRecorder;
import com.anonymous.test.benchmark.hbase.HBaseSpatioTemporalStorageDriver;
import com.anonymous.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-10-05 2:34 PM
 **/
public class HBaseSpatioTemporalQueryTableCreator {

    public static String FILENAME = "/home/ubuntu/data/porto_data_v1_5x.csv";

    public static String TABLE_NAME = "porto-5x-spatiotemporal-table";

    public static int createTableAndInsert() {
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(FILENAME);
        HBaseSpatioTemporalStorageDriver.createTable(TABLE_NAME);
        List<TrajectoryPoint> dataBatch = new ArrayList<>();

        TrajectoryPoint point;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            dataBatch.add(point);
            //System.out.println(point.getTimestamp() + ", " + point.getOid());
            if (dataBatch.size() == 20000) {
                HBaseSpatioTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, TABLE_NAME);
                dataBatch.clear();
            }
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
            }

        }
        HBaseSpatioTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, TABLE_NAME);
        return count;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int count = createTableAndInsert();
        long stop = System.currentTimeMillis();
        System.out.println("total record count: " + count);
        System.out.println("insertion time: " + (stop - start) + " ms");
        String log = String.format("[HBase][Spatio Temporal Insertion] record count: %d, total time: %d ms \n", count, (stop - start));
        TimeRecorder.recordTime("insertion-time.log", log);
    }
}
