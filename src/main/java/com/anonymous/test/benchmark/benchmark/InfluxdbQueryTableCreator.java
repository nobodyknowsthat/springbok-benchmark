package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.geomesa.client.TimeRecorder;
import com.anonymous.test.benchmark.influxdb.InfluxdbStorageDriver;
import com.anonymous.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.List;

import com.anonymous.test.benchmark.influxdb.InfluxdbStorageNewDriver;

/**
 * @author anonymous
 * @create 2022-10-05 2:50 PM
 **/
public class InfluxdbQueryTableCreator {

    public static String FILENAME = "/home/ubuntu/dataset/porto_data_v1.csv";

    public static String TABLE_NAME = "porto-data-table";

    public static int createTableAndInsert() {
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(FILENAME);

        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            dataBatch.add(point);
            //System.out.println(point.getTimestamp() + ", " + point.getOid());
            if (dataBatch.size() == 20000) {
                InfluxdbStorageNewDriver.writeTrajectoryPoints(dataBatch);
                dataBatch.clear();
            }
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
            }

        }
        InfluxdbStorageDriver.writeTrajectoryPoints(dataBatch);
        return count;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int count = createTableAndInsert();
        long stop = System.currentTimeMillis();
        System.out.println("total record count: " + count);
        System.out.println("insertion time: " + (stop - start) + " ms");
        String log = String.format("[InfluxDB 20000 batch new driver] record count: %d, total time: %d ms \n", count, (stop - start));
        TimeRecorder.recordTime("insertion-time.log", log);
    }

}
