package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.springbok.SpringbokDriver;
import com.anonymous.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-06-28 2:39 PM
 **/
public class SpringbokQueryTableCreator {

    public static void createAndInsertData() {
        String dataFile = "/home/ubuntu/data/porto_data_v1_5x.csv";
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(dataFile);

        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            dataBatch.add(point);
            if (dataBatch.size() == 20000) {
                SpringbokDriver.insertData(dataBatch);
                dataBatch.clear();
            }
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
            }
        }

    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        createAndInsertData();
        long stop = System.currentTimeMillis();
        System.out.println("insertion time: " + (stop - start) + " ms");
    }

}
