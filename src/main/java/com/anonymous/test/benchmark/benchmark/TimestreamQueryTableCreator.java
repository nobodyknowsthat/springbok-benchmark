package com.anonymous.test.benchmark.benchmark;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.timestream.TimestreamStorageDriver;
import com.anonymous.test.common.TrajectoryPoint;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-06-08 8:17 PM
 **/
public class TimestreamQueryTableCreator {

    public static String DATABASE_NAME = "benchmark";

    public static String TABLE_NAME = "trajectoryForQuery2";

    public static long HT_TTL_HOURS = 24 * 30 * 12;

    public static long CT_TTL_DAYS = 1;

    public static String CURRENT_DATE = "2021-07-07";

    static String FILE_NAME = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_100w.csv";

    public static void createAndInsertData() throws ParseException {
        //TimestreamStorageDriver.deleteTable(DATABASE_NAME, TABLE_NAME);
        //TimestreamStorageDriver.createTable(DATABASE_NAME, TABLE_NAME, HT_TTL_HOURS, CT_TTL_DAYS);
        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(FILE_NAME);

        long timestampOffset = getTimestampOffset(CURRENT_DATE);

        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            dataBatch.add(point);
            if (dataBatch.size() == 10000) {
                TimestreamStorageDriver.writeMultiMeasureRecordsHistorical(dataBatch, DATABASE_NAME, TABLE_NAME, timestampOffset);
                dataBatch.clear();

            }

            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
                System.out.println("current timestamp: " + System.currentTimeMillis());
            }

        }
        TimestreamStorageDriver.writeMultiMeasureRecords(dataBatch, DATABASE_NAME, TABLE_NAME);

    }

    public static long getTimestampOffset(String currentDateString) throws ParseException {
        long firstTimestampInPorto = 1372636853000L; // unit is millisecond
        DateFormat df = new SimpleDateFormat("yyy-MM-dd");
        Date date = df.parse(currentDateString);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        long timestampMillis = calendar.getTimeInMillis();

        long timestampOffset = timestampMillis - firstTimestampInPorto;
        return timestampOffset;
    }


    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            createAndInsertData();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long stop = System.currentTimeMillis();
        System.out.println("insertion time: " + (stop - start) + " ms");
    }

}
