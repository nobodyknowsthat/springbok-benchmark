package com.anonymous.test.benchmark.geomesa.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/3/30 14:44
 * @Created by X1 Carbon
 */
public class TimeRecorder {

    private static List<String> recordList = new ArrayList<>();

    private static int batchSize = 8192;

    public static void recordTime(String filePath, String record) {
        /*recordList.add(record);
        if (recordList.size() % batchSize == 0) {

            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
                for (String item : recordList) {
                    writer.write(item);
                }
                writer.flush();
                writer.close();
                recordList.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }*/



        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));

            writer.write(record);

            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
