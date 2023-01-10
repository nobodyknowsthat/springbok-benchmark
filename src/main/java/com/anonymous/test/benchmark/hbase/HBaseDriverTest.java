package com.anonymous.test.benchmark.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * @author anonymous
 * @create 2022-10-03 11:06 AM
 **/
public class HBaseDriverTest {
    public static void main(String[] args) throws Exception {
        HBaseDriver driver = new HBaseDriver("127.0.0.1");
        String tableName = "testtable";
        testScan(driver, tableName);

    }

    public static void testPut(HBaseDriver driver, String tableName) throws Exception {
        String testKey = "testkey";
        Put put = new Put(Bytes.toBytes(testKey));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("testqualifier"), Bytes.toBytes("testvalue"));
        driver.put(tableName, put);
    }

    public static void testScan(HBaseDriver driver, String tableName) throws Exception {
        String startKey = "testkey";
        String stopKey = "testkeyStop";
        List<Result> resultList = driver.scan(tableName, Bytes.toBytes(startKey), Bytes.toBytes(stopKey));
        System.out.println(resultList.size());
        for (Result result : resultList) {
            for (Cell cell : result.listCells()) {
                System.out.println("value offset: " + cell.getValueOffset());
                System.out.println("value length: " + cell.getValueLength());
                System.out.println(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }
    }
}
