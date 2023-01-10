package com.anonymous.test.benchmark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author anonymous
 * @create 2022-10-03 10:47 AM
 **/
public class HBaseDriver {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Configuration configuration;

    private Connection connection;


    public HBaseDriver(String zookeeperUrl) {
        try {
            this.configuration = HBaseConfiguration.create();
            this.configuration.set("hbase.zookeeper.quorum", zookeeperUrl);
            this.connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            table.addFamily(new HColumnDescriptor("cf"));

            if (admin.tableExists(TableName.valueOf(tableName))) {
                logger.info(tableName + " exists");
            } else {
                admin.createTable(table);
                logger.info("Create table: " + tableName);
            }
        }

    }

    public void createTable(String tableName, List<String> familyNameList) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

            for (String familyName : familyNameList) {
                table.addFamily(new HColumnDescriptor(familyName));
            }
            if (admin.tableExists(TableName.valueOf(tableName))) {
                logger.info(tableName + " exists");
            } else {
                admin.createTable(table);
                logger.info("Create table: " + tableName);
            }
        }

    }



    public void put(String tableName, Put put) throws IOException {
        try (Table hTable = connection.getTable(TableName.valueOf(tableName))) {
            hTable.put(put);
        }

    }

    public void batchPut(String tableName, List<Put> putList) throws IOException {
        try (Table hTable = connection.getTable(TableName.valueOf(tableName))) {
            hTable.put(putList);
        }
    }

    public Result get(String tableName, byte[] rowKey) throws IOException {
        try (Table hTable = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowKey);
            Result result = hTable.get(get);

            return result;
        }
    }

    public Result[] batchGet(String tableName, List<Get> getList) throws IOException {
        try (Table hTable = connection.getTable(TableName.valueOf(tableName))) {
            return hTable.get(getList);
        }
    }


    public Result get(String tableName, byte[] rowKey, byte[] columnFamily, byte[] qualifier) throws IOException {
        try (Table hTable = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowKey);
            get.addColumn(columnFamily, qualifier);
            Result result = hTable.get(get);

            return result;
        }
    }

    public List<Result> scan(String tableName, byte[] startKey, byte[] stopKey) throws IOException {
        List<Result> resultList = new ArrayList<>();

        try (Table hTable = connection.getTable(TableName.valueOf(tableName))) {

            Scan scan = new Scan();
            //scan.withStartRow(startKey, true);
            //scan.withStopRow(stopKey, true);
            scan.setStartRow(startKey);
            scan.setStopRow(stopKey);
            //scan.withStartRow(startKey, true);
            //scan.withStopRow(stopKey, true);
            ResultScanner results = hTable.getScanner(scan);

            for (Result result : results) {
                resultList.add(result);
            }

            return resultList;
        }
    }

    public ResultScanner scan(String tableName) throws IOException {

        try (Table hTable = connection.getTable(TableName.valueOf(tableName))) {

            Scan scan = new Scan();
            ResultScanner resultScanner = hTable.getScanner(scan);

            return resultScanner;
        }
    }

    public void delete(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));

        }
    }

}
