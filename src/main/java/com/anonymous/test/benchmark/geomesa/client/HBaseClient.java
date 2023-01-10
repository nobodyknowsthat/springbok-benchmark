/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.anonymous.test.benchmark.geomesa.client;

import org.apache.commons.cli.ParseException;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;

public class HBaseClient extends GeoMesaClient {



    public HBaseClient(String[] args, CommonData data, boolean readOnly, String logFilename) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), data, readOnly, logFilename);
    }

    public static void main(String[] args) {
        try {
            String[] myargs = new String[4];
            myargs[0] = "--hbase.zookeepers";
            //myargs[1] = "44.204.240.14";
            myargs[1] = "44.201.250.21";
            myargs[2] = "--hbase.catalog";
            //myargs[3] = "geomesa-porto-benchmark";
            myargs[3] = "geomesa-porto-mycost";
            String logFilename = "test-cost.log";
            String queryFilename = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_1h_01.query";
            //String queryFilename = null;

            String dataFilename = "/home/ubuntu/dataset/porto_data_v1.csv";

            CommonData data = new GeoMesaPortoTaxiData(dataFilename, queryFilename,"spatiotemporal");

            HBaseClient client = new HBaseClient(myargs, data, true, logFilename);
            client.myExecute();
            client.myExecute();
            client.myExecute();
            client.myExecute();


        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
