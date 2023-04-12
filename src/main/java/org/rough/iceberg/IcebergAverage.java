package org.rough.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.*;

import java.util.concurrent.CountDownLatch;

public class IcebergAverage {

    public static void main(String[] args)  {
        IcebergAverage ia = new IcebergAverage();

        try {
            ia.useTable();
        } catch (Exception e) {
            //ignore it
        }
    }

    public void useTable( ) throws Exception {
        final Configuration config = new Configuration();

        config.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, Boolean.FALSE);

        final EnvironmentSettings envSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .withConfiguration(config)
                .build();

        final TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH ( " +
                " 'type' = 'iceberg', " +
                " 'catalog-type' = 'hadoop', " +
                " 'warehouse' = 'hdfs://localhost:9000/iceberg/catalog', " +
                " 'property-version' = '1' " +
                ");");

        tableEnv.useCatalog("hadoop_catalog");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg_db;");

        tableEnv.useDatabase("iceberg_db");

        tableEnv.executeSql("SHOW CURRENT CATALOG").print();

        tableEnv.executeSql("SHOW CURRENT DATABASE").print();

        tableEnv.executeSql("SHOW TABLES").print();

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS source_table (num BIGINT);");

        Thread datagenThread = new Thread(() -> {

            int k = 0, n = 100;

            while (true) {

                try {

                    StringBuilder stringBuilder =
                            new StringBuilder("INSERT INTO source_table values ");

                    for(int i=0; i<n-1; i++) {
                        stringBuilder.append(String.format("(%d), ", ++k));
                    }

                    stringBuilder.append(String.format("(%d);", ++k));

                    String cmd = stringBuilder.toString();

                    tableEnv.executeSql(cmd);

                    Thread.sleep(30 * 1_000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        datagenThread.start();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }
}
