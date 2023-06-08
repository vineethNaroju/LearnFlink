package org.rough.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.*;

public class IcebergSource {

    public static void main(String[] args)  {
        IcebergSource ia = new IcebergSource();

        try {
            ia.useTable(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        } catch (Exception e) {
            //ignore it
        }
    }

    public void useTable(int loop, int cnt ) throws Exception {
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
                " 'warehouse' = 'hdfs:///iceberg/catalog', " +
                " 'property-version' = '1' " +
                ");");

        tableEnv.useCatalog("hadoop_catalog");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg_db;");

        tableEnv.useDatabase("iceberg_db");

        tableEnv.executeSql("SHOW CURRENT CATALOG").print();

        tableEnv.executeSql("SHOW CURRENT DATABASE").print();

        tableEnv.executeSql("SHOW TABLES").print();

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS source_table (num BIGINT);");

        int k = 0, n = cnt;

        while(true) {
            try {
                StringBuilder sb = new StringBuilder("INSERT INTO source_table values");

                for(int i=0; i<n; i++) {
                    k++;

                    sb.append(" (" + k + ")");

                    if(i + 1 == n) {
                        sb.append(";");
                    } else {
                        sb.append(",");
                    }
                }
                tableEnv.executeSql(sb.toString());
                Thread.sleep( loop * 1_000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
