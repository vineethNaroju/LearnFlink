package org.rough.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WindowedAverage {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH ( " +
                " 'type' = 'iceberg', " +
                " 'catalog-type' = 'hadoop', " +
                " 'warehouse' = 'hdfs:///iceberg/catalog', " +
                " 'property-version' = '1' " +
                ")");

        tableEnv.useCatalog("hadoop_catalog");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg_db;");

        tableEnv.useDatabase("iceberg_db");

        tableEnv.executeSql("SHOW TABLES").print();

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS clicks (" +
                "cnt INT" +
                "ts TIMESTAMP(3)" +
                "WATERMARK FOR ts AS ts - INTERVAL '3' SECOND" +
                ");");

        tableEnv.executeSql("SELECT AVG(cnt), COUNT(cnt)" +
                " FROM tclicks" +
                " GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)" +
                " /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/").print();
    }
}
