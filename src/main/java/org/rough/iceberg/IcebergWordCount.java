package org.rough.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class IcebergWordCount {

    public void streamEnv() {
        /* Alternatively, users can create a StreamTableEnvironment from an existing
         * StreamExecutionEnvironment to interoperate with the DataStream API.
         * */
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(see);
    }

    public static void main(String[] args)  throws Exception {
        IcebergWordCount iwc = new IcebergWordCount();
        iwc.demo();
    }

    public void demo() throws Exception {
        final Configuration config = new Configuration();

        config.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, Boolean.FALSE);

        // config.set(StreamFormat.FETCH_IO_SIZE, MemorySize.ofMebiBytes(1L));

        EnvironmentSettings envSettings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .withConfiguration(config)
                .build();

        final TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        // SET CONFIG
        tableEnv.executeSql("SET 'execution.runtime-mode' = 'streaming';");
        tableEnv.executeSql("SET 'sql-client.execution.result-mode' = 'changelog';");

        // CATALOG CREATION
        tableEnv.executeSql("create catalog hadoop_catalog with (" +
                "'type' = 'iceberg', " +
                "'catalog-type' = 'hadoop', " +
                "'warehouse' = 'hdfs://localhost:9000/iceberg/catalog', " +
                "'property-version' = '1');");

        tableEnv.executeSql("use catalog hadoop_catalog;");

        // DATABASE CREATION
        tableEnv.executeSql("create database if not exists iceberg_db;");
        tableEnv.executeSql("use iceberg_db");

        // SOURCE TABLE CREATION
        tableEnv.executeSql("create table if not exists source_table (" +
                "id BIGINT comment 'unique id', " +
                "data STRING, " +
                "type STRING" +
                ") partitioned by (type);");

        // SINK TABLE CREATION
        tableEnv.executeSql("create table if not exists sink_table (" +
                "data STRING);");

        // source count table creation
        tableEnv.executeSql("create table if not exists source_count_table (cnt BIGINT);");


        // SOURCE TABLE DATA GENERATOR
        Thread datagenThread = new Thread(() -> {

            while (true) {
                try {
                    StatementSet statementSet = tableEnv.createStatementSet();

                    StringBuilder stringBuilder = new StringBuilder("insert into `hadoop_catalog`.`iceberg_db`.`source_table` values");

                    for(int i=0; i<100; i++) {

                        stringBuilder.append(String.format("(%d, 'iceberg flink', 'text'), ", i));

//                        String cmd = String.format(
//                                "insert into `hadoop_catalog`.`iceberg_db`.`source_table` values (%d, '%s', 'text');",
//                                i, Data.content);

//                        statementSet.addInsertSql(cmd);
                    }

                    stringBuilder.append(String.format("(999, 'iceberg flink', 'text');"));

                    statementSet.addInsertSql(stringBuilder.toString());

                    statementSet.execute();

                    Thread.sleep(30 * 1_000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        datagenThread.start();

        Thread.sleep(5000);

        tableEnv.executeSql("insert into `hadoop_catalog`.`iceberg_db`.`source_count_table` " +
                "select count(*) from `hadoop_catalog`.`iceberg_db`.`source_table`;");

//        CountDownLatch latch = new CountDownLatch(1);
//
////        tableEnv.executeSql("insert into `hadoop_catalog`.`iceberg_db`.`sink_table` " +
////                "select data from `hadoop_catalog`.`iceberg_db`.`source_table`");
//
//        // tableEnv.executeSql("select * from hadoop_catalog.iceberg_db.source_table;").print();
//
//        latch.await();
    }

    public void demop() {






        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("SET 'execution.runtime-mode' = 'streaming';");

        tableEnv.executeSql("SET 'sql-client.execution.result-mode' = 'changelog';");

        tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH ( " +
                " 'type' = 'iceberg', " +
                " 'catalog-type' = 'hadoop', " +
                " 'warehouse' = 'hdfs://localhost:9000/iceberg/catalog', " +
                " 'property-version' = '1' " +
                ");");

        tableEnv.useCatalog("hadoop_catalog");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg_db;");

        tableEnv.useDatabase("iceberg_db");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS source_table (num BIGINT);");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS source_avg_table (avgnum BIGINT);");


        Table sourceTable = tableEnv.from("source_table");

        DataStream<Row> dataStream = tableEnv.toDataStream(sourceTable);

        tableEnv.fromDataStream(dataStream.countWindowAll(10).sum("num")).execute();
    }
}
