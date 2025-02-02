package com.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PostgresCdcJob {
    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Add sources to the environment and create temporary tables
        tableEnv.executeSql(
            "CREATE TEMPORARY TABLE website_event (" +
            "  event_id STRING," +
            "  website_id STRING," +
            "  session_id STRING," +
            "  event_type STRING," +
            "  event_value STRING," +
            "  created_at TIMESTAMP(3)," +
            "  PRIMARY KEY (event_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'postgres-cdc'," +
            "  'hostname' = '192.168.1.173'," +
            "  'port' = '5432'," +
            "  'username' = 'postgres'," +
            "  'password' = 'changeit'," +
            "  'database-name' = 'umami'," +
            "  'schema-name' = 'public'," +
            "  'table-name' = 'website_event'," +
            "  'slot.name' = 'debezium_event'," +
            "  'decoding.plugin.name' = 'pgoutput'," +
            // database.server.name is a logical name, must be unique for each connector.
            "  'debezium.database.server.name' = 'website_event_server'," +
            "  'debezium.snapshot.mode' = 'initial'," +
            "  'debezium.slot.drop.on.stop' = 'false'" +
            ")"
        );

        tableEnv.executeSql(
            "CREATE TEMPORARY TABLE session (" +
            "  session_id STRING," +
            "  website_id STRING," +
            "  created_at TIMESTAMP(3)," +
            "  hostname STRING," +
            "  browser STRING," +
            "  os STRING," +
            "  device STRING," +
            "  screen STRING," +
            "  `language` STRING," +
            "  country STRING," +
            "  PRIMARY KEY (session_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'postgres-cdc'," +
            "  'hostname' = '192.168.1.173'," +
            "  'port' = '5432'," +
            "  'username' = 'postgres'," +
            "  'password' = 'changeit'," +
            "  'database-name' = 'umami'," +
            "  'schema-name' = 'public'," +
            "  'table-name' = 'session'," +
            "  'slot.name' = 'debezium_session'," +
            "  'decoding.plugin.name' = 'pgoutput'," +
            "  'debezium.database.server.name' = 'session_server'," +
            "  'debezium.snapshot.mode' = 'initial'," +
            "  'debezium.slot.drop.on.stop' = 'false'" +
            ")"
        );

        // Create and execute the join query
        Table resultTable = tableEnv.sqlQuery(
            "SELECT " +
            "  e.event_id," +
            "  e.website_id," +
            "  e.session_id," +
            "  e.event_type," +
            "  e.event_value," +
            "  e.created_at as event_created_at," +
            "  s.hostname," +
            "  s.browser," +
            "  s.os," +
            "  s.device," +
            "  s.screen," +
            "  s.`language`," +
            "  s.country," +
            "  s.created_at as session_created_at " +
            "FROM website_event e " +
            "JOIN session s ON e.session_id = s.session_id"
        );

        // Convert the Table to a RetractStream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        // Create Redis sink
        String redisNodes = "192.168.1.173:31001,192.168.1.173:31002,192.168.1.173:31003,192.168.1.173:31004,192.168.1.173:31005,192.168.1.173:31006";
        RedisClusterSinkFunction redisSink = new RedisClusterSinkFunction(redisNodes, "session_event");

        // Add the Redis sink to the result stream
        resultStream.addSink(redisSink);

        // Execute the job
        env.execute("PostgreSQL CDC - Session Events View");
    }
} 