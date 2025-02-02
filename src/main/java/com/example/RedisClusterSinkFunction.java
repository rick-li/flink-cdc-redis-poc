package com.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

public class RedisClusterSinkFunction extends RichSinkFunction<Row> {
    private final String nodes;
    private final String keyPrefix;
    private transient RedisClusterClient redisClient;
    private transient StatefulRedisClusterConnection<String, String> connection;
    private transient RedisAdvancedClusterCommands<String, String> syncCommands;

    public RedisClusterSinkFunction(String nodes, String keyPrefix) {
        this.nodes = nodes;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisClient = RedisClusterClient.create("redis://:changeit@" + nodes.split(",")[0]);
        connection = redisClient.connect();
        syncCommands = connection.sync();
    }

    @Override
    public void invoke(Row element, Context context) throws Exception {
        try {
            String sessionId = element.getField(2).toString(); // session_id is at index 2
            String key = "session:" + sessionId;
            
            // Create JSON string of the event
            Map<String, String> eventMap = new HashMap<>();
            eventMap.put("event_id", getStringField(element, 0));
            eventMap.put("website_id", getStringField(element, 1));
            eventMap.put("event_type", getStringField(element, 3));
            eventMap.put("event_value", getStringField(element, 4));
            eventMap.put("event_created_at", getStringField(element, 5));
            eventMap.put("hostname", getStringField(element, 6));
            eventMap.put("browser", getStringField(element, 7));
            eventMap.put("os", getStringField(element, 8));
            eventMap.put("device", getStringField(element, 9));
            eventMap.put("screen", getStringField(element, 10));
            eventMap.put("language", getStringField(element, 11));
            eventMap.put("country", getStringField(element, 12));
            
            // Convert map to JSON string
            String eventJson = mapToJson(eventMap);
            
            // Add to Redis list
            syncCommands.rpush(key, eventJson);
        } catch (Exception e) {
            System.err.println("Error writing to Redis: " + e.getMessage());
        }
    }

    private String getStringField(Row row, int pos) {
        Object field = row.getField(pos);
        return field != null ? field.toString() : "";
    }

    private String mapToJson(Map<String, String> map) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) {
                json.append(",");
            }
            json.append("\"").append(entry.getKey()).append("\":\"")
                .append(entry.getValue().replace("\"", "\\\"")).append("\"");
            first = false;
        }
        json.append("}");
        return json.toString();
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
} 