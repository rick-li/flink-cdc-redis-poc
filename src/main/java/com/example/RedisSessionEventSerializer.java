package com.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public class RedisSessionEventSerializer {
    public static Map<String, String> serializeRow(RowData row) {
        Map<String, String> hash = new HashMap<>();
        
        // Extract and serialize each field
        hash.put("event_id", getString(row, 0));
        hash.put("website_id", getString(row, 1));
        hash.put("session_id", getString(row, 2));
        hash.put("event_type", getString(row, 3));
        hash.put("event_value", getString(row, 4));
        hash.put("event_created_at", getTimestamp(row, 5));
        hash.put("hostname", getString(row, 6));
        hash.put("browser", getString(row, 7));
        hash.put("os", getString(row, 8));
        hash.put("device", getString(row, 9));
        hash.put("screen", getString(row, 10));
        hash.put("language", getString(row, 11));
        hash.put("country", getString(row, 12));
        hash.put("session_created_at", getTimestamp(row, 13));
        
        return hash;
    }
    
    private static String getString(RowData row, int pos) {
        StringData data = row.getString(pos);
        return data != null ? data.toString() : "";
    }
    
    private static String getTimestamp(RowData row, int pos) {
        TimestampData timestamp = row.getTimestamp(pos, 3);
        return timestamp != null ? timestamp.toString() : "";
    }
} 