# PostgreSQL CDC with Apache Flink

This guide demonstrates how to set up Change Data Capture (CDC) from PostgreSQL to Apache Flink.

## Prerequisites
- PostgreSQL 10+
- Apache Flink 1.17+
- Java 17+
- Network access between Flink and PostgreSQL

## 1. PostgreSQL Configuration

### 1.1 Enable Logical Replication
Edit `postgresql.conf`:
```
wal_level = logical # Enable logical decoding
max_wal_senders = 10 # Number of simultaneous replication slots
max_replication_slots = 10 # Number of replication slots
```

Create a replication user:
```
GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;
```

Create a replication slot:
```
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');

SELECT pg_drop_replication_slot('flink_slot');


 SELECT * FROM pg_replication_slots;
```


