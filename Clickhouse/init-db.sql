-- init-db.sql
CREATE DATABASE IF NOT EXISTS psykalytics;

USE psykalytics;

CREATE TABLE IF NOT EXISTS events (
    id UUID,
    type String,
    session_id UUID,
    timestamp DateTime64,
    sint Int32,
    lint Int64,
    sstr String,
    lstr String
) ENGINE = MergeTree()
ORDER BY (session_id, timestamp);
