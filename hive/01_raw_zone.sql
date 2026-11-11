-- Raw zone of the data lake: Hive external tables over the generated CSVs in HDFS.
-- External means Hive only reads the files; dropping a table never deletes the data.
--
-- Expects the data in HDFS at:
--   /user/hadoop/dewa/raw/meters/meters.csv
--   /user/hadoop/dewa/raw/readings/dt=YYYY-MM-DD/readings.csv
--   /user/hadoop/dewa/raw/ground_truth/ground_truth.csv
--
-- Run with: hive -f hive/01_raw_zone.sql

CREATE DATABASE IF NOT EXISTS dewa;
USE dewa;

DROP TABLE IF EXISTS raw_meters;
CREATE EXTERNAL TABLE raw_meters (
  meter_id      STRING,
  meter_type    STRING,
  premise_id    STRING,
  premise_type  STRING,
  area          STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/dewa/raw/meters'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS raw_readings;
CREATE EXTERNAL TABLE raw_readings (
  meter_id          STRING,
  meter_type        STRING,
  premise_id        STRING,
  area              STRING,
  ts                STRING,   -- ISO format, e.g. 2026-07-01T00:15:00
  interval_value    DOUBLE,
  unit              STRING,
  cumulative_value  DOUBLE,
  signal_quality    INT
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/dewa/raw/readings'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Register the dt=... folders as partitions
MSCK REPAIR TABLE raw_readings;

DROP TABLE IF EXISTS raw_ground_truth;
CREATE EXTERNAL TABLE raw_ground_truth (
  anomaly_id    STRING,
  meter_id      STRING,
  anomaly_type  STRING,
  start_ts      STRING,
  end_ts        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/dewa/raw/ground_truth'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Checks (screenshot these for the report)
SHOW PARTITIONS raw_readings;

SELECT dt, meter_type, COUNT(*) AS readings, ROUND(SUM(interval_value), 1) AS total
FROM raw_readings
GROUP BY dt, meter_type
ORDER BY dt, meter_type;

SELECT anomaly_type, COUNT(*) AS injected
FROM raw_ground_truth
GROUP BY anomaly_type;
