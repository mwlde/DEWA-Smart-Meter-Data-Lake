--cleansed and curated zones as hive external tables (parquet files written by
--batch/build_zones.py). the reporting star schema is built on the curated tables.
--
--run with: hive -f hive/02_lake_zones.sql 2>/dev/null

USE dewa;

DROP TABLE IF EXISTS cleansed_readings;
CREATE EXTERNAL TABLE cleansed_readings (
  meter_id          STRING,
  meter_type        STRING,
  premise_id        STRING,
  area              STRING,
  event_ts          TIMESTAMP,
  interval_value    DOUBLE,
  unit              STRING,
  cumulative_value  DOUBLE,
  signal_quality    INT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/cleansed/readings';

MSCK REPAIR TABLE cleansed_readings;

DROP TABLE IF EXISTS quarantine_readings;
CREATE EXTERNAL TABLE quarantine_readings (
  meter_id          STRING,
  meter_type        STRING,
  premise_id        STRING,
  area              STRING,
  event_ts          TIMESTAMP,
  interval_value    DOUBLE,
  unit              STRING,
  cumulative_value  DOUBLE,
  signal_quality    INT,
  ts                STRING,
  reject_reason     STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/quarantine/readings';

MSCK REPAIR TABLE quarantine_readings;

DROP TABLE IF EXISTS dim_meter;
CREATE EXTERNAL TABLE dim_meter (
  meter_id      STRING,
  meter_type    STRING,
  premise_id    STRING,
  premise_type  STRING,
  area          STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/curated/dim_meter';

DROP TABLE IF EXISTS daily_meter_usage;
CREATE EXTERNAL TABLE daily_meter_usage (
  usage_date      DATE,
  meter_id        STRING,
  meter_type      STRING,
  premise_id      STRING,
  premise_type    STRING,
  area            STRING,
  total_value     DOUBLE,
  readings        BIGINT,
  max_interval    DOUBLE,
  min_night_flow  DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/curated/daily_meter_usage';

--checks (screenshot these for the report, section 5.3.4)
SELECT reject_reason, COUNT(*) AS readings FROM quarantine_readings GROUP BY reject_reason;

--water use per area per day
SELECT usage_date, area, ROUND(SUM(total_value), 1) AS water_ig
FROM daily_meter_usage
WHERE meter_type = 'water'
GROUP BY usage_date, area
ORDER BY usage_date, area
LIMIT 20;

--meters whose lowest night flow stayed above 0.5 gallons per 15 mins (likely leaks)
SELECT usage_date, meter_id, area, ROUND(min_night_flow, 2) AS min_night_flow
FROM daily_meter_usage
WHERE meter_type = 'water' AND min_night_flow > 0.5
ORDER BY usage_date, meter_id;
