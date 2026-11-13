--reporting warehouse: hive tables over the files written by batch/build_warehouse.py.
--a star schema with two fact tables sharing the dimensions (a constellation).
--kept in its own database, dewa_dw, apart from the lake zone tables in dewa.
--
--  fact_daily_usage (date_id, meter_id)                 grain: one meter, one day
--  fact_alert       (date_id, meter_id, alert_type_id)  grain: one meter, one alert type, one day
--  dim_date, dim_meter, dim_alert_type                  surrogate key _id in each
--
--keys (hive does not enforce them; build_warehouse.py makes them unique):
--  primary keys   dim_date.date_id, dim_meter.meter_id, dim_alert_type.alert_type_id,
--                 fact_daily_usage (date_id, meter_id),
--                 fact_alert (date_id, meter_id, alert_type_id)
--  foreign keys   date_id to dim_date, meter_id to dim_meter, alert_type_id to dim_alert_type
--
--run with: hive -f hive/03_warehouse.sql 2>/dev/null

CREATE DATABASE IF NOT EXISTS dewa_dw;
USE dewa_dw;

DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date (
  date_id       INT,
  full_date     DATE,
  day_of_month  INT,
  day_name      STRING,
  is_weekend    BOOLEAN,
  week_of_year  INT,
  month_num     INT,
  month_name    STRING,
  quarter       INT,
  year          INT
)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/warehouse/dim_date';

--meter, premise and area in one table: the star schema flattens the hierarchy
DROP TABLE IF EXISTS dim_meter;
CREATE EXTERNAL TABLE dim_meter (
  meter_id        INT,
  meter_number    STRING,
  meter_type      STRING,
  unit            STRING,
  premise_number  STRING,
  premise_type    STRING,
  area            STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/warehouse/dim_meter';

DROP TABLE IF EXISTS dim_alert_type;
CREATE EXTERNAL TABLE dim_alert_type (
  alert_type_id  INT,
  alert_type     STRING,
  description    STRING,
  fires          STRING,
  handled_by     STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/warehouse/dim_alert_type';

--partitioned by month: a monthly report or billing run reads one folder (partition pruning)
DROP TABLE IF EXISTS fact_daily_usage;
CREATE EXTERNAL TABLE fact_daily_usage (
  date_id           INT,
  meter_id          INT,
  consumption       DOUBLE,
  readings          BIGINT,
  missing_readings  BIGINT,
  peak_interval     DOUBLE,
  min_night_flow    DOUBLE
)
PARTITIONED BY (usage_month STRING)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/warehouse/fact_daily_usage';

MSCK REPAIR TABLE fact_daily_usage;

DROP TABLE IF EXISTS fact_alert;
CREATE EXTERNAL TABLE fact_alert (
  date_id             INT,
  meter_id            INT,
  alert_type_id       INT,
  alert_windows       BIGINT,
  first_window_start  TIMESTAMP,
  last_window_end     TIMESTAMP
)
STORED AS PARQUET
LOCATION '/user/hadoop/dewa/warehouse/fact_alert';

--checks: row counts (screenshot for the report, section 5.3.5)
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_meter', COUNT(*) FROM dim_meter
UNION ALL SELECT 'dim_alert_type', COUNT(*) FROM dim_alert_type
UNION ALL SELECT 'fact_daily_usage', COUNT(*) FROM fact_daily_usage
UNION ALL SELECT 'fact_alert', COUNT(*) FROM fact_alert;
