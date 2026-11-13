--reporting queries on the warehouse (requirement R5): olap operations in hiveql.
--run after hive/03_warehouse.sql:  hive -f hive/04_reports.sql 2>/dev/null
--water is in imperial gallons (IG); 1 cubic metre = 219.969 IG. electricity is in kWh.
--in ROLLUP, CUBE and GROUPING SETS results, NULL in a grouping column means "all".

USE dewa_dw;

--1. roll-up along the time and location hierarchies: water use per month and area,
--with a subtotal per month and a grand total (ROLLUP on n columns = n + 1 groupings)
SELECT d.month_name, m.area,
       ROUND(SUM(f.consumption) / 219.969, 1) AS water_m3
FROM fact_daily_usage f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_meter m ON f.meter_id = m.meter_id
WHERE m.meter_type = 'water'
GROUP BY d.month_name, m.area WITH ROLLUP
ORDER BY month_name, area;

--2. cube over two independent dimensions: average daily water use per premise, by
--premise type and by weekday or weekend, with every subtotal (CUBE = 2^n groupings)
SELECT m.premise_type, d.is_weekend,
       ROUND(AVG(f.consumption), 1) AS avg_daily_ig
FROM fact_daily_usage f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_meter m ON f.meter_id = m.meter_id
WHERE m.meter_type = 'water'
GROUP BY m.premise_type, d.is_weekend WITH CUBE
ORDER BY premise_type, is_weekend;

--3. grouping sets: alerts per area and type, per type, and in total (the second fact
--table, sharing dim_meter and dim_date with the first)
SELECT m.area, t.alert_type,
       COUNT(*) AS meter_days,
       SUM(f.alert_windows) AS alert_windows
FROM fact_alert f
JOIN dim_meter m ON f.meter_id = m.meter_id
JOIN dim_alert_type t ON f.alert_type_id = t.alert_type_id
GROUP BY m.area, t.alert_type
GROUPING SETS ((m.area, t.alert_type), (t.alert_type), ())
ORDER BY area, alert_type;

--4. monthly billing extract: one row per premise and month, water and electricity
--side by side (slice on one month: the partition filter reads only that folder)
SELECT f.usage_month, m.premise_number, m.premise_type, m.area,
       ROUND(SUM(CASE WHEN m.meter_type = 'water' THEN f.consumption END) / 219.969, 2) AS water_m3,
       ROUND(SUM(CASE WHEN m.meter_type = 'electricity' THEN f.consumption END), 1) AS electricity_kwh,
       SUM(f.missing_readings) AS missing_readings
FROM fact_daily_usage f
JOIN dim_meter m ON f.meter_id = m.meter_id
WHERE f.usage_month = '2026-07'
GROUP BY f.usage_month, m.premise_number, m.premise_type, m.area
ORDER BY premise_number
LIMIT 10;

--5. window function: the three biggest water users in each area this month
SELECT area, premise_number, premise_type, water_m3, area_rank
FROM (
  SELECT area, premise_number, premise_type, water_m3,
         RANK() OVER (PARTITION BY area ORDER BY water_m3 DESC) AS area_rank
  FROM (
    SELECT m.area, m.premise_number, m.premise_type,
           ROUND(SUM(f.consumption) / 219.969, 1) AS water_m3
    FROM fact_daily_usage f
    JOIN dim_meter m ON f.meter_id = m.meter_id
    WHERE m.meter_type = 'water' AND f.usage_month = '2026-07'
    GROUP BY m.area, m.premise_number, m.premise_type
  ) per_premise
) ranked
WHERE area_rank <= 3
ORDER BY area, area_rank;

--6. dice: data completeness on weekdays, per area and meter type (missing readings
--come from silent meters and communication gaps)
SELECT m.area, m.meter_type,
       SUM(f.missing_readings) AS missing_readings,
       ROUND(100 * SUM(f.missing_readings) / SUM(f.readings + f.missing_readings), 3) AS missing_pct
FROM fact_daily_usage f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_meter m ON f.meter_id = m.meter_id
WHERE d.is_weekend = false
GROUP BY m.area, m.meter_type
HAVING SUM(f.missing_readings) > 0
ORDER BY missing_readings DESC
LIMIT 10;
