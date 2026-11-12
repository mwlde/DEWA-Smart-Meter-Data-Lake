--benchmark, hive version: the same night-flow leak rule as the streaming job.
--hive 3 runs this as mapreduce jobs on yarn (hive.execution.engine=mr).
--reads the raw zone table (hive/01_raw_zone.sql) and writes one line per leaking
--meter and night to hdfs: meter_id,night,leak_windows
--run by scripts/run_benchmark.sh, or alone with: hive -f benchmark/leak_check.hql
--a leak window: 4 readings, 45 mins from first to last (none missing), all above 0.5 gallons.
--no comments inside the statement below: some hive versions mishandle them

USE dewa;

INSERT OVERWRITE DIRECTORY '/user/hadoop/dewa/benchmark/hive'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT meter_id, dt AS night, COUNT(*) AS leak_windows
FROM (
  SELECT meter_id, dt, ts,
         MIN(interval_value) OVER w AS min_flow,
         COUNT(*) OVER w AS n,
         MAX(ts) OVER w AS last_ts
  FROM raw_readings
  WHERE meter_type = 'water' AND substr(ts, 12, 2) IN ('01', '02', '03')
  WINDOW w AS (PARTITION BY meter_id, dt ORDER BY ts ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING)
) x
WHERE n = 4 AND min_flow > 0.5
  AND unix_timestamp(last_ts, "yyyy-MM-dd'T'HH:mm:ss") - unix_timestamp(ts, "yyyy-MM-dd'T'HH:mm:ss") = 2700
GROUP BY meter_id, dt;
