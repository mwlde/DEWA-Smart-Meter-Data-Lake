# To do

Status of the code and the evidence the report needs. Tick items off as they are done. Measured results go in [RESULTS.md](RESULTS.md).

## Build

- [x] 1. Synthetic data generator (`data_generator/generate.py`)
- [x] 2. Container setup script (`scripts/setup_container.sh`, `scripts/check_tools.sh`)
- [x] 3. Raw zone: data loaded into HDFS, Hive tables over it (`scripts/load_raw_zone.sh`, `hive/01_raw_zone.sql`)
- [x] 4. Generator weighted towards leaks; spikes generated as meter glitches
- [x] 5. Kafka topics and replay script (`scripts/start_kafka.sh`, `streaming/replay.py`)
- [x] 6. Streaming detection job: leak, spike, negative, stuck, silent (`streaming/detect_stream.py`), with the baseline batch job it uses (`streaming/baseline.py`). Stops by itself 30 seconds after the replay ends
- [x] 7. HBase tables for each meter's readings and open alerts, fed by the streaming job (`scripts/start_hbase.sh`, `streaming/hbase_rest.py`, `hbase/queries.hbase`). Run on real HBase: 30 open-alert rows for 30 anomalies
- [x] 8. Batch job: raw zone to cleansed zone (validated, deduplicated, Parquet), quarantine and curated zone (`batch/build_zones.py`, `hive/02_lake_zones.sql`). Run in the container
- [ ] 9. Hive star schema over the curated zone (design from task 5 of the team split)
- [ ] 10. Benchmark: the same leak check in MapReduce, Hive, Spark batch and Spark Streaming, timed
- [x] 11. Evaluation script: alerts against ground truth (`evaluation/evaluate.py`)
- [x] 12. Setup for Mac and Windows (`SETUP.md`, `Dockerfile`, `.gitattributes`)
- [x] 13. Results log (`RESULTS.md`)
- [x] 14. Loud failures: every script names its step and prints a FAILED banner that shows even with `2>/dev/null` (`common/errors.py`); pre-flight checks for Kafka, baseline and HBase

## Check and run

- [x] First live run of the streaming job with Kafka in the container (1,000 meters: 30 of 30 anomalies detected, no false alerts)
- [x] Zones built in the container (671,788 cleansed, 4 quarantined, 7,000 daily rows); HBase started and tables created
- [x] Full pipeline with HBase (run 3): 30 of 30 detected, no false alerts, 30 rows in HBase
- [x] Re-ran the evaluation on the run 3 alerts with the first alert per anomaly (RESULTS.md, correction section)
- [ ] Next streaming run: latency is then measured to when the alert was ready (`emitted_ts`), not the micro-batch start
- [ ] Test-build the `Dockerfile` (on Maria's Mac) and run `SETUP.md` start to finish, so Windows users can follow it
- [x] Replace `OWNER` in the clone commands in `SETUP.md` with the GitHub account name
- [ ] Calibrate consumption levels in the generator against DEWA's annual statistics on Dubai Pulse
- [ ] Sensitivity test: smaller leaks (0.2 to 1 gallon per 15 minutes) to find where detection starts to fail
- [ ] Full-size run for the final results: `--meters 10000 --days 30`

## Evidence for the report

Screenshots go in `screenshots/mac/` (Mac) or `screenshots/other/` (any other setup), named by report section (e.g. `5-3-4_hdfs_raw_zone_load.png`).

- [x] 5.1 Environment check (`check_tools.sh` output)
- [x] 5.3.4 HDFS raw zone listing and Hive partitions
- [x] 5.3.4 Hive readings per day and anomalies per type
- [x] 5.3.1 Replay sending readings into Kafka, the replay finishing, and the alerts topic in a console consumer
- [x] 5.3.2 Streaming job printing alerts as they are raised (start, negative and silent, leak, stuck)
- [x] 5.3.3 HBase: open alerts, one meter's newest readings, one alert's history
- [x] 5.3.4 Zone build summary and Hive partitions for the cleansed and quarantine zones
- [x] 5.3.4 `hdfs dfs -ls` of all zones, including landing
- [ ] 5.3.4 Hive: the leak query on `daily_meter_usage` (last query in `hive/02_lake_zones.sql`, cut off in the zones screenshot)
- [ ] 5.3.3 One clean HBase screenshot with `bash scripts/hbase_queries.sh` (the current ones carry HBase's log lines)
- [ ] 5.3.5 Star schema queries in Hive
- [ ] 5.5 Benchmark table (runtime and time to alert for each approach)
- [x] 7.1 Evaluation table, 1,000-meter run (`7-1_evaluation_results_1000_meters.png`)
- [ ] 7.1 Evaluation table, full-size run
