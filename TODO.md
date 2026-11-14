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
- [x] 9. Hive star schema over the curated zone (`batch/build_warehouse.py`, `hive/03_warehouse.sql`, `hive/04_reports.sql`). Run in the container on the run 6 data: all six reports ran
- [x] 15. Latency fix (13 November): leak and stuck fire on the reading that completes their window (update mode); silent waits for the watermark (append). Replay-speed test (`scripts/speed_test.sh`, `evaluation/speed_compare.py`)
- [x] 16. Restart-safe alert writes (one folder per micro-batch, overwritten; HBase cell version = window start). Kill and restart tested in the container (restart test)
- [x] 17. Hard negatives: empty premises in the generator (`--vacant-rate`, default 5%), late readings in the replay (`--late-share`, 1% in `run_stream.sh`); water stuck check needs 24 hours of zero in an occupied premise
- [x] 18. MapReduce micro-batch experiment (`scripts/run_microbatch.sh`, `benchmark/stuck_mapper.py`, `benchmark/stuck_reducer.py`)
- [x] 19. Batch layer from the landing zone (`batch/build_zones.py --source landing`)
- [x] 10. Benchmark: the same leak check in MapReduce, Hive and Spark batch, timed and compared with each other, the streaming job and the injected leaks (`scripts/run_benchmark.sh`, `benchmark/`). Run in the container on three data sizes (benchmarks 1 to 3)
- [x] 11. Evaluation script: alerts against ground truth (`evaluation/evaluate.py`)
- [x] 12. Setup for Mac and Windows (`SETUP.md`, `Dockerfile`, `.gitattributes`)
- [x] 13. Results log (`RESULTS.md`)
- [x] 14. Loud failures: every script names its step and prints a FAILED banner that shows even with `2>/dev/null` (`common/errors.py`); pre-flight checks for Kafka, baseline and HBase

## Check and run

**Next (13 November), in the container, in this order:**

- [x] Run 6a: 1,000-meter run with HBase on the new code: 27 of 30 (3 water stuck spells under 24 hours, by design), no false alerts, no alerts on 20 empty premises, no late readings dropped
- [x] Replay-speed test on Kafka: leak, stuck, spike, negative wait 0 intervals; silent 3 (45 min real time)
- [x] Micro-batch experiment: leak 15.1 s, stuck 19.2 s per cycle; re-reads 4 and 24 intervals per cycle
- [x] Run 6: full size on regenerated data (empty premises, late readings): 290 of 300 (the 10 water stuck spells under 24 hours, by design), no false alerts, no alerts on 226 empty premises, no late readings dropped
- [ ] Optional: zones from the landing zone after run 6 (`build_zones.py --source landing`), compare counts, then rebuild from raw (`build_zones.py`).
- [x] Warehouse and reports (`build_warehouse.py`, `hive/03_warehouse.sql`, `hive/04_reports.sql`), screenshots for 5.3.5
- [x] Benchmark 2 again on the run 6 data (benchmark 4): all tools found the same 1,370 leak nights; streaming 1.9 s
- [x] Restart test in the container: run B killed with kill -9 mid-stream and restarted; same 29 of 29 detected, same 208 alert windows as run A, landing zone 191,824 readings, 0 missing, 0 extra

Done before:

- [x] First live run of the streaming job with Kafka in the container (1,000 meters: 30 of 30 anomalies detected, no false alerts)
- [x] Zones built in the container (671,788 cleansed, 4 quarantined, 7,000 daily rows); HBase started and tables created
- [x] Full pipeline with HBase (run 3): 30 of 30 detected, no false alerts, 30 rows in HBase
- [x] Re-ran the evaluation on the run 3 alerts with the first alert per anomaly (RESULTS.md, correction section)
- [x] First real run of `scripts/run_stream.sh` with latency measured to `emitted_ts` (run 4: 30 of 30, no false alerts)
- [ ] Test-build the `Dockerfile` (on Maria's Mac) and run `SETUP.md` start to finish, so Windows users can follow it
- [x] Replace `OWNER` in the clone commands in `SETUP.md` with the GitHub account name
- [x] Calibrate consumption levels in the generator against DEWA's Annual Statistics Booklet 2025 (all four yearly averages within 5%)
- [x] Sensitivity test run in the container (`evaluation/sensitivity.py`): detection limit about 0.5 gallons per 15 mins at the current threshold
- [x] Benchmark in the container on the 1,000-meter data (benchmark 1: all four tools found the same 19 leak nights; Hive 33 s, MapReduce 15 s, Spark local 4.2 s, streaming alert in 6.5 s)
- [x] Spark on YARN added to the benchmark for a like-for-like comparison (first run with the full-size data)
- [x] Full-size run (run 5): 10,000 meters, 30 days, 300 of 300 detected, no false alerts; benchmark 2 at that size
- [x] Benchmark 3, batch tools on 30,000 meters (6.5 GB): all agree; cost per GB measured for each tool
- [x] Screenshot of the benchmark 3 table (`5-5_benchmark_3x.png`)

## Evidence for the report

Screenshots go in `screenshots/mac/` (Mac) or `screenshots/other/` (any other setup), named by report section (e.g. `5-3-4_hdfs_raw_zone_load.png`). Screenshots from before the 13 November fix are in `screenshots/mac/superseded/`; RESULTS.md still refers to them for the older runs.

- [x] 5.1 Environment check (`check_tools.sh` output)
- [x] 5.3.4 HDFS raw zone listing and Hive partitions
- [x] 5.3.4 Hive readings per day and anomalies per type
- [x] 5.3.1 Replay sending readings into Kafka, the replay finishing, and the alerts topic in a console consumer
- [x] 5.3.2 Streaming job printing alerts as they are raised (start, negative and silent, leak, stuck)
- [x] 5.3.3 HBase: open alerts, one meter's newest readings, one alert's history
- [x] 5.3.4 Zone build summary and Hive partitions for the cleansed and quarantine zones
- [x] 5.3.4 `hdfs dfs -ls` of all zones, including landing
- [x] 5.3.4 Hive leak query on `daily_meter_usage` (`5-3-4_hive_leak_query_full_size.png`)
- [x] 5.3.3 Clean HBase screenshots (`5-3-3_hbase_queries_clean_1.png`, `_2.png`)
- [x] 6 / 7.1 Sensitivity table (`7-1_sensitivity_test.png`)
- [x] 5.3.4 Zone sizes at full size (`5-3-4_zone_sizes_full_size.png`)
- [x] 5.3.2 Full-size leak alerts (`5-3-2_full_size_leak_alerts.png`)
- [x] 5.3.5 Star schema: table counts and the six reports in Hive (`5-3-5_warehouse_counts_rollup_cube.png`, `5-3-5_reports_grouping_sets_billing.png`, `5-3-5_reports_rank_and_missing.png`)
- [x] 5.5 Replay-speed test table, micro-batch table
- [x] 7.1 Evaluation table, run 6 (`7-1_evaluation_run6.png`); run 6 data, raw zone, leak alerts and zone sizes (`5-3-4_generate_run6.png`, `5-3-4_raw_zone_run6.png`, `5-3-2_run6_leak_alerts.png`, `5-3-4_zone_sizes_run6.png`)
- [x] 5.5 Benchmark table, 1,000 meters (`5-5_benchmark_1000_meters.png`)
- [x] 5.5 Benchmark table, full-size data (`5-5_benchmark_run6.png`, benchmark 4; the run 5 one is in `superseded/`)
- [x] 7.1 Evaluation table, 1,000-meter run (`7-1_evaluation_results_1000_meters.png`)
- [x] 7.1 Evaluation table, full-size run (`7-1_evaluation_full_size.png`)
