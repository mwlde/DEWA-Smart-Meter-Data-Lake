# Results

Every figure in the report, with the conditions it came from. All runs were made on a MacBook Air (Apple Silicon) in one Docker container (10 CPUs and 12 GB of memory for Docker, 4 GB for YARN containers), with Hadoop 3.3.6, Hive 3.1.3, Spark 3.5.6, Kafka 3.9.1 and HBase 2.5.10 on Java 8 (environment check: `5-1_environment_check_tools.png`). Screenshots are in `screenshots/mac/`.

**Final results** are the runs on the final code, and the batch-only tests that the final code changes do not affect. **How we got here** lists the earlier runs and the corrections that led to the final code; their full entries and screenshots are in the git history.

| Result | Report section |
| --- | --- |
| Full-size run | 5.3, 6, 7.1 |
| 1,000-meter run with HBase | 5.3.3, 5.4 |
| Benchmark: the leak check in five tools, full size | 4.3, 5.5 |
| Benchmark on three times the data | 5.5 |
| Replay-speed test | 5.5 |
| Micro-batch experiment: MapReduce every 15 minutes | 4.3, 5.5 |
| Restart test | 5.3.4, 6 |
| Sensitivity test | 6 |

## Final results

### Full-size run

**The headline figures for the report.**

| | |
| --- | --- |
| Machine | MacBook Air (Apple Silicon), one Docker container, HBase off |
| Data | 10,000 meters, 30 days (1 to 30 July), 28,799,091 readings, seed 42, calibrated generator, 226 empty premises (`--vacant-rate 0.05`); raw zone 2.2 GB |
| Live stream | Days 2 to 30: 27,839,091 readings at one interval per second (10,000 readings a second, about 46 minutes); 1% of readings 15 or 30 minutes late |
| Detection job | Final code: leak and stuck in update mode, 30-minute watermark, water stuck needs 24 hours of zero in an occupied premise |
| Anomalies | 300: 91 leak, 42 negative, 35 silent, 75 spike, 57 stuck (38 electricity, 19 water) |

**Detection** (`evaluation/evaluate.py`, first alert per anomaly, latency to `emitted_ts`)

| Type | Injected | Detected | Detection | False alerts | Waits for later readings | Median delay, simulated | Median latency, real | Median real time from start to alert |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| leak | 91 | 91 | 100% | 0 | no | 525 min | 1.92 s | 525 min |
| negative | 42 | 42 | 100% | 0 | no | 0 min | 2.01 s | 0 min |
| silent | 35 | 35 | 100% | 0 | yes | 15 min | 8.47 s | 45 min |
| spike | 75 | 75 | 100% | 0 | no | 0 min | 2.06 s | 0 min |
| stuck | 57 | 47 | 82.5% | 0 | no | 390 min | 2.60 s | 390 min |
| **All** | **300** | **290** | **96.7%** | **0** | | | | |

**Latency over every alert record**: leak 12,289 records, median 2.69 s; negative 42, 2.01 s; silent 210, 8.42 s; spike 75, 2.06 s; stuck 944, 2.52 s.

- **The 10 missed anomalies are exactly the 10 water meters stuck for less than 24 hours** (of 19 water stuck spells; the other 9, all over a day, were caught). All 38 electricity stuck meters were caught. This is the water rule's design: a water meter reading zero is normal in an empty home, so it needs a full day of zero while the premise's electricity shows it is occupied.
- **Empty premises**: 226 in the data, no alert raised on any of them while empty.
- **Late readings**: none dropped (`Readings dropped as later than the watermark (30 minutes): leak 0, stuck 0, stuck_water 0, silent 0, point 0`); 988 alert batches; Spark's error log clean.
- 13,560 alert records, 290 incidents (one per meter and type), 6 repeated records for the same window (late readings changing a window that had already fired; the alerts zone keeps both, HBase would overwrite).
- **First leak alert of the run** (`grep -m 1 -A 14 "\[leak\]" logs/stream.log`): W0007004 in Jebel Ali, window 01:30 to 02:30 on 2 July, raised in the micro-batch that started 2.0 s after the window's last reading arrived (09:35:42.024 to 09:35:44.014); the next window fired 0.9 s after its last reading. On the earlier code the same alert took 4.1 s, two replay intervals of waiting.
- **Against the earlier full-size run** (old code): leak 5.77 to 1.92 s and stuck 6.55 to 2.60 s, because they no longer wait two intervals (30 minutes in real time); silent 7.05 to 8.47 s, now waiting for a 30-minute watermark. Detection 300 to 290 of 300 (the short water stuck spells, by design), with 226 empty premises and late readings added.

**Storage per zone** (`hdfs dfs -du -h /user/hadoop/dewa`, after the run):

| Zone | Size | Note |
| --- | --- | --- |
| Raw (CSV) | 2.2 GB | as received |
| Landing (Parquet) | 536.8 MB | every streamed reading |
| Cleansed (Parquet) | 338.1 MB | 6.5 times smaller than the raw CSV |
| Checkpoints | 752.1 MB | up from 443.1 MB on the earlier code: one more query, and 24-hour windows for the water stuck check |
| Alerts | 5.4 MB | 13,560 alerts |
| Curated | 5.0 MB | daily usage, meters, baseline |
| Warehouse | 3.9 MB | star schema |
| Quarantine | 59.8 KB | the 42 injected negative readings |

**Warehouse** (`batch/build_warehouse.py`, `hive/03_warehouse.sql`): `fact_daily_usage` 300,000 rows, `fact_alert` 1,615, `dim_meter` 10,000, `dim_date` 30, `dim_alert_type` 5.

**Reports** (`hive/04_reports.sql`, all six ran in Hive):

- *Roll-up*: 252,799.1 m3 of water in July, from 23,172.8 (Al Barsha) to 26,155.1 m3 (Mirdif) per area, with the month subtotal and grand total rows.
- *Cube*: average daily use per premise 370.7 IG; apartment 155.8, villa 619.1, commercial 791.9 IG (consistent with the calibration with July's seasonal factor); weekday and weekend almost equal (370.8 and 370.5 IG), because the generator has no weekly pattern (a real utility's data would show one).
- *Grouping sets*: 1,615 alert meter-days and 13,554 alert windows in total; leak 1,370 meter-days (exactly the 1,370 leaking meter-nights every tool found in the benchmark), 12,289 windows; negative 42; silent 44 meter-days; spike 75; stuck 84 meter-days, 938 windows. Per area, for example Al Nahda 205 leak meter-days.
- *Billing extract*: one row per premise for July, water in m3 and electricity in kWh side by side (for example P0000000, villa, Al Barsha: 102.19 m3, 5,851.3 kWh, no missing readings).
- *Window function*: the three biggest water users in each area, nearly all commercial (150 to 160 m3 in the month); the outliers, Jebel Ali P0003190 (189.7 m3) and P0000743 (173.9 m3) and the villa P0001737 in Al Quoz (157.3 m3), stand out from their peers.
- *Dice*: missing readings on weekdays by area; at most 0.013% (International City electricity, 133 readings).

**Screenshots** (in `screenshots/mac/`): `5-3-4_generate_full_size.png`, `5-3-4_raw_zone_full_size.png`, `5-3-2_leak_alerts_full_size.png`, `7-1_evaluation_full_size.png`, `5-3-5_warehouse_counts_rollup_cube.png`, `5-3-5_reports_grouping_sets_billing.png`, `5-3-5_reports_rank_and_missing.png`, `5-3-4_zone_sizes_full_size.png`. The zone build (`5-3-4_zone_build_full_size.png`: 28,799,049 cleansed, 42 quarantined, 300,000 daily usage rows) and the Hive leak query on the curated zone (`5-3-4_hive_leak_query_full_size.png`) were taken on the earlier full-size data; the reading counts, water readings and anomalies are identical in the final data.

### 1,000-meter run with HBase

The figures for the 1,000-meter dataset. MacBook Air, one Docker container, HBase on, `bash scripts/run_stream.sh`, with the final code: leak and stuck in update mode, 30-minute watermark, water stuck needs 24 hours of zero in an occupied premise.

| | |
| --- | --- |
| Data | 1,000 meters, 7 days, 671,792 readings, seed 42, calibrated generator, 20 empty premises (`--vacant-rate 0.05`) |
| Live stream | Days 2 to 7: 575,792 readings, one interval per second; 5,732 sent 15 or 30 minutes late (1%) |
| Anomalies | 30: 8 leak, 4 negative, 6 silent, 4 spike, 8 stuck (5 electricity, 3 water) |
| Zones | 671,792 raw, 671,788 cleansed, 4 quarantined (the 4 injected negatives) |

**Detection** (`evaluation/evaluate.py`, first alert per anomaly, latency to `emitted_ts`)

| Type | Injected | Detected | Detection | False alerts | Waits for later readings | Median delay, simulated | Median latency, real | Median real time from start to alert |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | no | 735 min | 1.99 s | 735 min |
| negative | 4 | 4 | 100% | 0 | no | 0 min | 2.03 s | 0 min |
| silent | 6 | 6 | 100% | 0 | yes | 15 min | 8.44 s | 45 min |
| spike | 4 | 4 | 100% | 0 | no | 0 min | 2.01 s | 0 min |
| stuck | 8 | 5 | 62.5% | 0 | no | 390 min | 2.20 s | 390 min |
| **All** | **30** | **27** | **90%** | **0** | | | | |

- **The 3 missed anomalies are water meters stuck for less than a day**, which the water rule no longer catches by design (it needs 24 hours of zero so that empty homes are not flagged): W0000886 for 11.25 hours, W0000376 for 6.25 hours, W0000564 for 12.75 hours. The one water spell longer than a day (W0000286, 39 hours) was caught, and all 5 electricity stuck meters.
- **Empty premises**: 20 in the data, no alerts raised on them while empty.
- **Late readings**: none of the 5,732 dropped (`Readings dropped as later than the watermark (30 minutes): leak 0, stuck 0, stuck_water 0, silent 0, point 0`).
- 317 alert records, 27 incidents (one per meter and type), 1 repeated record for the same window, 129 alert batches.
- **Against the earlier code** (same data apart from the empty premises): leak 6.46 to 1.99 s and stuck 6.02 to 2.20 s, because they no longer wait two intervals; silent 6.50 to 8.44 s, because it now waits for a 30-minute watermark instead of 15. In real time, leak and stuck now alert within seconds of the revealing reading; silent 45 minutes after the first missing reading.

**HBase** (`bash scripts/hbase_queries.sh`): `meter_alerts` holds 27 rows, one per incident (count in 0.06 s); a 10-row scan of open alerts in 0.02 s; meter W0000108's 4 newest readings newest first in 0.01 s (7.4 to 7.9 gallons per 15 minutes late at night: the leak). The alert history now uses the window start as each version, so `get 'meter_alerts', 'W0000108#leak'` lists the windows 6 July 03:00 and 7 July 01:00 to 03:00 by their own times.

**Screenshots** (in `screenshots/mac/`): `5-3-4_zone_build_1000_meters.png`, `5-3-2_stream_start_1000_meters.png`, `7-1_evaluation_1000_meters.png`, `5-3-3_hbase_open_alerts.png`, `5-3-3_hbase_readings_and_history.png`.

### Benchmark: the leak check in five tools, full size

On the final full-size data (the full-size run's dataset: 10,000 meters, 30 days, 2.2 GB, with 226 empty premises), with the streaming row from that run. `bash scripts/run_benchmark.sh`, three runs of each batch tool, one Docker container.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 30.8 s | 28.3 to 33.5 s | 1370 | 1370 | 0 | 0 | about 2 h + 31 s | 36 |
| Hive (HiveQL on MapReduce) | 45.3 s | 43.6 to 45.4 s | 1370 | 1370 | 0 | 0 | about 2 h + 45 s | 16 |
| Spark batch (DataFrames, local mode) | 9.8 s | 9.3 to 10.0 s | 1370 | 1370 | 0 | 0 | about 2 h + 10 s | 39 |
| Spark batch (DataFrames, on YARN) | 25.2 s | 24.2 to 25.5 s | 1370 | 1370 | 0 | 0 | about 2 h + 25 s | 39 |
| Spark Structured Streaming | continuous | - | 1370 | 1370 | 0 | 0 | 1.9 s | 280 (all 5 checks) |

- Every approach found the same 1,370 leaking meter-nights the ground truth expects (91 injected leaks), none missed and none extra, as on the earlier data. The empty premises did not add any.
- Against the same benchmark on the earlier full-size data: MapReduce 30.3 to 30.8 s, Hive 44.6 to 45.3 s, Spark on YARN 22.2 to 25.2 s, Spark local 9.9 to 9.8 s. Run-to-run noise on one laptop; the ranking is the same.
- Streaming: 5.8 to 1.9 s, because leak no longer waits two replay intervals (update mode). 237 to 280 lines: the water stuck rule, the late-readings listener and the restart-safe alert writes.
- Screenshot: `5-5_benchmark_full_size.png`.

### Benchmark on three times the data, batch tools only

`bash scripts/run_benchmark.sh --no-streaming` on 30,000 meters for 30 days (86,396,079 readings, 6.5 GB raw CSV, calibrated generator, 868 anomalies of which 237 leaks). Docker had 10 CPUs and 12 GB of memory; Kafka and HBase were not running. The stream was not run on this data, so there is no streaming row. Batch tools only, so the final code changes to the streaming job do not affect it, and the generator fix changed only electricity readings.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 61.4 s | 60.7 to 62.5 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 61 s | 36 |
| Hive (HiveQL on MapReduce) | 71.4 s | 71.2 to 75.1 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 71 s | 16 |
| Spark batch (DataFrames, local mode) | 18.5 s | 17.4 to 20.2 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 19 s | 39 |
| Spark batch (DataFrames, on YARN) | 42.8 s | 42.3 to 43.6 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 43 s | 39 |

**How the tools scale** (54 MB and 2.2 GB from the earlier benchmarks on 1,000 meters and at full size, 6.5 GB here; a straight line fits each tool closely):

| Approach | Fixed start-up | Extra time per GB | Runtime at 54 MB / 2.2 GB / 6.5 GB |
| --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | about 15 s | about 7.2 s | 15.1 / 30.3 / 61.4 s |
| Hive (HiveQL on MapReduce) | about 32 s | about 6.0 s | 33.0 / 44.6 / 71.4 s |
| Spark on YARN | about 12 s | about 4.8 s | - / 22.2 / 42.8 s (two sizes) |
| Spark local mode | about 4.5 s | about 2.2 s | 4.2 / 9.9 / 18.5 s |

**What it shows**

- **All four agree exactly** again: 3,366 leaking meter-nights from 237 injected leaks.
- **Two costs, not one.** Each tool has a fixed start-up cost and a cost per gigabyte. Hive has the highest start-up (about 32 s, compiling the query and starting several MapReduce jobs) but processes each extra gigabyte slightly faster than our MapReduce job, which pipes every line through a Python mapper and reducer (Hadoop Streaming) rather than running compiled Java. Spark on YARN has a lower start-up than either and the lowest cost per gigabyte of the three YARN tools.
- **Like for like on YARN at 6.5 GB**: Spark 42.8 s, MapReduce 61.4 s (Spark 30% faster), Hive 71.4 s. The gap grows slowly with the data: Spark's lead over MapReduce went from 27% at 2.2 GB to 30% at 6.5 GB.
- **Why Spark local mode is fastest**: it uses all 10 CPUs directly, while YARN in this container is set to 4 GB of memory for containers, so MapReduce, Hive and Spark on YARN run only a few tasks at once. On a real cluster YARN would have many machines' worth of resources; the local-mode figure shows what this one machine can do without that limit, not a fair comparison with the YARN tools.
- **These are single-machine figures.** They show how the tools behave as the data grows here; they do not predict runtimes on DEWA's data on a cluster, and the report should not extrapolate them that way.
- **Time to alert is unchanged**: whatever the tool, a nightly batch reports a leak about 2 hours after it shows. The streaming job, on the final code, alerts 1.9 seconds after the revealing reading (benchmark above).

**Screenshot** (in `screenshots/mac/`): `5-5_benchmark_3x.png`.

### Replay-speed test

`bash scripts/speed_test.sh`: 1,000 meters, 3 days (2 live days, 191,824 readings, 1,906 late), 29 anomalies, 22 empty premises, HBase off, the same data at 1 and at 4 seconds per interval.

Both runs: 29 of 29 detected, no false alerts, no alerts on the empty premises, no late readings dropped (2 and 3 repeated records for one window).

| Check | Alert records | Latency at 1 s per interval | Latency at 4 s per interval | Change | Intervals waited | Real time from anomaly start to alert (median) |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 99 | 2.25 s | 2.20 s | -0.05 s | 0.0 | 930 min |
| negative | 3 | 2.07 s | 1.59 s | -0.48 s | -0.2 | 0 min |
| silent | 36 | 8.10 s | 16.96 s | +8.86 s | 3.0 | 45 min |
| spike | 3 | 2.99 s | 3.83 s | +0.84 s | 0.3 | 0 min |
| stuck | 69 | 2.05 s | 3.03 s | +0.98 s | 0.3 | 1,440 min |

Latency is the median over every alert record (intervals waited = change / 3 extra seconds per interval). Leak, stuck, spike and negative waited no interval: they fire on the reading that reveals the anomaly, so their replay latency (about 2 to 4 seconds) is their real-world latency. Silent waited 3 intervals, as designed: 45 minutes after the first missing reading in real time. The last column is set by the rules: a leak shows only at night (in this 2-day stream, mostly the night after the next), a stuck water meter only after 24 hours.

First-alert medians (11 leaks, 6 stuck): fast run leak 2.25, negative 2.07, silent 8.19, spike 2.99, stuck 2.62 s; slow run leak 2.70, negative 1.59, silent 19.95, spike 3.83, stuck 3.01 s.

**Screenshots** (in `screenshots/mac/`): `5-5_speed_test_fast.png`, `5-5_speed_test_slow_and_comparison.png`.

### Micro-batch experiment: MapReduce every 15 minutes

`bash scripts/run_microbatch.sh` on the same 1,000-meter, 3-day data: 8 cycles (data up to 01:45 to 03:30 on 3 July), each cycle running the leak check on the last 4 interval files and the electricity stuck check on the last 24 as Hadoop Streaming jobs on YARN.

| Check | Runtime per cycle (median) | Range | Intervals read per cycle | New intervals per cycle | Data read per cycle |
| --- | --- | --- | --- | --- | --- |
| leak | 15.1 s | 11.1 to 15.2 s | 4 | 1 | 0.3 MB |
| stuck (electricity, 6 hours) | 19.2 s | 19.1 to 24.6 s | 24 | 1 | 1.9 MB |

- The leak job found the same 11 leaking meters every cycle; no electricity meter was stuck for a full 6 hours in that window, so the stuck job found none (correct).
- **MapReduce can meet R2 this way**: triggered as each interval lands, an alert would come 15 to 20 seconds later, inside 5 minutes. Run nightly it is about 2 hours.
- **What rules it out is cost**: about 34 seconds of YARN jobs per cycle for two checks, almost all of it job start-up (a cycle reads under 2 MB); a new job per check every 15 minutes, 480 a day for five checks; and no state between runs, so each reading is read 4 times by the leak check, 24 times by the 6-hour stuck check and would be 96 times by the 24-hour water rule. At DEWA's scale (2.44 million readings an interval, about 80 bytes each as CSV) a 6-hour window is about 4.7 GB read every 15 minutes to add 0.2 GB. The streaming job reads each reading once, keeps its windows in memory and alerts in about 2 seconds.
- Single-machine figures: they show the pattern (start-up per job, re-reading per cycle), not runtimes on a cluster.

**Screenshot** (in `screenshots/mac/`): `5-5_microbatch_mapreduce.png`.

### Restart test

`bash scripts/restart_test.sh`, one Docker container, Kafka on, HBase off. Its own data (1,000 meters, 3 days, seed 42, 22 empty premises, 29 anomalies in the live days) in a separate lake folder, `/user/hadoop/dewa_restart_test`. Both runs replay the 2 live days (191,824 readings, 1,906 of them late) at one interval per second.

- **Run A**, without interruption: 86 alert batches, no reading dropped by the watermark.
- **Run B**: the detection job killed with `kill -9` at 16:11:37, 110 seconds into the replay, when the replay had sent 03:00 on 3 July (108,820 readings), in the middle of that night's leak windows; 31 micro-batches with alerts before the kill. Restarted from its checkpoint at 16:11:40 and ready at 16:11:46 (down for 9 seconds), then caught up on the readings that had reached Kafka meanwhile. 80 alert batches, no reading dropped.

| Check | Injected | Detected A / B | False alerts A / B | Alert windows A / B | Only in A / only in B | Median first-alert latency A / B | Median latency, every record, A / B |
| --- | --- | --- | --- | --- | --- | --- | --- |
| leak | 11 | 11 / 11 | 0 / 0 | 99 / 99 | 0 / 0 | 1.07 / 1.98 s | 1.50 / 11.62 s |
| negative | 3 | 3 / 3 | 0 / 0 | 3 / 3 | 0 / 0 | 2.65 / 1.02 s | 2.65 / 1.02 s |
| silent | 6 | 6 / 6 | 0 / 0 | 36 / 36 | 0 / 0 | 8.18 / 8.49 s | 8.02 / 8.51 s |
| spike | 3 | 3 / 3 | 0 / 0 | 3 / 3 | 0 / 0 | 1.08 / 1.92 s | 1.08 / 1.92 s |
| stuck | 6 | 6 / 6 | 0 / 0 | 67 / 67 | 0 / 0 | 1.97 / 2.69 s | 2.13 / 2.26 s |

- **The restarted run raised exactly the same 208 alert windows as the uninterrupted run**: none missing, none extra. Alert records A 211, B 210; repeated records for the same window A 3, B 2 (late readings updating a window that had already fired, which depends on micro-batch timing, not on the restart).
- **Landing zone against the raw zone**: 191,824 readings on the streamed days; both runs stored 191,824, 0 missing, 0 extra.
- Empty premises: 22, no alert on them in either run.
- Latency: the median over all leak alert records went from 1.5 s in run A to 11.6 s in run B, because the kill fell during that night's leak windows: the windows completed while the job was down were raised when it caught up. Nothing was lost.
- Screenshots: `6_restart_test_kill_restart.png`, `6_restart_test_comparison.png`, `6_restart_test_run_a_evaluation.png`, `6_restart_test_run_b_evaluation.png`, and `5-3-2_kafka_alerts_topic.png` (run B's first alerts on the Kafka topic `alerts`: six silent alerts, each about 8 seconds after its last reading, since silent waits for the watermark).

### Sensitivity test: smallest detectable leak

`spark-submit evaluation/sensitivity.py`: six datasets of 2,000 meters for 7 days (calibrated generator, seed 42, anomaly rate 0.3), identical except for the leak size, 156 leaks each. The leak rule (the one in `benchmark/spark_batch.py`, identical in results to the streaming job) was run at four thresholds. Run in the container (screenshot `7-1_sensitivity_test.png`). It tests the leak rule alone, which the final code changes did not alter.

Share of leaks detected (in brackets: share of all nights a leak was running that were flagged):

| Leak size (gallons per 15 min) | Threshold 0.2 | Threshold 0.3 | **Threshold 0.5 (pipeline)** | Threshold 1.0 |
| --- | --- | --- | --- | --- |
| 0.1 to 0.25 | 47% (34%) | 9% (3%) | 1% (0%) | 0% (0%) |
| 0.25 to 0.5 | 100% (100%) | 87% (79%) | 13% (7%) | 0% (0%) |
| 0.5 to 0.75 | 100% (100%) | 100% (100%) | 99% (98%) | 1% (0%) |
| 0.75 to 1 | 100% (100%) | 100% (100%) | 100% (100%) | 15% (9%) |
| 1 to 2 | 100% (100%) | 100% (100%) | 100% (100%) | 100% (99%) |
| 1 to 6 (normal runs) | 100% (100%) | 100% (100%) | 100% (100%) | 100% (100%) |
| **False alerts** (meter-nights, out of 7,000 water meter-nights) | **33** | 0 | 0 | 0 |

**What it shows**

- **The detection limit equals the threshold.** At the pipeline's 0.5 gallons per 15 minutes, leaks of 0.5 and above are caught almost every night; below it, detection collapses (13% for 0.25 to 0.5, 1% below 0.25). A leak of 0.5 gallons per 15 minutes is 48 gallons (about 218 litres) a day, around a fifth of an average Dubai home's use (258 gallons a day, DEWA 2025). Slower drips are invisible to this method at this threshold.
- **The trade-off.** Lowering the threshold to 0.3 catches most 0.25 to 0.5 leaks (87%) with no false alerts on this data; 0.2 catches even smaller ones but starts flagging healthy meters (33 meter-nights, about 0.5%), mostly villas and commercial premises whose normal night use is highest.
- **Why the pipeline keeps 0.5.** The clean result at 0.3 is partly because the same team wrote the generator: real night use is noisier (irrigation, pool pumps, night-shift businesses). Choosing 0.3 from this table would fit the rule to our own synthetic data. In a real deployment the threshold would be set per meter from its own night history (future work), not one value for all.
- Normal-size leaks (1 to 6 gallons, all the main runs) are caught at every threshold up to 1.0, so the main results do not depend on the exact threshold.

## How we got here

The runs and fixes before the final code, newest last. Each was replaced by a result above or still applies to it, as the last column says.

| What | Result | Status |
| --- | --- | --- |
| Raw zone in HDFS and Hive (1,000 meters, 7 days) | Raw zone 51.7 MB. Hive took 31.6 s and 16.2 s for two simple counts on 671,792 readings, because each query starts MapReduce jobs on YARN | Still valid: the Hive start-up cost in report section 7.3 (`5-3-4_hive_mapreduce_jobs.png`, `5-3-4_hive_readings_and_anomalies.png`) |
| First live run through Kafka (1,000 meters, 7 days, 30 anomalies) | 30 of 30 detected, no false alerts; 348 alert messages for 30 anomalies, the repeats from sliding windows | Replaced by the 1,000-meter run |
| Lake zones and HBase in the container | 671,788 readings cleansed, 4 quarantined (the 4 injected negatives); both HBase tables created | Replaced by the 1,000-meter run |
| Full pipeline with HBase | 30 of 30; HBase held 30 open-alert rows for 30 anomalies, and a prefix scan returned one meter's newest readings in 0.02 s; writing to HBase added little latency | Replaced by the 1,000-meter run |
| Latency measure corrected | The evaluation had taken each anomaly's fastest alert, measured to the start of its micro-batch. It now takes the first alert, measured to when it was computed (`emitted_ts`) | Applies to every result above |
| First one-command run (`run_stream.sh`) | 30 of 30; window checks about 6 s in the replay | Replaced: the 6 s hid a wait (latency fix below) |
| Generator calibrated to DEWA's 2025 statistics | Yearly use per account within 5% of DEWA's figures (README, "Calibration") | Applies to all data above |
| Benchmarks on 1,000 meters and at full size | Every tool found the same leak nights (19 and 1,370); streaming alerted in 6.5 and 5.8 s | Replaced by the full-size benchmark; their batch runtimes are in the scaling table |
| First full-size run (10,000 meters, 30 days) | 300 of 300 detected, no false alerts; window checks 6 to 7 s in the replay; checkpoints 443.1 MB | Replaced by the full-size run |
| Generator fix: one type and area per premise | A premise's electricity meter now takes its water meter's premise type and area. Water readings and anomalies are unchanged | Applies to all data above |
| First replay-speed test | Too noisy: it compared first alerts only (11 leaks, 6 stuck meters), which move by a second or two by chance | Redone as the replay-speed test |
| Latency fix, empty homes and late readings, restart-safe writes | Window checks had waited for two later intervals: a few seconds in the replay, about 30 minutes in real time. Leak and stuck moved to update mode; empty premises and late readings added; alert writes made safe to repeat | The final code behind every result above |
| First attempt on the final code | Old day folders left in `data/` mixed two datasets | Not used; fixed in the generator and `run_stream.sh` |

## Limits of these results

- **Best-case figures.** The same team wrote the generator and the detection rules, so the rules suit the data. Real meters are noisier, and real leaks can be smaller than the 1 to 6 gallons per 15 minutes simulated here (see the sensitivity test).
- **One machine.** The largest run is 10,000 meters on one laptop. DEWA had about 2.44 million electricity and water accounts at the end of 2025, about 240 times more. The runs show the design works and keeps up at 10,000 readings a second; throughput at DEWA's scale needs a cluster and is not measured here.
- **Anomaly count.** 300 anomalies in the full-size run, 35 to 91 of each type, so detection rates are estimates, not guarantees.
- **Window alignment.** The 6-hour electricity stuck windows start on the hour, so a spell shorter than 6 hours 45 minutes can be missed, depending on when it starts.
- **HBase not used at full size.** Writing 10,000 readings a second through one REST server from one laptop would measure that bottleneck, not the pipeline. HBase is shown in the 1,000-meter run.
