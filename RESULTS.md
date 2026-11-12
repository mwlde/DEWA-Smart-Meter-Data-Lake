# Results

Every measured result, with the conditions it came from. Report sections 5.5 and 7.1 take their figures from here. Add a new run at the top; keep the old ones. Screenshots are in `screenshots/mac/`.

## Benchmark 3: batch tools on three times the data (12 November 2026)

`bash scripts/run_benchmark.sh --no-streaming` on 30,000 meters for 30 days (86,396,079 readings, 6.5 GB raw CSV, calibrated generator, 868 anomalies of which 237 leaks). Docker had 10 CPUs and 12 GB of memory; Kafka and HBase were not running. The stream was not run on this data, so there is no streaming row.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 61.4 s | 60.7 to 62.5 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 61 s | 36 |
| Hive (HiveQL on MapReduce) | 71.4 s | 71.2 to 75.1 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 71 s | 16 |
| Spark batch (DataFrames, local mode) | 18.5 s | 17.4 to 20.2 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 19 s | 39 |
| Spark batch (DataFrames, on YARN) | 42.8 s | 42.3 to 43.6 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 43 s | 39 |

**How the tools scale** (all three benchmarks: 54 MB, 2.2 GB and 6.5 GB of raw data; a straight line fits each tool closely):

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
- **Time to alert is unchanged**: whatever the tool, a nightly batch reports a leak about 2 hours after it shows. Streaming (benchmarks 1 and 2) did it in about 6 seconds.

**Screenshot** (in `screenshots/mac/`): `5-5_benchmark_3x.png`.

**Note**: the raw zone now holds this 30,000-meter data; the cleansed, curated and streaming results in HDFS are still from run 5 (10,000 meters). Regenerate with `--meters 10000 --days 30` and reload before running anything that combines them.

## Sensitivity test: smallest detectable leak (12 November 2026)

`spark-submit evaluation/sensitivity.py`: six datasets of 2,000 meters for 7 days (calibrated generator, seed 42, anomaly rate 0.3), identical except for the leak size, 156 leaks each. The leak rule (the one in `benchmark/spark_batch.py`, identical in results to the streaming job) was run at four thresholds. **Run again in the container on 12 November with identical results** (screenshot `7-1_sensitivity_test.png`).

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

## Run 5: full-size run, final results (12 November 2026)

**These are the headline figures for the report.**

**Setup**

| | |
| --- | --- |
| Machine | Maria's MacBook Air (Apple Silicon), one Docker container |
| Data | 10,000 meters (5,000 water, 5,000 electricity), 30 days (1 to 30 July), 28,799,091 readings, calibrated generator (DEWA 2025 statistics), seed 42 |
| History | Day 1, used for the spike baseline (rebuilt automatically: 10,000 meters) |
| Live stream | Days 2 to 30: 27,839,091 readings replayed into Kafka, one 15-minute interval per second (10,000 readings a second), about 46 minutes |
| Detection job | Spark Structured Streaming, micro-batch every 2 seconds, watermark 15 minutes, HBase off (see below) |
| Anomalies injected | 300: 91 leak, 42 negative, 35 silent, 75 spike, 57 stuck |

**Batch layer** (`batch/build_zones.py`): 28,799,091 raw readings, 28,799,049 cleansed, 42 quarantined (exactly the 42 injected negative readings), 0 duplicates, 300,000 curated daily usage rows (10,000 meters x 30 days). Hive registered all 30 daily partitions; the raw zone holds 480,000 readings per meter type per day.

**Detection** (`evaluation/evaluate.py`, latency to `emitted_ts`, first alert per anomaly)

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 91 | 91 | 100% | 0 | 525 min | 5.77 s |
| negative | 42 | 42 | 100% | 0 | 0 min | 1.97 s |
| silent | 35 | 35 | 100% | 0 | 15 min | 7.05 s |
| spike | 75 | 75 | 100% | 0 | 0 min | 2.06 s |
| stuck | 57 | 57 | 100% | 0 | 375 min | 6.55 s |
| **All** | **300** | **300** | **100%** | **0** | | |

- 13,775 alerts raised (repeats from sliding windows), 1,005 alert batches printed.
- **The job kept up with 10 times the load of run 4**: latencies are within about a second of run 4 (1,000 meters), so processing did not fall behind the 10,000 readings a second.
- Every alert was ready within about 7 seconds of the revealing reading reaching Kafka: far under the 5-minute requirement (R2).
- **Stuck 57 of 57** here, but this is partly luck: none of the 57 stuck spells happened to fall in the 6 to 6.75-hour range that the on-the-hour windows can miss (see the scale test below, where 3 of 65 did). The limitation stands and goes in the report.
- **HBase was off for this run.** HBase was shown working in runs 3 and 4 (30 open-alert rows for 30 anomalies, prefix scans in 0.02 s). Writing 10,000 readings a second through its REST server from one laptop would have measured that single-machine bottleneck rather than the pipeline; at DEWA's scale the HBase-Spark connector or bulk loading would write from the cluster's executors instead.

**Storage per zone** (`hdfs dfs -du -h /user/hadoop/dewa`, after the run):

| Zone | Size | Note |
| --- | --- | --- |
| Raw (CSV) | 2.2 GB | the data as received |
| Cleansed (Parquet) | 338.7 MB | the same 28.8 million readings, **6.5 times smaller**: Parquet stores each column together and compresses it |
| Landing (Parquet) | 527.5 MB | every streamed reading; larger than cleansed partly because the stream writes many small files (one set per 2-second micro-batch), which compress less well. Compacting them (for example nightly) is future work |
| Checkpoints | 443.1 MB | the streaming job's offsets and window state, what lets it restart without losing or repeating data |
| Alerts | 6.0 MB | 13,775 alerts |
| Curated | 5.0 MB | 300,000 daily usage rows, dimensions and the baseline |
| Quarantine | 59.7 KB | the 42 rejected readings |

**Hive on the curated zone**: a plain SQL query on `daily_meter_usage` (meters whose lowest night flow stayed above 0.5 gallons) lists the leaking meters from the day after each leak starts, for example W0000504 in Dubai Marina from 3 July at about 3.3 gallons per 15 minutes. The batch layer finds the same leaks the next day; the stream finds them within seconds.

**Screenshots** (in `screenshots/mac/`): `5-3-4_generate_full_size.png`, `5-3-4_zone_build_full_size.png`, `5-3-4_zone_sizes_full_size.png`, `5-3-4_hive_leak_query_full_size.png`, `5-3-2_full_size_run_stream_start.png`, `5-3-2_full_size_leak_alerts.png` (the first leak alerts of the run, W0007004 in Jebel Ali), `7-1_evaluation_full_size.png`.

## Benchmark 2: the leak check in five tools, full size (12 November 2026)

`bash scripts/run_benchmark.sh` on the run 5 data (28,799,091 readings, about 2.3 GB in the raw zone), 3 runs of each tool.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 30.3 s | 28.3 to 31.3 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 30 s | 36 |
| Hive (HiveQL on MapReduce) | 44.6 s | 42.8 to 45.4 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 45 s | 16 |
| Spark batch (DataFrames, local mode) | 9.9 s | 9.4 to 10.7 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 10 s | 37 |
| Spark batch (DataFrames, on YARN) | 22.2 s | 21.5 to 23.5 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 22 s | 37 |
| Spark Structured Streaming | continuous | - | 1,370 | 1,370 | 0 | 0 | 5.8 s | 237 (all 5 checks) |

**What it shows**

- **All five agree exactly** on the 1,370 leaking meter-nights the 91 injected leaks produce.
- Lines of code: the screenshots show 219 for the streaming job; the counter skipped part of that file by mistake (fixed 12 November, checked against a count made with Python's own parser). The correct figure, 237, is in both tables here.
- **Like for like on YARN**: Spark 22.2 s, MapReduce 30.3 s, Hive 44.6 s. Spark is about 27% faster than hand-written MapReduce and twice as fast as Hive. Spark in local mode (9.9 s) shows how much of each figure is YARN job start-up.
- **Scaling** from benchmark 1 (1,000 meters, 7 days) to here (about 43 times the data): MapReduce took 2.0 times as long (15.1 to 30.3 s), Hive 1.35 times (33.0 to 44.6 s), Spark local 2.4 times (4.2 to 9.9 s). Runtimes grew far less than the data, so at 2.3 GB every tool is still dominated by fixed start-up costs. This data is too small to show the gap that large-scale processing would; the report should say so rather than extrapolate.
- **Time to alert decides the question.** Whatever the tool, a nightly batch reports a leak about 2 hours after it first shows; streaming reported it in 5.8 seconds. Batch runtimes of 10 to 45 seconds hardly matter next to the batch schedule. MapReduce and Hive suit the reporting side of the lake (Hive's 16 lines of SQL are the easiest to write and maintain); only the streaming path meets the alerting requirement.

**Screenshot** (in `screenshots/mac/`): `5-5_benchmark_full_size.png`.

## Scale test before the full-size run (12 November 2026)

10,000 meters, 3 days (calibrated generator), streaming job in file mode on a 2-core machine, HBase off.

- **Throughput**: the 2 live days (192 intervals, 1.92 million readings) took about 145 seconds of processing: 0.75 seconds per 15-minute interval. The replay sends one interval per second, so the job keeps up even on 2 cores.
- **Detection**: 300 anomalies, no false alerts. Leak, negative, silent and spike 100%; **stuck 62 of 65 (95.4%)**.
- **Why 3 stuck meters were missed**: the stuck rule uses 6-hour windows that start on the hour. A meter that records zeros for exactly 6 hours but starts at, say, 20:45 never fills a whole on-the-hour window. Detection is certain only for spells of at least 6 hours 45 minutes; the generator makes spells from 6 hours up, and the 3 missed ones lasted 6.0 to 6.25 hours. (The 1,000-meter data had no stuck spell that short.)
- **The fix and its cost**: sliding the stuck windows every 15 minutes instead of every hour caught all 65, but the job then needed about 1.2 seconds per interval on this machine, slower than the replay. The rule was kept as it is for the full-size run; this is a limitation to report (section 6): shorter windows or a finer slide catch more, at the cost of more state and processing.

## Benchmark 1: the leak check in four tools, 1,000 meters (12 November 2026)

`bash scripts/run_benchmark.sh` on the same data as run 4 (1,000 meters, 7 days, 671,792 readings in the raw zone, uncalibrated generator), 3 runs of each tool. Runtime is the wall-clock time of the whole command, start-up included.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 15.1 s | 12.4 to 15.2 s | 19 | 19 | 0 | 0 | about 2 h + 15 s | 36 |
| Hive (HiveQL on MapReduce) | 33.0 s | 32.9 to 34.0 s | 19 | 19 | 0 | 0 | about 2 h + 33 s | 16 |
| Spark batch (DataFrames, local mode) | 4.2 s | 4.2 to 4.4 s | 19 | 19 | 0 | 0 | about 2 h + 4 s | 36 |
| Spark Structured Streaming | continuous | - | 19 | 19 | 0 | 0 | 6.5 s | 237 (all 5 checks) |

**What it shows**

- **All four agree exactly**: each found the 19 leaking meter-nights the 8 injected leaks produce, nothing missed, nothing extra. The differences below are about the tools, not about what they compute.
- **Hive is the slowest** (33 s, about twice MapReduce): it compiles the query into several MapReduce jobs, each started on YARN. It is the shortest to write (16 lines of SQL), which is why it suits reporting rather than alerting.
- **MapReduce** (15 s) pays one YARN job start-up and writes intermediate results to disk between map and reduce; the rule itself takes 36 lines of hand-written mapper and reducer.
- **Spark batch** (4.2 s) was fastest, but it ran in local mode, inside the container without YARN, so part of its lead is the missing YARN start-up. The benchmark now also runs Spark on YARN (from benchmark 2 on) for a like-for-like comparison.
- **Time to alert** is the decisive column for leak detection. A nightly batch can only report a leak after the night window ends (04:00), about 2 hours after the leak first shows (02:00), whatever the tool. Streaming reported it 6.5 seconds after the revealing reading arrived. The tool's runtime (4 to 33 s) hardly matters next to the batch schedule.
- At 1,000 meters every tool finishes in seconds, so runtime differences are mostly start-up overhead. The full-size run (10,000 meters, 30 days, about 43 times the data) repeats the benchmark to show how each tool scales.

**Screenshot** (in `screenshots/mac/`): `5-5_benchmark_1000_meters.png`.

## Generator calibrated to DEWA's 2025 statistics (12 November 2026)

The generator's daily averages now match DEWA's Annual Statistics Booklet 2025 (details and sources in README, "How the synthetic data is generated"). **Runs 1 to 4 used the earlier, uncalibrated averages**: homes used about 25% too little water and commercial premises about 15% too little; electricity was also lower. Anomalies are unchanged (same meters, same times), because they do not depend on the consumption levels.

Checks after the change:

- **Yearly averages** (a 3-day sample of each month, 2,000 meters, no anomalies): home water 252 gallons a day (DEWA 258), commercial water 707 (708), home electricity 55.1 kWh (52.6), commercial electricity 343 kWh (336). All within 5%.
- **Detection** (300 meters, 4 days, 61 anomalies, the local test from run 1): 61 of 61 detected, no false alerts.
- **Night-flow margin** (default 1,000-meter dataset): the highest one-hour minimum night flow of any healthy water meter is 0.055 gallons for apartments, 0.234 for villas and 0.294 for commercial premises, against the 0.5-gallon leak threshold. Healthy meters stay well clear of it.

## Run 4: one-command run with the corrected latency measure (12 November 2026)

Same setup and data as runs 2 and 3, HBase on, started with `bash scripts/run_stream.sh` (first real run of that script). Latency is now measured to when each anomaly's first alert was computed and ready to publish (`emitted_ts`). **These are the figures to quote** for the 1,000-meter dataset (uncalibrated consumption; the full-size run will use the calibrated generator).

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | 735 min | 6.46 s |
| negative | 4 | 4 | 100% | 0 | 0 min | 2.02 s |
| silent | 6 | 6 | 100% | 0 | 15 min | 6.50 s |
| spike | 4 | 4 | 100% | 0 | 0 min | 1.03 s |
| stuck | 8 | 8 | 100% | 0 | 375 min | 6.02 s |
| **All** | **30** | **30** | **100%** | **0** | | |

- 348 alerts raised, 136 alert batches printed, the same as runs 2 and 3: the result repeats exactly.
- Compared with run 3 re-evaluated (first alert, measured to the micro-batch start), every type rose by about 0.2 to 1.7 seconds: the time Spark takes to compute a micro-batch, as predicted in the correction below.
- Point checks (spike, negative) alert in 1 to 2 seconds; window checks (leak, silent, stuck) in about 6 seconds, because they also wait for their window to close. All are far under the 5-minute requirement (R2).

**Screenshot** (in `screenshots/mac/`): `7-1_evaluation_run4_one_command.png`.

## Correction to the latency figures (12 November 2026)

The "median latency, real" figures in runs 2 and 3 below are too low, for two reasons found in the mid-project review:

1. **Fastest alert, not first alert.** `evaluate.py` took the fastest of each anomaly's alerts. A leak raises an alert for every window that shows it, so this picked the quickest repeat and hid slower first alerts. The clearest case: in run 2 the first silent alert took about 26 seconds (Spark was still starting), but the table shows 4.35 s for silent. The script now takes each anomaly's **first** alert.
2. **Measured to the start of the micro-batch.** `alert_ts` is when the micro-batch that raised the alert started, before Spark had computed it. The streaming job now also records `emitted_ts`, when the alert was computed and ready to publish, and latency is measured to that. In a test here, the gap between the two was about 1.3 to 1.7 seconds per alert.

Reason 2 is fixed from run 4 on (old alerts have no `emitted_ts`, so the script falls back to `alert_ts` and says so). Detection rates and false alert counts are not affected.

**Run 3 re-evaluated with the first alert per anomaly** (same alerts, latency still to the micro-batch start, so add roughly 1.5 s for the true time to alert):

| Type | Median latency, fastest alert (old) | Median latency, first alert (corrected) |
| --- | --- | --- |
| leak | 4.35 s | 4.79 s |
| negative | 1.00 s | 1.00 s |
| silent | 4.24 s | 6.26 s |
| spike | 0.56 s | 0.56 s |
| stuck | 5.25 s | 5.36 s |

Silent moved most: one silent anomaly began while Spark was still warming up at the start of the stream, so its first alert was slow. All types are still about 6 seconds or less, far under the 5-minute requirement. Screenshot: `7-1_evaluation_run3_first_alert.png`.

## Run 3: full pipeline with HBase (11 November 2026)

Same setup and data as run 2, with the streaming job also writing to HBase (`--hbase`) and the batch zones built beforehand. This is the first run of every layer together: Kafka, Spark Structured Streaming, HDFS zones, Hive and HBase.

**Detection** (`evaluation/evaluate.py`)

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | 735 min | 4.35 s |
| negative | 4 | 4 | 100% | 0 | 0 min | 1.00 s |
| silent | 6 | 6 | 100% | 0 | 15 min | 4.24 s |
| spike | 4 | 4 | 100% | 0 | 0 min | 0.56 s |
| stuck | 8 | 8 | 100% | 0 | 375 min | 5.25 s |
| **All** | **30** | **30** | **100%** | **0** | | |

348 alerts raised, the same as run 2: the same data gives the same alerts, so the pipeline is repeatable.

**Effect of writing to HBase**: latencies are within about 0.1 seconds of run 2 for four types; stuck rose from 4.48 to 5.25 seconds. Writing every reading and alert to HBase adds little or nothing to the time to alert. Requirement R2 (under 5 minutes) is met by a wide margin either way.

**HBase serving layer** (`bash scripts/hbase_queries.sh`):

- `meter_alerts` holds **30 rows for 30 anomalies**: the 348 repeat alerts from sliding windows collapse to one row per meter and alert type, which is what an operator's "open problems" list needs. The count query took 0.05 seconds.
- A prefix scan on `W0000108#` in `meter_readings` returned that meter's 4 newest readings (23:45 back to 23:00 on 7 July) in 0.02 seconds, newest first, because the row key holds a reversed timestamp. The readings show the leak: about 7 gallons per 15 minutes late at night.
- `get 'meter_alerts', 'W0000108#leak'` with 10 versions shows the leak's history across two nights (6 and 7 July, 01:00 to 03:00). Two limits to state in the report: the table keeps only the 10 newest versions, and alerts written in the same micro-batch become one version. The complete alert history is in the HDFS alerts zone; HBase is for fast lookups, not the archive.

**HDFS** after the run: raw, cleansed, quarantine, curated, landing, alerts and checkpoints folders under `/user/hadoop/dewa`.

**Screenshots** (in `screenshots/mac/`): `5-3-2_streaming_with_hbase_start.png`, `5-3-2_streaming_with_hbase_stopped.png`, `5-3-3_hbase_count_open_alerts.png`, `5-3-3_hbase_open_alerts_scan.png`, `5-3-3_hbase_meter_readings_and_alert_history.png`, `7-1_evaluation_run3_and_hdfs_zones.png`.

## HBase serving layer, clean screenshots (12 November 2026)

`bash scripts/hbase_queries.sh` on the HBase tables still holding the run 4 data (1,000 meters; HBase was stopped before run 5, so they were not cleared). Same results as run 3: 30 open-alert rows for 30 anomalies (count in 0.06 s), a 10-row scan of open alerts in 0.02 s, meter W0000108's 4 newest readings newest-first in 0.02 s (about 7 gallons per 15 minutes late at night: the leak), and that leak's history as 10 versions across three nights (5 to 7 July). Screenshots: `5-3-3_hbase_queries_clean_1.png`, `5-3-3_hbase_queries_clean_2.png`.

## Batch zones and HBase start-up in the container (11 November 2026)

Same 1,000-meter, 7-day dataset as run 2.

**Zone build** (`batch/build_zones.py`):

| | Readings |
| --- | --- |
| Raw | 671,792 |
| Cleansed | 671,788 |
| Quarantined (negative value) | 4 |
| Duplicates removed | 0 |
| Curated daily usage rows | 7,000 (1,000 meters x 7 days) |

The 4 quarantined readings are exactly the 4 injected negative anomalies. Hive registered the cleansed partitions (7 days) and quarantine partitions (the 4 days with a negative reading) with `MSCK REPAIR TABLE`, and the quarantine query in Hive gives the same count.

**HBase**: `start_hbase.sh` started HBase 2.5.10 and the REST server and created both tables; the streaming job's pre-flight check found them and started 6 queries (landing, hbase_readings, leak, stuck, silent, point). Results in run 3 above.

**Screenshots** (in `screenshots/mac/`): `5-3-4_zone_build_summary.png`, `5-3-4_lake_zones_hive_partitions.png`, `5-3-3_hbase_ready_and_streaming_preflight.png`.

## Run 2: first live run through Kafka (11 November 2026)

**Setup**

| | |
| --- | --- |
| Machine | Maria's MacBook Air (Apple Silicon), one Docker container |
| Software | Hadoop 3.3.6, Hive 3.1.3, Spark 3.5.6, Kafka 3.9.1, Java 8 |
| Data | 1,000 meters (500 water, 500 electricity), 7 days, 671,792 readings, seed 42 |
| History | Day 1 (1 July), used for the spike baseline |
| Live stream | Days 2 to 7: 575,792 readings replayed into Kafka (3 partitions), one 15-minute interval per second |
| Detection job | Spark Structured Streaming, micro-batch every 2 seconds, watermark 15 minutes |
| Anomalies injected in the live days | 30: 8 leak, 4 negative, 6 silent, 4 spike, 8 stuck |

**Detection** (`evaluation/evaluate.py`)

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | 735 min | 4.41 s |
| negative | 4 | 4 | 100% | 0 | 0 min | 1.09 s |
| silent | 6 | 6 | 100% | 0 | 15 min | 4.35 s |
| spike | 4 | 4 | 100% | 0 | 0 min | 0.45 s |
| stuck | 8 | 8 | 100% | 0 | 375 min | 4.48 s |
| **All** | **30** | **30** | **100%** | **0** | | |

**How to read the two time columns**

- **Latency, real** is the processing delay: from the moment the reading that reveals the anomaly reached Kafka to the moment the alert was raised (measured to the micro-batch start and from the fastest alert; see the correction at the top). This is requirement R2 (under 5 minutes). Every type was under 5 seconds. Point checks (spike, negative) take under about 1 second; window checks (leak, silent, stuck) take about 4 seconds, because they wait for their window to close (2 replay intervals of watermark plus the 2-second trigger).
- **Delay, simulated** is how long after the anomaly started the data could show it, and depends on the anomaly, not the system. A leak only stands out in the quiet night hours (minimum night flow), so leaks that start during the day are caught on their first night: 735 minutes (about 12 hours) median. "Zero for 6 hours" (stuck) can only be known after 6 hours. Spikes and negative readings are visible in the reading itself.

**Comparison with DEWA's published figure**: DEWA reports that its Hydro Insight system detects anomalies within one hour. Our pipeline raises an alert within seconds of the revealing reading. Both are still bound by when the anomaly becomes visible in the data (a leak at night).

**Other observations**

- The Kafka `alerts` topic received 348 alert messages for 30 anomalies. The extras are repeats: a sliding window raises one alert per position that still shows the problem (a leak shows in every 1-hour window from 01:00 to 03:00, a stuck meter in every 6-hour window). The HBase open-alerts table (TODO item 7) collapses these to one row per meter.
- The very first alerts took about 26 seconds, because Spark was still starting while readings queued in Kafka. Afterwards latency settled at the figures above.
- Silent meters show directly in the replay: terminal 2 sent 999 readings per interval instead of 1,000 while meter E0000899 was silent (2 July, 08:00 to 19:15).

**Screenshots** (in `screenshots/mac/`): `5-3-1_*`, `5-3-2_streaming_start_and_first_alerts.png`, `5-3-2_streaming_negative_and_silent_alerts.png`, `5-3-2_streaming_leak_alerts.png`, `5-3-2_streaming_stuck_alerts.png`, `7-1_evaluation_results_1000_meters.png` (crop the Ctrl+C traceback at the top before using it).

## Raw zone in HDFS and Hive (11 November 2026)

1,000 meters, 7 days, 671,792 readings. The Hive timings come from the first load, made with the first version of the generator (before leaks were weighted up): same size, different anomaly mix. Screenshots: `5-3-4_hdfs_raw_zone_load.png`, `5-3-4_hive_mapreduce_jobs.png` and `5-3-4_hive_readings_and_anomalies.png` are from that first load; the `_v2` ones are from the current data.

| Measure | Value |
| --- | --- |
| Raw zone size in HDFS | 51.7 MB (7 daily partitions of about 7.7 MB, plus meters and ground truth) |
| Hive: readings and totals per day and meter type (2 MapReduce jobs) | 31.6 seconds |
| Hive: anomalies per type (1 MapReduce job) | 16.2 seconds |

Hive took 16 to 32 seconds for simple counts on under a million rows, because every query starts MapReduce jobs on YARN. Fine for daily reports; far too slow for alerting. This is early evidence for the tool comparison (report section 4.3); the benchmark (TODO item 10) measures it properly.

Silent meters also show in plain Hive: some days have fewer than 48,000 readings per meter type (500 meters x 96).

## Batch zones and HBase writer: tests (11 and 12 November 2026)

Same 300-meter, 4-day test data as run 1. The container results are in run 3 and in "Batch zones and HBase start-up in the container" above.

**Benchmark test (12 November)**: on calibrated test data (300 meters, 4 days, 17 leaks), the MapReduce mapper and reducer (run through a pipe, as Hadoop Streaming does), the Hive query (run through Spark SQL, which accepts the same HiveQL) and the Spark batch job each found the same 27 leaking meter-nights, exactly the 27 the injected leaks should produce; the streaming job's alerts found the same 27. Timings here are not meaningful: they come from the container run.

**Validation test (12 November)**: 4 bad rows added to the raw data by hand: one with an unreadable timestamp, one from an unknown meter, one with a non-numeric reading, and one duplicate of an existing reading. Result: 115,012 raw, 114,997 cleansed, 14 quarantined (11 negative_value, 1 bad_timestamp, 1 unknown_meter, 1 missing_value), 1 duplicate removed. Every rule caught exactly its own row.

**Zone build** (`batch/build_zones.py`):

| | Readings |
| --- | --- |
| Raw | 115,008 |
| Cleansed | 114,997 |
| Quarantined (negative value) | 11 |
| Duplicates removed | 0 (the generator makes none) |
| Curated daily usage rows | 1,200 (300 meters x 4 days) |

The 11 quarantined readings are exactly the 11 injected negative anomalies: validation catches all of them and nothing else.

**HBase writer** (streaming job with `--hbase`, against a stand-in server that accepts the same REST requests as HBase and checks them):

- `meter_readings`: 86,208 rows, one per streamed reading. Row keys sort each meter's newest reading first, as intended.
- `meter_alerts`: 61 rows for 61 injected anomalies. The repeat alerts from sliding windows collapse into one row per meter and alert type, and the repeats are kept as versions (the alert's history).

**Failure handling**: tested with Kafka stopped, the baseline missing, HBase stopped before the start, HBase stopped in the middle of a run, and the raw zone missing. Each stopped with a banner naming the step, even with `2>/dev/null`; the mid-run case named the exact batch (`writing readings to HBase (batch 1)`).

## Run 1: logic test without Kafka (11 November 2026)

300 meters, 3 live days, 61 anomalies (17 leak, 11 negative, 9 silent, 15 spike, 9 stuck), CSV files instead of Kafka.

- First attempt: spikes 10 of 15 detected, 3 false spike alerts. Two causes found and fixed: night spikes were impossible to detect (the generator multiplied a near-zero night reading), and a threshold of 3 times the baseline flagged one meter's normal busy days. After generating spikes as glitches of 10 to 30 times the meter's peak and raising the threshold to 4 times: all 61 detected, no false alerts.
- Also found: parallel queries writing to the same HDFS folder crash each other; each query now writes to its own folder.

These fixes are worth a sentence in report section 6 (difficulties).

## Caveats (for report section 6)

- **Best-case figures.** The same team wrote the data generator and the detection rules, so the rules suit the data. Real meters are noisier, and real leaks can be smaller than the 1 to 6 gallons per 15 minutes simulated here.
- **Single-machine run.** The largest run is 10,000 meters on one laptop. DEWA had about 2.4 million customer accounts in 2025 (1.28 million electricity and 1.16 million water, Annual Statistics Booklet 2025), roughly 240 times more. The runs show the design works and keeps up at 10,000 readings a second; throughput at DEWA's scale would need a cluster and is not measured here.
- **Anomaly count.** 300 anomalies in the full-size run (30 in the smaller runs). Enough to show the method works across all five types, but each type has 35 to 91 cases, so detection rates are estimates, not guarantees.
- **Window alignment.** Stuck spells shorter than 6 hours 45 minutes can be missed, depending on when they start (scale test). A finer window slide fixes this at a processing cost.
- **HBase not used at full size** (see run 5).
