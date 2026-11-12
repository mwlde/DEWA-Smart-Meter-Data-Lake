# Results

Every measured result, with the conditions it came from. Report sections 5.5 and 7.1 take their figures from here. Add a new run at the top; keep the old ones. Screenshots are in `screenshots/mac/`.

## Correction to the latency figures (12 November 2026)

The "median latency, real" figures in runs 2 and 3 below are too low, for two reasons found in the mid-project review:

1. **Fastest alert, not first alert.** `evaluate.py` took the fastest of each anomaly's alerts. A leak raises an alert for every window that shows it, so this picked the quickest repeat and hid slower first alerts. The clearest case: in run 2 the first silent alert took about 26 seconds (Spark was still starting), but the table shows 4.35 s for silent. The script now takes each anomaly's **first** alert.
2. **Measured to the start of the micro-batch.** `alert_ts` is when the micro-batch that raised the alert started, before Spark had computed it. The streaming job now also records `emitted_ts`, when the alert was computed and ready to publish, and latency is measured to that. In a test here, the gap between the two was about 1.3 to 1.7 seconds per alert.

What to do: re-run `spark-submit evaluation/evaluate.py 2>/dev/null` on the run 3 alerts (still in HDFS) to fix reason 1 now; reason 2 is fixed from the next streaming run on (old alerts have no `emitted_ts`, so the script falls back to `alert_ts` and says so). Until then, quote the figures below as lower bounds. Detection rates and false alert counts are not affected.

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
- **Small, single-machine run.** 1,000 meters on one laptop. DEWA has about 2.3 million meters. Latency figures show the design works; throughput at DEWA's scale is extrapolated, not measured.
- **Few anomalies.** 30 anomalies give a clear result but not a statistically strong one. The full-size run (TODO) gives about 300.

## Planned

- **Full-size run**: 10,000 meters, 30 days, about 300 anomalies.
- **Sensitivity test**: smaller leaks (for example 0.2 to 1 gallon per 15 minutes) to find where detection starts to fail. Showing the limit of the method is stronger evidence than a perfect score.
- **Benchmark**: the same leak check in MapReduce, Hive, Spark batch and Spark Streaming, timed.
