# DEWA Smart Meter Data Lake

ISIT312 Big Data Management group project, Autumn 2026.

A data lake design for DEWA's smart meter network: Kafka ingestion, a Spark Structured Streaming path for near-real-time leak detection, HBase for per-meter lookups, HDFS lake zones, and a Hive star schema for reporting.

- **Setting up on Mac or Windows:** [SETUP.md](SETUP.md)
- **Every measured result behind the report:** [RESULTS.md](RESULTS.md)

**Final result:** 10,000 meters for 30 days with 226 empty premises and 1% of readings arriving late: 290 of 300 anomalies detected (96.7%), no false alerts, no alerts on the empty premises and no late reading dropped. The 10 missed are the water meters stuck for less than a day, which the 24-hour water rule leaves out by design. Leak, stuck, spike and negative alerts come about 2 to 2.6 seconds after the reading that reveals them, without waiting for later readings; silent alerts come about 45 minutes after the first missing reading, because they wait for the watermark. In the benchmark, every tool found the same 1,370 leak nights; on three times the data (6.5 GB), Spark on YARN was 30% faster than MapReduce and 40% faster than Hive. Every figure, with the conditions behind it, is in [RESULTS.md](RESULTS.md).

## Folders

| Folder | Contents |
| --- | --- |
| `data_generator/` | Creates synthetic 15-minute meter readings with injected anomalies |
| `docker/`, `Dockerfile` | Builds the Hadoop container (see SETUP.md) |
| `scripts/` | Container setup and tool check, loading the raw zone, starting Kafka and HBase, the one-command streaming run and its reset, the HBase queries, and one script per experiment (benchmark, micro-batch, replay speed, restart) |
| `hive/` | Hive table definitions (raw zone, cleansed and curated zones, warehouse) and the reporting queries |
| `batch/` | Batch layer: builds the cleansed, quarantine and curated zones, and the star schema warehouse |
| `hbase/` | Example HBase queries on the serving layer |
| `common/` | Shared error handling (step names and failure banners) |
| `streaming/` | Replay into Kafka, baseline batch job, streaming detection job, HBase writer |
| `evaluation/` | Compares alerts with the injected anomalies; sensitivity test for small leaks; replay-speed and restart comparisons |
| `benchmark/` | The same leak check in MapReduce, Hive and Spark batch, the comparison script, and the MapReduce stuck check for the micro-batch experiment |
| `screenshots/` | Screenshots for the report, named by report section: `mac/` for the Mac run, `other/` for any other setup (Windows, Linux) |
| `data/` | Generated data (not committed: regenerate it with the script) |
| `logs/` | Output of the streaming run (`stream.log`) and of each experiment script, such as `benchmark_results.md` (not committed) |

## How the synthetic data is generated

DEWA does not publish its 15-minute meter readings, so the project generates realistic readings with `data_generator/generate.py`. It uses only Python's standard library and a fixed random seed (42), so the same command always produces identical data on any computer.

**Meters.** Each premise has one water meter and one electricity meter. Premises are 60% apartments, 25% villas and 15% commercial, spread across ten Dubai areas (Al Barsha, Deira, Jumeirah, Dubai Marina, Al Quoz, Mirdif, Business Bay, International City, Al Nahda, Jebel Ali).

**Consumption.** Every meter reports once every 15 minutes (96 readings a day), the interval DEWA reports for its smart meters. Each reading is built from:

1. **A daily average for the premise type**, calibrated to DEWA's Annual Statistics Booklet 2025 (see "Calibration" below):

   | Premise | Water (imperial gallons a day) | Electricity (kWh a day) |
   | --- | --- | --- |
   | Apartment | 135 | 32 |
   | Villa | 540 | 155 |
   | Commercial | 700 | 440 |

2. **A per-premise factor** between 0.6 and 1.5, so no two homes use exactly the same amount.
3. **A daily profile** spreading the day's use over the 96 intervals. Homes have morning (06:00 to 08:00) and evening (18:00 to 20:00) water peaks and almost no use between midnight and 05:00; their electricity rises through the afternoon. Commercial premises follow office hours (08:00 to 17:00).
4. **A seasonal factor** by month. Electricity is highest in July and August (air conditioning) and about half of that in January; water varies less (0.85 to 1.1).
5. **Random noise** of about 25% on every reading. In the quiet night hours a home's water reading is exactly zero 60% of the time, as real meters show.

Each reading also carries a running total (`cumulative_value`, the meter register) and a signal quality score (70 to 100).

**Anomalies.** 3% of meters get one anomaly each, starting after the first day (day 1 stays clean, because the pipeline uses it as history to learn each meter's normal range). Water meters mostly get leaks, because leak detection is the project's main use case.

| Type | Meters | Share | What is injected |
| --- | --- | --- | --- |
| leak | Water | 50% of water anomalies | A constant extra flow of 1 to 6 gallons per 15 minutes (plus or minus 10%), from the start time until the end of the data, as an unrepaired leak would. `--leak-min` and `--leak-max` change the range (used by the sensitivity test) |
| spike | Both | 15% water, 30% electricity | One reading of 10 to 30 times the meter's normal peak (a meter glitch) |
| stuck | Both | 15% water, 30% electricity | Zero consumption for 6 to 48 hours (the meter stops recording) |
| silent | Both | 10% water, 20% electricity | No readings at all for 2 to 12 hours (communication failure) |
| negative | Both | 10% water, 20% electricity | One negative reading (meter fault) |

Every anomaly is written to `data/ground_truth.csv` with its meter and start and end time. The evaluation script compares the pipeline's alerts against this file, which is how detection rate and detection time are measured exactly.

**Normal behaviour that looks like a fault.** The anomalies above are ones we designed the rules to find, so catching them proves the code works more than the method. Two kinds of healthy behaviour are added to test the rules against things they should not flag:

- **Empty premises** (`--vacant-rate`, default 5% of premises): residents away for 3 to 14 days. The water meter reads exactly zero, as a stuck meter would; electricity drops to a base load of 30% (fridge, a/c, standby). Listed in `data/vacancies.csv`. Premises with an injected anomaly are never made empty, and the empty spells use their own random generator, so every other reading is identical to `--vacant-rate 0` data.
- **Late readings** (`streaming/replay.py --late-share`, 1% by default in `run_stream.sh`): held back and sent 15 or 30 minutes later, after newer readings of the same meter.

**Calibration.** The daily averages are set so that, averaged over a year, each kind of account uses what DEWA's [Annual Statistics Booklet 2025](https://dewa.gov.ae/-/media/Files/About-DEWA/Annual-Statistics/DEWA_Annual_Statistics_Booklet_2025_EN.ashx) reports (2025 consumption by category, divided by the number of customers in that category and by 365):

| Per account, per day | DEWA 2025 | Generator (checked over a sample of every month) |
| --- | --- | --- |
| Home, water | 258 gallons | 252 gallons |
| Commercial, water | 708 gallons | 707 gallons |
| Home, electricity | 52.6 kWh | 55.1 kWh |
| Commercial, electricity | 336 kWh | 343 kWh |

Our share of commercial premises (15%) is close to DEWA's (14% of water customers, 19% of electricity customers).

**Assumptions.** DEWA does not split homes into apartments and villas, so the 70/30 apartment and villa mix and the size difference between them (a villa uses 4 times the water and 4.8 times the electricity of an apartment) are our own estimates, as are the daily profiles and seasonal factors. The report states them as assumptions.

**Units.** Water is in imperial gallons, the unit of DEWA's annual statistics. DEWA has billed water in cubic metres since March 2025 (1 cubic metre = 219.97 imperial gallons).

## Running the pipeline

All commands below are typed inside the container, from `/home/hadoop/dewa` (see SETUP.md for getting there).

### 1. Generate the data

```
python3 data_generator/generate.py
```

A quick test set: 1,000 meters for 7 days, about 670,000 readings. Full project dataset: `--meters 10000 --days 30` (about 28.8 million readings, about 2.2 GB). `--days` must be at least 3.

| File | What it is |
| --- | --- |
| `data/meters.csv` | One row per meter: ID, type, premise, premise type, area |
| `data/readings/dt=YYYY-MM-DD/readings.csv` | One file per day, in the folder layout Hive uses for partitions |
| `data/ground_truth.csv` | Every injected anomaly with its start and end time |

Timestamps are Dubai local time. Water is in imperial gallons, electricity in kWh.

### 2. Load the raw zone and build the lake zones

```
bash scripts/load_raw_zone.sh 2>/dev/null
hive -f hive/01_raw_zone.sql 2>/dev/null
spark-submit batch/build_zones.py 2>/dev/null
hive -f hive/02_lake_zones.sql 2>/dev/null
```

| Zone | HDFS folder | What it holds |
| --- | --- | --- |
| Raw | `/user/hadoop/dewa/raw` | The CSV files exactly as received |
| Cleansed | `/user/hadoop/dewa/cleansed/readings` | Typed, validated, deduplicated readings (Parquet, one folder per day) |
| Quarantine | `/user/hadoop/dewa/quarantine/readings` | Readings that failed validation (bad timestamp, unknown meter, missing value, negative value), with the reason |
| Curated | `/user/hadoop/dewa/curated` | `dim_meter`, `daily_meter_usage` (one row per meter per day) and `meter_baseline` |
| Landing | `/user/hadoop/dewa/landing/readings` | Every reading that arrived through the stream (Parquet) |
| Alerts | `/user/hadoop/dewa/alerts` | Every alert the streaming job raised (Parquet) |

### 3. Streaming: live leak detection

Day 1 of the data is history: the batch layer builds each meter's normal range from it. Days 2 onwards are replayed into Kafka as a live stream, one 15-minute interval per second (a simulated day every 96 seconds).

**One terminal, one command** (the normal way):

```
bash scripts/run_stream.sh
```

It runs every step in order and says which one it is on: reset the last run, start Kafka and HBase, build the baseline if it is missing, start the detection job in the background, replay the six live days (about 10 minutes), wait for the detection job to stop, then print the evaluation. Add `--no-hbase` to run without HBase. Other options: `--interval N` (real seconds per 15-minute interval), `--days N` (replay only the first N live days), `--no-late` (no late readings), `--tag NAME` (also save the evaluation summary, used by the speed test).

The detection job's output (every batch of alerts) goes to `logs/stream.log`. To watch the alerts arrive, open a **second** terminal (new terminal tab, `docker exec -it hadoop bash`, `cd /home/hadoop/dewa`) and run:

```
tail -f logs/stream.log
```

Stop it with Ctrl+C when the run is over; that only stops the watching, not the run.

**By hand, three terminals** (only if you want to run each part yourself):

| Terminal | Command | Leave it |
| --- | --- | --- |
| 1 | `bash scripts/reset_stream.sh`, `bash scripts/start_hbase.sh`, then `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 streaming/detect_stream.py --kafka-alerts --hbase 2>/dev/null` | running; start terminal 2 once it prints `waiting for readings` |
| 2 | `python3 streaming/replay.py --interval 1 \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic meter-readings --property parse.key=true --property key.separator='\|'` | until it prints `Replay finished`; terminal 1 then stops by itself 30 seconds later |
| 3 (optional) | `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts` | Ctrl+C when done |

If the baseline is missing, run `spark-submit streaming/baseline.py 2>/dev/null` in terminal 1 first. `--idle-stop SECONDS` changes the 30-second wait; `--idle-stop 0` runs until Ctrl+C.

### 4. Look at the serving layer (HBase)

Only after the replay has finished and the detection job has stopped: before that the tables are empty.

```
bash scripts/hbase_queries.sh
```

(This runs `hbase/queries.hbase` and hides HBase's start-up log lines. To type your own queries, use `hbase shell`.)

Shows the open alerts (one row per meter and alert type), one meter's newest readings, and the history of one alert. The file uses meter W0000108, which has a leak in the default dataset; with other data, change it to a meter from your own alerts.

### 5. Evaluate

`run_stream.sh` already does this at the end. To run it again on the alerts in HDFS:

```
spark-submit evaluation/evaluate.py 2>/dev/null
```

Prints detection rate, false alerts, simulated detection delay and real processing latency per anomaly type, and lists any missed anomalies. Latency is taken from each anomaly's first alert: the time from the revealing reading's arrival in Kafka to the moment that alert was computed and ready to publish. It also prints whether each check waits for later readings, the real time from each anomaly starting to its first alert, the number of incidents (alerts collapsed to one per meter and type), repeated records for the same window, and alerts raised on empty premises. It can be run again at any time on the alerts already in HDFS.

### 6. Reporting warehouse (star schema)

After the zones are built (and after a streaming run, for the alert facts):

```
spark-submit batch/build_warehouse.py 2>/dev/null
hive -f hive/03_warehouse.sql 2>/dev/null
hive -f hive/04_reports.sql 2>/dev/null
```

A star schema with two fact tables sharing their dimensions (a constellation), in the Hive database `dewa_dw`:

| Table | One row per | Key |
| --- | --- | --- |
| `fact_daily_usage` | meter and day (partitioned by month) | `date_id`, `meter_id` |
| `fact_alert` | meter, alert type and day | `date_id`, `meter_id`, `alert_type_id` |
| `dim_date` | day: day name, weekend flag, week, month, quarter, year | `date_id` |
| `dim_meter` | meter, with its premise and area (hierarchy flattened) | `meter_id` |
| `dim_alert_type` | check: what it detects, when it fires, who acts on it | `alert_type_id` |

`hive/04_reports.sql` holds the reports: water use by month and area with subtotals (ROLLUP), average daily use by premise type and weekday or weekend (CUBE), alerts by area and type (GROUPING SETS), a monthly billing extract per premise, the three biggest users per area (RANK), and missing readings by area.

### 7. Full-size run (final results)

The same steps on 10,000 meters for 30 days (28.8 million readings, about 2.2 GB). Allow about 2 hours in all: the stream takes about 50 minutes and the benchmark 30 to 45. Before starting: close other apps, give Docker at least 8 GB of memory and 4 CPUs (Docker Desktop, Settings, Resources; the final runs used 12 GB and 10 CPUs), and stop HBase to free its memory (`hbase-daemon.sh stop rest`, then `stop-hbase.sh`).

```
python3 data_generator/generate.py --meters 10000 --days 30
bash scripts/load_raw_zone.sh 2>/dev/null
hive -f hive/01_raw_zone.sql 2>/dev/null
spark-submit --driver-memory 2g batch/build_zones.py 2>/dev/null
hive -f hive/02_lake_zones.sql 2>/dev/null
bash scripts/run_stream.sh --no-hbase
spark-submit --driver-memory 2g batch/build_warehouse.py 2>/dev/null
hive -f hive/03_warehouse.sql 2>/dev/null
hive -f hive/04_reports.sql 2>/dev/null
bash scripts/run_benchmark.sh
```

`run_stream.sh` rebuilds the baseline by itself when the raw zone is newer than it, and runs the evaluation at the end. The full-size stream runs without HBase: HBase is shown in the 1,000-meter runs, and writing 10,000 readings a second through its REST server from one machine would measure that bottleneck rather than the pipeline. To go back to the small dataset, run the same steps with `python3 data_generator/generate.py` (no options).

## Experiments behind the report

Each of these produced a table in the report; RESULTS.md has the figures and the conditions they came from.

### Benchmark: why some tools fit and others do not

```
bash scripts/run_benchmark.sh
```

Runs the same night-flow leak rule on the raw zone with each tool below, 3 times each, and times the whole command each time (what a user waits for, start-up included). Spark batch runs twice: in local mode, and on YARN like MapReduce and Hive:

| Version | File |
| --- | --- |
| MapReduce (Hadoop Streaming, Python mapper and reducer) | `benchmark/leak_mapper.py`, `benchmark/leak_reducer.py` |
| Hive (HiveQL with window functions, run as MapReduce jobs) | `benchmark/leak_check.hql` |
| Spark batch (DataFrame window functions) | `benchmark/spark_batch.py` |

It then checks that every version found the same leaking meter-nights as the injected leaks, adds the streaming job's alerts and latency from the last `run_stream.sh` run, and prints one table (also saved to `logs/benchmark_results.md`). About 5 to 10 minutes for the 1,000-meter data. If YARN cannot run Spark, that row is skipped and the rest still runs. `bash scripts/run_benchmark.sh 5` does 5 runs of each.

Run it after `run_stream.sh` on the same data, so the streaming row matches.

### Micro-batch experiment: MapReduce every 15 minutes

```
bash scripts/run_microbatch.sh
```

The benchmark above runs each batch tool once over the whole data, as a nightly job would. This experiment asks the harder question: could MapReduce do the streaming job's work by running every 15 minutes? Each cycle runs the leak check on the last hour (4 interval files) and the electricity stuck check on the last 6 hours (24 files) as Hadoop Streaming jobs on YARN, and times them. MapReduce keeps nothing between runs, so each cycle reads its whole window again. Needs data of at least 3 days; about 5 to 10 minutes. Results in `logs/microbatch_results.md`.

### Replay-speed test (about 25 minutes)

```
bash scripts/speed_test.sh
```

The replay squeezes 15 minutes into 1 second. A check that waits for later readings before it fires looks seconds-fast in the replay but is not in real time. The test makes its own small dataset (1,000 meters, 3 days, replacing what is in `data/` and the raw zone) and runs the stream twice on it, at 1 and at 4 seconds per interval, comparing the median latency over every alert record (hundreds of samples, not just each anomaly's first alert): a check whose latency does not grow when the replay slows down fires on the reading that reveals the anomaly, so its replay latency is its real-world latency. A check that waits for k later intervals gets k x 3 seconds slower. Results in `logs/speed_test_results.md`.

### Restart test (about 15 minutes)

```
bash scripts/restart_test.sh
```

Checks that the streaming job loses and repeats nothing when it crashes. It runs the same replay twice on its own small dataset (1,000 meters, 3 days, in `data/restart_test`) in a separate lake folder, `/user/hadoop/dewa_restart_test`, so the main lake and its results are left alone. Run A runs without interruption. In run B the detection job is killed with `kill -9` partway through (by default 110 seconds in: 03:30 on the second live day, in the middle of that night's leak windows) and restarted from its checkpoint while the replay carries on into Kafka. Both runs are evaluated, then `evaluation/compare_runs.py` compares them: detection and false alerts per check, every alert window each run raised (a window in one run only would be an alert the restart lost or added), and each run's landing zone against the raw zone (no reading missing, none stored twice). Output in `logs/restart_test_results.txt`. Latency in run B is higher for the anomalies that came in while the job was down: that is the catch-up, not a fault.

### Sensitivity test: how small a leak can it catch?

```
spark-submit evaluation/sensitivity.py 2>/dev/null
```

Generates six datasets that differ only in leak size (from 0.1 up to the normal 1 to 6 gallons per 15 minutes; 2,000 meters, 7 days, about 156 leaks each), runs the leak rule on each at four alarm thresholds (0.2, 0.3, the pipeline's 0.5, and 1.0), and prints the share of leaks caught and the false alerts. It uses the same rule as `benchmark/spark_batch.py`, which matched the streaming job exactly in every benchmark. About 3 to 5 minutes; Hadoop is not needed. The table is also saved to `logs/sensitivity_results.md`.

### Bigger batch-only benchmark (optional)

At 2.2 GB, every batch tool's runtime is mostly start-up. To see how the tools compare when there is more work to do, run the benchmark alone on three times the data (30,000 meters, 30 days, about 86 million readings, about 6.5 GB). The stream is not run on this data, so the streaming row is left out. This replaces the raw zone.

```
python3 data_generator/generate.py --meters 30000 --days 30
bash scripts/load_raw_zone.sh 2>/dev/null
bash scripts/run_benchmark.sh --no-streaming
```

About 45 to 60 minutes in all. Docker needs about 15 GB of free disk (`df -h /` inside the container) and preferably 12 GB of memory. To go back to the full-size data afterwards, regenerate it with `--meters 10000 --days 30` and reload.

## If something fails

Every script says which step it is on (`[step] ...`). If anything goes wrong it stops and prints a banner like this, even when the command ends in `2>/dev/null`:

```
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
FAILED at step: pre-flight: Kafka
Error: Kafka is not reachable at localhost:9092 ([Errno 111] Connection refused)
What to do: start it with: bash scripts/start_kafka.sh
Full details: run the same command again without 2>/dev/null
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
```

Shell scripts print `FAILED in <script> at line N: <command>`. `2>/dev/null` only hides Spark's log noise, so for the full error, run the command again without it. Send the banner to the group when asking for help.

## Detection rules

Thresholds are set at the top of `streaming/detect_stream.py`.

| Alert | Rule | Fires |
| --- | --- | --- |
| leak | Water meter, every reading in a 1-hour window between 01:00 and 04:00 above 0.5 gallons (minimum night flow analysis) | On the reading that completes the window (update mode) |
| spike | One reading above 4 times the meter's 99th percentile from the baseline | On the reading |
| negative | A negative reading | On the reading |
| stuck (electricity) | Zero consumption for 6 hours (an empty home still draws power) | On the reading that completes the window (update mode) |
| stuck (water) | Zero for 24 hours while the premise's electricity over the same day is at least 60% of its normal (occupied) | On the readings that complete the window for both meters (update mode) |
| silent | Fewer than 4 readings in a 1-hour window | When the 30-minute watermark passes the end of the window (append mode): about 45 minutes after the first missing reading |

**Why the modes matter.** In append mode a window check fires only once the watermark passes the end of its window, so it waits for later readings. The replay squeezes an interval into a second, so that wait looks like a few seconds but is about 30 minutes in real time. Leak and stuck therefore run in update mode and fire on the reading that completes their window, which the replay-speed test confirms. Silent has to wait by nature: a missing reading only shows once later ones arrive.

**Restarts.** Each micro-batch's alerts are written to their own folder (`alerts/source=<check>/batch=<id>`) with overwrite, and HBase alert cells carry the window start as their version, so a batch Spark repeats after a crash is written once, not twice. The Kafka `alerts` topic is at-least-once: a consumer should treat meter, type and window start as the alert's key.

## Known limitations

- The `Dockerfile` route in SETUP.md has not yet been test-built on a clean machine; the project's runs used an equivalent image built earlier on a Mac.
- The batch layer can build the lake zones from the landing zone (`batch/build_zones.py --source landing`). The restart test showed the landing zone holds every streamed reading exactly once, but the zones have not been rebuilt from it after the full-size run.
- Further limits of the results (synthetic data, one machine, HBase not used at full size) are listed at the end of RESULTS.md.
