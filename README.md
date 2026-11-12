# DEWA Smart Meter Data Lake

ISIT312 Big Data Management group project, Autumn 2026.

A data lake design for DEWA's smart meter network: Kafka ingestion, a Spark Structured Streaming path for near-real-time leak detection, HBase for per-meter lookups, HDFS lake zones, and a Hive star schema for reporting.

- **Setting up on Mac or Windows:** [SETUP.md](SETUP.md)
- **What is done and what is left:** [TODO.md](TODO.md)
- **Results of every run, for the report:** [RESULTS.md](RESULTS.md)

## Folders

| Folder | Contents |
| --- | --- |
| `data_generator/` | Creates synthetic 15-minute meter readings with injected anomalies |
| `docker/`, `Dockerfile` | Builds the Hadoop container (see SETUP.md) |
| `scripts/` | Container setup, loading the raw zone, starting Kafka and HBase, resetting a run |
| `hive/` | Hive table definitions (raw zone, cleansed and curated zones) |
| `batch/` | Batch layer: builds the cleansed, quarantine and curated zones |
| `hbase/` | Example HBase queries on the serving layer |
| `common/` | Shared error handling (step names and failure banners) |
| `streaming/` | Replay into Kafka, baseline batch job, streaming detection job, HBase writer |
| `evaluation/` | Compares alerts with the injected anomalies |
| `screenshots/` | Screenshots for the report, named by report section: `mac/` for the Mac run, `other/` for any other setup (Windows, Linux) |
| `data/` | Generated data (not committed: regenerate it with the script) |

## How the synthetic data is generated

DEWA does not publish its 15-minute meter readings, so the project generates realistic readings with `data_generator/generate.py`. It uses only Python's standard library and a fixed random seed (42), so the same command always produces identical data on any computer.

**Meters.** Each premise has one water meter and one electricity meter. Premises are 60% apartments, 25% villas and 15% commercial, spread across ten Dubai areas (Al Barsha, Deira, Jumeirah, Dubai Marina, Al Quoz, Mirdif, Business Bay, International City, Al Nahda, Jebel Ali).

**Consumption.** Every meter reports once every 15 minutes (96 readings a day), the interval DEWA reports for its smart meters. Each reading is built from:

1. **A daily average for the premise type:**

   | Premise | Water (imperial gallons a day) | Electricity (kWh a day) |
   | --- | --- | --- |
   | Apartment | 100 | 25 |
   | Villa | 400 | 120 |
   | Commercial | 600 | 200 |

2. **A per-premise factor** between 0.6 and 1.5, so no two homes use exactly the same amount.
3. **A daily profile** spreading the day's use over the 96 intervals. Homes have morning (06:00 to 08:00) and evening (18:00 to 20:00) water peaks and almost no use between midnight and 05:00; their electricity rises through the afternoon. Commercial premises follow office hours (08:00 to 17:00).
4. **A seasonal factor** by month. Electricity is highest in July and August (air conditioning) and about half of that in January; water varies less (0.85 to 1.1).
5. **Random noise** of about 25% on every reading. In the quiet night hours a home's water reading is exactly zero 60% of the time, as real meters show.

Each reading also carries a running total (`cumulative_value`, the meter register) and a signal quality score (70 to 100).

**Anomalies.** 3% of meters get one anomaly each, starting after the first day (day 1 stays clean, because the pipeline uses it as history to learn each meter's normal range). Water meters mostly get leaks, because leak detection is the project's main use case.

| Type | Meters | Share | What is injected |
| --- | --- | --- | --- |
| leak | Water | 50% of water anomalies | A constant extra flow of 1 to 6 gallons per 15 minutes (plus or minus 10%), from the start time until the end of the data, as an unrepaired leak would |
| spike | Both | 15% water, 30% electricity | One reading of 10 to 30 times the meter's normal peak (a meter glitch) |
| stuck | Both | 15% water, 30% electricity | Zero consumption for 6 to 48 hours (the meter stops recording) |
| silent | Both | 10% water, 20% electricity | No readings at all for 2 to 12 hours (communication failure) |
| negative | Both | 10% water, 20% electricity | One negative reading (meter fault) |

Every anomaly is written to `data/ground_truth.csv` with its meter and start and end time. The evaluation script compares the pipeline's alerts against this file, which is how detection rate and detection time are measured exactly.

**Assumptions.** The daily averages, profiles and seasonal factors are our own estimates. They are to be calibrated against DEWA's annual statistics on Dubai Pulse (see TODO.md), and the report states them as assumptions.

## Running the pipeline

All commands below are typed inside the container, from `/home/hadoop/dewa` (see SETUP.md for getting there).

### 1. Generate the data

```
python3 data_generator/generate.py
```

A quick test set: 1,000 meters for 7 days, about 670,000 readings. Full project dataset: `--meters 10000 --days 30` (about 28.8 million readings, about 2.3 GB). `--days` must be at least 3.

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

Use three container terminals (a new terminal tab, then `docker exec -it hadoop bash` and `cd /home/hadoop/dewa`).

**Terminal 1: Kafka, HBase, baseline, then the detection job**

```
bash scripts/start_kafka.sh
bash scripts/start_hbase.sh
spark-submit streaming/baseline.py 2>/dev/null
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 streaming/detect_stream.py --kafka-alerts --hbase 2>/dev/null
```

The first run of the last command downloads the Kafka connector for Spark (about a minute). It checks that Kafka, the baseline and HBase are ready, prints `waiting for readings`, then prints each batch of alerts as it raises them. Leave out `--hbase` to run without HBase.

**Terminal 2: replay the readings into Kafka**

```
python3 streaming/replay.py --interval 1 | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic meter-readings --property parse.key=true --property key.separator='|'
```

It prints each interval as it sends it. Six simulated days take about 10 minutes.

**Terminal 3 (optional): watch the alerts topic**

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts
```

When the replay finishes (terminal 2 prints `Replay finished` and returns to the prompt), the detection job stops by itself 30 seconds later and prints `All queries stopped.` Stop terminal 3 with Ctrl+C. (To change the wait, use `--idle-stop SECONDS`; `--idle-stop 0` runs until you press Ctrl+C.)

To run the stream again from scratch: `bash scripts/reset_stream.sh`.

### 4. Look at the serving layer (HBase)

Only after the replay has finished and the detection job has stopped: before that the tables are empty.

```
bash scripts/hbase_queries.sh
```

(This runs `hbase/queries.hbase` and hides HBase's start-up log lines. To type your own queries, use `hbase shell`.)

Shows the open alerts (one row per meter and alert type), one meter's newest readings, and the history of one alert. The file uses meter W0000108, which has a leak in the default dataset; with other data, change it to a meter from your own alerts.

### 5. Evaluate

```
spark-submit evaluation/evaluate.py 2>/dev/null
```

Prints detection rate, false alerts, simulated detection delay and real processing latency per anomaly type, and lists any missed anomalies. Latency is taken from each anomaly's first alert: the time from the revealing reading's arrival in Kafka to the moment that alert was computed and ready to publish. It can be run again at any time on the alerts already in HDFS.

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

| Alert | Rule |
| --- | --- |
| leak | Water meter, every reading in a 1-hour window between 01:00 and 04:00 above 0.5 gallons (minimum night flow analysis) |
| spike | One reading above 4 times the meter's 99th percentile from the baseline |
| negative | A negative reading |
| stuck | Zero consumption for 6 hours |
| silent | Fewer than 4 readings in a 1-hour window |

**Latest result** (run 3: 1,000 meters, 6 live days through Kafka, 30 injected anomalies, HBase on): all 30 detected, no false alerts, alerts a few seconds after the revealing reading arrived. The latency figures are being re-measured with the corrected method (see RESULTS.md). Full figures and caveats are in [RESULTS.md](RESULTS.md).
