# DEWA Smart Meter Data Lake

ISIT312 Big Data Management group project, Autumn 2026.

A data lake design for DEWA's smart meter network: Kafka ingestion, a Spark Structured Streaming path for near-real-time leak detection, HBase for per-meter lookups, HDFS lake zones, and a Hive star schema for reporting.

## Folders

| Folder | Contents |
| --- | --- |
| `data_generator/` | Script that creates synthetic 15-minute meter readings with injected anomalies |
| `data/` | Generated data (not committed: regenerate it with the script) |
| `scripts/` | Shell scripts, e.g. `load_raw_zone.sh` copies `data/` into the HDFS raw zone |
| `hive/` | Hive SQL, e.g. `01_raw_zone.sql` creates external tables over the raw zone |

More folders (ingestion, streaming, HBase, benchmark) are added as the build progresses.

## Generate the data

Needs Python 3 only, no packages to install.

```
python3 data_generator/generate.py
```

That makes a quick test set: 1,000 meters for 7 days, about 670,000 readings. For the full project dataset:

```
python3 data_generator/generate.py --meters 10000 --days 30
```

That is about 28.8 million readings and roughly 2 GB, so it takes several minutes.

The seed is fixed, so everyone who runs the same command gets identical data.

## Output

| File | What it is |
| --- | --- |
| `data/meters.csv` | One row per meter: ID, type, premise, premise type, area |
| `data/readings/dt=YYYY-MM-DD/readings.csv` | One file per day, in the folder layout Hive uses for partitions |
| `data/ground_truth.csv` | Every injected anomaly with its start and end time, used to measure detection |

Anomaly types: `leak` (water only, constant extra flow until the end of the data), `spike` (one very high reading), `stuck` (zero consumption for 6 to 48 hours), `silent` (no readings for 2 to 12 hours), `negative` (one negative reading).

Timestamps are Dubai local time. Water is in imperial gallons, electricity in kWh. Consumption levels are placeholders to be calibrated against DEWA's annual statistics on Dubai Pulse.
