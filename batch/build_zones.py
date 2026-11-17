"""
batch layer: builds the cleansed and curated zones of the data lake from the raw zone.

  raw zone       csv exactly as received (scripts/load_raw_zone.sh)
  cleansed zone  typed, validated, deduplicated readings (parquet, partitioned by day)
  quarantine     readings that failed validation, with the reason, kept for checking
  curated zone   analysis-ready tables (parquet):
                   dim_meter          one row per meter
                   daily_meter_usage  one row per meter per day: total use, number of
                                      readings, highest reading, lowest night flow

the curated tables are what the hive star schema (reporting layer) is built on.

validation rules (a reading goes to quarantine if any fails):
  bad_timestamp   timestamp cannot be parsed
  unknown_meter   meter id not in the meter list
  missing_value   consumption missing or not a number
  negative_value  negative consumption (a meter fault)
duplicates (same meter and timestamp) are removed, keeping one copy.

source: by default the raw zone, the csv files loaded by scripts/load_raw_zone.sh
(day 1 included, which the stream never sends: it is the baseline history). with
--source landing it reads the landing zone instead: every reading that arrived
through kafka, as the streaming job stored it. that is the single-ingestion path of
the architecture (kafka feeds both layers); the two sources give the same cleansed
readings for the streamed days.

run inside the container, from /home/hadoop/dewa:
    spark-submit batch/build_zones.py 2>/dev/null
    spark-submit batch/build_zones.py --source landing 2>/dev/null
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, functions as F  #noqa: E402

from common.errors import fail, run, step  #noqa: E402

READING_SCHEMA = ("meter_id STRING, meter_type STRING, premise_id STRING, area STRING, ts STRING, "
                  "interval_value DOUBLE, unit STRING, cumulative_value DOUBLE, signal_quality INT")
METER_SCHEMA = "meter_id STRING, meter_type STRING, premise_id STRING, premise_type STRING, area STRING"
NIGHT_HOURS = (1, 3)   #01:00 to 03:45, same window as the streaming leak check


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa", help="data lake root")
    p.add_argument("--source", choices=["raw", "landing"], default="raw",
                   help="raw zone (CSV as loaded) or landing zone (what arrived through kafka)")
    args = p.parse_args()
    base = args.base

    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-build-zones")
                 .config("spark.sql.session.timeZone", "UTC")   #timestamps are dubai local time as written
                 .config("spark.sql.shuffle.partitions", "8")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    #------------------------------------------------------------------ raw zone
    with step(f"reading the {args.source} zone"):
        try:
            if args.source == "raw":
                raw = (spark.read.option("header", True).schema(READING_SCHEMA)
                       .csv(f"{base}/raw/readings")
                       .withColumn("dt", F.col("dt").cast("string")))
            else:
                #same columns as the raw csv, so the validation below is identical
                raw = (spark.read.parquet(f"{base}/landing/readings")
                       .withColumn("ts", F.date_format("event_ts", "yyyy-MM-dd'T'HH:mm:ss"))
                       .select("meter_id", "meter_type", "premise_id", "area", "ts", "interval_value",
                               "unit", "cumulative_value", "signal_quality", F.col("dt").cast("string")))
            meters = spark.read.option("header", True).schema(METER_SCHEMA).csv(f"{base}/raw/meters")
            n_meters = meters.count()
        except Exception as e:
            fail("reading the raw zone", f"cannot read the raw zone under {base}/raw ({str(e).splitlines()[0][:200]})",
                 "load it with: bash scripts/load_raw_zone.sh")
        if n_meters == 0:
            fail("reading the raw zone", f"no meters in {base}/raw/meters", "load it with: bash scripts/load_raw_zone.sh")

    with step("validating and writing the cleansed zone and quarantine"):
        #------------------------------------------------------------------ validate
        checked = (raw
                   .withColumn("event_ts", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))
                   .join(meters.select("meter_id", F.lit(True).alias("known")), "meter_id", "left")
                   .withColumn("reject_reason",
                               F.when(F.col("event_ts").isNull(), "bad_timestamp")
                               .when(F.col("known").isNull(), "unknown_meter")
                               .when(F.col("interval_value").isNull(), "missing_value")
                               .when(F.col("interval_value") < 0, "negative_value"))
                   .drop("known"))

        columns = ["meter_id", "meter_type", "premise_id", "area", "event_ts",
                   "interval_value", "unit", "cumulative_value", "signal_quality", "dt"]

        cleansed = (checked.where(F.col("reject_reason").isNull())
                    .select(*columns)
                    .dropDuplicates(["meter_id", "event_ts"]))
        quarantine = checked.where(F.col("reject_reason").isNotNull()).select(*columns[:-1], "ts", "reject_reason", "dt")

        cleansed.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/cleansed/readings")
        quarantine.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/quarantine/readings")

    with step("building the curated zone"):
        #------------------------------------------------------------------ curated zone
        cleansed = spark.read.parquet(f"{base}/cleansed/readings")
        meters.write.mode("overwrite").parquet(f"{base}/curated/dim_meter")

        night = F.hour("event_ts").between(*NIGHT_HOURS)
        daily = (cleansed.groupBy(F.to_date("event_ts").alias("usage_date"), "meter_id")
                 .agg(F.round(F.sum("interval_value"), 3).alias("total_value"),
                      F.count("*").alias("readings"),
                      F.max("interval_value").alias("max_interval"),
                      F.min(F.when(night, F.col("interval_value"))).alias("min_night_flow"))
                 .join(meters, "meter_id")
                 .select("usage_date", "meter_id", "meter_type", "premise_id", "premise_type", "area",
                         "total_value", "readings", "max_interval", "min_night_flow"))
        daily.write.mode("overwrite").parquet(f"{base}/curated/daily_meter_usage")

    with step("checking the results"):
        #------------------------------------------------------------------ report
        n_raw = raw.count()
        n_clean = cleansed.count()
        n_quar = spark.read.parquet(f"{base}/quarantine/readings").count()
        n_daily = spark.read.parquet(f"{base}/curated/daily_meter_usage").count()

        print(f"\nZone build finished (source: {args.source} zone)")
        print(f"  {args.source + ' readings':<20}{n_raw:>10,}")
        print(f"  cleansed readings   {n_clean:>10,}")
        print(f"  quarantined         {n_quar:>10,}")
        print(f"  duplicates removed  {n_raw - n_clean - n_quar:>10,}")
        print(f"  daily usage rows    {n_daily:>10,}  (meters x days)")

        print("\nQuarantine reasons:")
        spark.read.parquet(f"{base}/quarantine/readings").groupBy("reject_reason").count().show()

        print("Sample of curated daily_meter_usage:")
        spark.read.parquet(f"{base}/curated/daily_meter_usage").orderBy("usage_date", "meter_id").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    run(main)
