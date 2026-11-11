"""
Batch layer: builds each meter's normal range from historical readings.

Takes the first day of the raw zone as history (the replay script streams the
later days as "live" data), computes each meter's 99th percentile 15-minute
reading, and saves it to the curated zone. The streaming job joins against this
table to spot spikes: a reading far above what that meter normally does.

This is the batch layer feeding the speed layer (Lambda architecture).

Run inside the container, from /home/hadoop/dewa:
    spark-submit streaming/baseline.py
"""

import argparse

from pyspark.sql import SparkSession, functions as F

READING_SCHEMA = ("meter_id STRING, meter_type STRING, premise_id STRING, area STRING, ts STRING, "
                  "interval_value DOUBLE, unit STRING, cumulative_value DOUBLE, signal_quality INT")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa", help="data lake root (HDFS path by default)")
    p.add_argument("--history-days", type=int, default=1)
    args = p.parse_args()

    spark = SparkSession.builder.appName("dewa-baseline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.read.option("header", True).schema(READING_SCHEMA)
           .csv(f"{args.base}/raw/readings"))  #dt column comes from the dt=... folders
    days = sorted(r.dt for r in raw.select(F.col("dt").cast("string").alias("dt")).distinct().collect())
    history = days[: args.history_days]
    print(f"History used for the baseline: {', '.join(history)}")

    baseline = (raw.where(F.col("dt").cast("string").isin(history) & (F.col("interval_value") >= 0))
                .groupBy("meter_id", "meter_type")
                .agg(F.percentile_approx("interval_value", 0.99).alias("p99_value"),
                     F.avg("interval_value").alias("avg_value"),
                     F.count("*").alias("readings")))

    out = f"{args.base}/curated/meter_baseline"
    baseline.write.mode("overwrite").parquet(out)

    saved = spark.read.parquet(out)
    print(f"Baseline saved to {out}: {saved.count():,} meters")
    saved.orderBy("meter_id").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
