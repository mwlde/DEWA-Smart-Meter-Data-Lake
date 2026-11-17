"""
batch layer: builds each meter's normal range from historical readings.

takes the first day of the raw zone as history (the replay script streams the
later days as "live" data), computes each meter's 99th percentile 15-minute
reading, and saves it to the curated zone. the streaming job joins against this
table to spot spikes: a reading far above what that meter normally does.

this is the batch layer feeding the speed layer (lambda architecture).

run inside the container, from /home/hadoop/dewa:
    spark-submit streaming/baseline.py 2>/dev/null
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, functions as F  #noqa: E402

from common.errors import fail, run, step  #noqa: E402

READING_SCHEMA = ("meter_id STRING, meter_type STRING, premise_id STRING, area STRING, ts STRING, "
                  "interval_value DOUBLE, unit STRING, cumulative_value DOUBLE, signal_quality INT")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa", help="data lake root (HDFS path by default)")
    p.add_argument("--history-days", type=int, default=1)
    args = p.parse_args()

    with step("starting Spark"):
        spark = SparkSession.builder.appName("dewa-baseline").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

    with step("reading the raw zone"):
        try:
            raw = (spark.read.option("header", True).schema(READING_SCHEMA)
                   .csv(f"{args.base}/raw/readings"))  #dt column comes from the dt=... folders
            days = sorted(r.dt for r in raw.select(F.col("dt").cast("string").alias("dt")).distinct().collect())
        except Exception as e:
            fail("reading the raw zone", f"cannot read {args.base}/raw/readings ({str(e).splitlines()[0][:200]})",
                 "load it with: bash scripts/load_raw_zone.sh")
        if not days:
            fail("reading the raw zone", f"{args.base}/raw/readings has no dt=... day folders",
                 "load it with: bash scripts/load_raw_zone.sh")
        history = days[: args.history_days]
        print(f"  history used for the baseline: {', '.join(history)}")

    with step("computing each meter's normal range"):
        baseline = (raw.where(F.col("dt").cast("string").isin(history) & (F.col("interval_value") >= 0))
                    .groupBy("meter_id", "meter_type")
                    .agg(F.percentile_approx("interval_value", 0.99).alias("p99_value"),
                         F.avg("interval_value").alias("avg_value"),
                         F.count("*").alias("readings")))
        out = f"{args.base}/curated/meter_baseline"
        baseline.write.mode("overwrite").parquet(out)

    with step("checking the saved baseline"):
        saved = spark.read.parquet(out)
        n = saved.count()
        if n == 0:
            fail("checking the saved baseline", "the baseline is empty", "check the raw zone has readings")
        print(f"Baseline saved to {out}: {n:,} meters")
        saved.orderBy("meter_id").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    run(main)
