"""
Benchmark, Spark batch version: the same night-flow leak rule as the streaming
job, run once over the whole raw zone with DataFrame window functions.

Writes one line per leaking meter and night to {base}/benchmark/{out-name}:
    meter_id,night,leak_windows

Run by scripts/run_benchmark.sh twice: in local mode (Spark inside the container)
and on YARN (like MapReduce and Hive), or alone with:
    spark-submit benchmark/spark_batch.py 2>/dev/null
    spark-submit --master yarn benchmark/spark_batch.py --out-name spark_batch_yarn 2>/dev/null
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, Window, functions as F  #noqa: E402

from common.errors import fail, run, step  #noqa: E402

READING_SCHEMA = ("meter_id STRING, meter_type STRING, premise_id STRING, area STRING, ts STRING, "
                  "interval_value DOUBLE, unit STRING, cumulative_value DOUBLE, signal_quality INT")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa")
    p.add_argument("--out-name", default="spark_batch", help="output folder under {base}/benchmark")
    args = p.parse_args()

    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-benchmark-spark-batch")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    with step("running the leak check"):
        try:
            raw = spark.read.option("header", True).schema(READING_SCHEMA).csv(f"{args.base}/raw/readings")
        except Exception as e:
            fail("running the leak check", f"cannot read {args.base}/raw/readings ({str(e).splitlines()[0][:200]})",
                 "load it with: bash scripts/load_raw_zone.sh")
        night = (raw.where((F.col("meter_type") == "water") & F.substring("ts", 12, 2).isin("01", "02", "03"))
                 .withColumn("night", F.substring("ts", 1, 10)))
        w = Window.partitionBy("meter_id", "night").orderBy("ts").rowsBetween(0, 3)
        fmt = "yyyy-MM-dd'T'HH:mm:ss"
        leaks = (night.withColumn("min_flow", F.min("interval_value").over(w))
                 .withColumn("n", F.count("*").over(w))
                 .withColumn("last_ts", F.max("ts").over(w))
                 .where((F.col("n") == 4) & (F.col("min_flow") > 0.5) &
                        (F.unix_timestamp("last_ts", fmt) - F.unix_timestamp("ts", fmt) == 2700))
                 .groupBy("meter_id", "night").agg(F.count("*").alias("leak_windows")))
        leaks.write.mode("overwrite").csv(f"{args.base}/benchmark/{args.out_name}")
    spark.stop()


if __name__ == "__main__":
    run(main)
