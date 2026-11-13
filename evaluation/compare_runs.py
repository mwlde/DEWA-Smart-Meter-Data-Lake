"""
Compares two streaming runs on the same data, for the restart test (scripts/restart_test.sh):
run A ran without interruption, run B had its detection job killed mid-stream and
restarted from its checkpoint. If the restart lost or repeated work, it shows here.

  detection  injected, detected and false alerts per check in each run (from the
             summaries evaluation/evaluate.py saved)
  alerts     every alert window (meter, check, window start) each run raised: a window
             in one run only means the restart lost or added an alert
  landing    every reading each run stored in its landing zone, against the raw zone's
             readings for the streamed days: none missing (lost) and none extra (stored twice)

Run inside the container, from /home/hadoop/dewa (restart_test.sh does this):
    spark-submit evaluation/compare_runs.py --a /user/hadoop/dewa_restart_test/a \
        --b /user/hadoop/dewa_restart_test/b 2>/dev/null
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, functions as F  #noqa: E402

from common.errors import fail, step  #noqa: E402

READING_SCHEMA = ("meter_id STRING, meter_type STRING, premise_id STRING, area STRING, ts STRING, "
                  "interval_value DOUBLE, unit STRING, cumulative_value DOUBLE, signal_quality INT")
WINDOW_KEY = ["alert_type", "meter_id", "window_start"]
READING_KEY = ["meter_id", "event_ts", "interval_value"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--a", required=True, help="lake root of the uninterrupted run")
    p.add_argument("--b", required=True, help="lake root of the restarted run")
    p.add_argument("--stream-start", default="2026-07-02 00:00:00", help="first streamed reading (day 1 is history)")
    return p.parse_args()


def read(spark, what, path, reader):
    try:
        return reader(path)
    except Exception as e:
        fail(f"reading {what}", f"cannot read {path} ({str(e).splitlines()[0][:200]})",
             "run the whole test again: bash scripts/restart_test.sh")


def main():
    args = parse_args()
    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-compare-runs")
                 .config("spark.sql.session.timeZone", "UTC")   #same as the streaming job, so timestamps match
                 .config("spark.sql.shuffle.partitions", "8")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    runs = {"A": args.a, "B": args.b}

    with step("reading the two runs"):
        summary = {r: read(spark, f"run {r}'s evaluation", f"{b}/evaluation/summary",
                           lambda p: spark.read.option("header", True).option("inferSchema", True).csv(p))
                   for r, b in runs.items()}
        alerts = {r: read(spark, f"run {r}'s alerts", f"{b}/alerts",
                          lambda p: spark.read.parquet(p).drop("source", "batch"))
                  for r, b in runs.items()}
        #the landing zone is read through its _spark_metadata log, as the batch layer reads it:
        #files from a micro-batch the killed job never committed are not part of it
        landing = {r: read(spark, f"run {r}'s landing zone", f"{b}/landing/readings",
                           lambda p: spark.read.parquet(p).select(*READING_KEY))
                   for r, b in runs.items()}
        raw = read(spark, "the raw zone", f"{args.a}/raw/readings",
                   lambda p: spark.read.option("header", True).schema(READING_SCHEMA).csv(p))
        raw = (raw.withColumn("event_ts", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))
               .where(F.col("event_ts") >= F.to_timestamp(F.lit(args.stream_start)))
               .select(*READING_KEY))

    with step("comparing detection"):
        d = (summary["A"].select("type", "injected", F.col("detected").alias("detected_A"),
                                 F.col("false_alerts").alias("false_alerts_A"))
             .join(summary["B"].select("type", F.col("detected").alias("detected_B"),
                                       F.col("false_alerts").alias("false_alerts_B")), "type", "full_outer")
             .orderBy("type"))
        print("\nDetection: run A (uninterrupted) and run B (killed and restarted)")
        d.show(truncate=False)
        tot = d.agg(*[F.sum(c).alias(c) for c in ["injected", "detected_A", "detected_B",
                                                  "false_alerts_A", "false_alerts_B"]]).first()

    with step("comparing the alert windows"):
        win = {r: a.select(*WINDOW_KEY).distinct().persist() for r, a in alerts.items()}
        only_a = win["A"].subtract(win["B"]).persist()
        only_b = win["B"].subtract(win["A"]).persist()
        per_type = (win["A"].groupBy("alert_type").agg(F.count("*").alias("windows_A"))
                    .join(win["B"].groupBy("alert_type").agg(F.count("*").alias("windows_B")), "alert_type", "full_outer")
                    .join(only_a.groupBy("alert_type").agg(F.count("*").alias("only_in_A")), "alert_type", "left")
                    .join(only_b.groupBy("alert_type").agg(F.count("*").alias("only_in_B")), "alert_type", "left")
                    .fillna(0).orderBy("alert_type"))
        print("Alert windows (meter, check, window start) raised by each run")
        per_type.show(truncate=False)
        n_only_a, n_only_b = only_a.count(), only_b.count()
        records = {r: a.count() for r, a in alerts.items()}
        repeated = {r: records[r] - win[r].count() for r in runs}
        print(f"Alert records: A {records['A']:,}, B {records['B']:,} "
              f"(repeated records for the same window: A {repeated['A']}, B {repeated['B']})")
        if n_only_a or n_only_b:
            print("Windows raised by one run only (up to 20):")
            only_a.withColumn("run", F.lit("A only")).unionByName(only_b.withColumn("run", F.lit("B only"))) \
                .orderBy("window_start").show(20, truncate=False)

    with step("comparing the landing zones with the raw zone"):
        n_raw = raw.count()
        print(f"\nLanding zone against the raw zone ({n_raw:,} readings on the streamed days)")
        land = {}
        for r in runs:
            n = landing[r].count()
            missing = raw.exceptAll(landing[r]).count()   #in the raw zone, never stored: lost
            extra = landing[r].exceptAll(raw).count()     #stored more often than sent: repeated
            land[r] = (n, missing, extra)
            print(f"  run {r}: {n:,} readings stored, {missing:,} missing, {extra:,} extra")

    ok = (n_only_a == 0 and n_only_b == 0 and tot["detected_A"] == tot["detected_B"]
          and tot["false_alerts_B"] == tot["false_alerts_A"] and land["B"][1] == 0 and land["B"][2] == 0)
    print()
    if ok:
        print(f"Result: the restarted run detected the same {tot['detected_B']} of {tot['injected']} anomalies with "
              f"{tot['false_alerts_B']} false alerts, raised exactly the same {win['B'].count():,} alert windows as "
              f"the uninterrupted run, and stored every streamed reading once: nothing lost, nothing repeated.")
    else:
        print("Result: the two runs differ (see the tables above). Check logs/restart_b.log for the restart, "
              "and send the output to the group.")
    spark.stop()


if __name__ == "__main__":
    main()
