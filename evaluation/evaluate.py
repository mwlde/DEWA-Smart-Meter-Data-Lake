"""
Evaluation: compares the alerts raised by the streaming job with the anomalies
the generator injected (ground truth).

Reports, per anomaly type:
  injected       anomalies in the live part of the stream
  detected       anomalies with at least one matching alert
  detection %    detected / injected
  false alerts   alerts that match no injected anomaly
  delay (sim)    median simulated time from anomaly start to the end of the
                 first alerting window (how long the data took to show it)
  latency (real) median real seconds, for each anomaly's FIRST alert, from the
                 arrival in Kafka of the reading that revealed it to the moment
                 the alert was computed and ready to publish (the processing
                 delay: requirement R2). Alerts from runs before 12 November have
                 no emitted_ts column; for those the start of the micro-batch
                 that raised the alert (alert_ts) is used instead.
  waits          whether the check has to wait for LATER readings before it can
                 fire. The replay squeezes 15 minutes into 1 second, so a check
                 that waits for later readings looks seconds-fast in the replay
                 but is not in real time. Only silent waits (until the watermark
                 passes the end of its window).
  real time      median time from the anomaly starting to its first alert, in
                 real minutes: the simulated delay, plus the watermark for
                 silent (see waits), plus the processing seconds.
  incidents      alerts collapsed to one per meter and type (what the HBase
                 open-alerts table shows an operator)

An alert matches an anomaly when it is for the same meter and type and its
window overlaps the anomaly (with 1 hour of slack after the anomaly ends).

Run inside the container, from /home/hadoop/dewa, after the streaming job:
    spark-submit evaluation/evaluate.py 2>/dev/null
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, functions as F  #noqa: E402

from common.errors import fail, run, step  #noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa")
    p.add_argument("--ground-truth", help="ground truth CSV path (default: {base}/raw/ground_truth)")
    p.add_argument("--stream-start", default="2026-07-02 00:00:00")
    p.add_argument("--watermark-min", type=float, default=30, help="the detection job's watermark, in minutes")
    p.add_argument("--save-to", help="folder for the summary (default: {base}/evaluation/summary)")
    args = p.parse_args()

    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-evaluation")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    gt_path = args.ground_truth or f"{args.base}/raw/ground_truth"
    with step("reading the ground truth and the alerts"):
        try:
            truth = (spark.read.option("header", True).csv(gt_path)
                     .withColumn("start", F.to_timestamp("start_ts", "yyyy-MM-dd'T'HH:mm:ss"))
                     .withColumn("end", F.to_timestamp("end_ts", "yyyy-MM-dd'T'HH:mm:ss"))
                     .where(F.col("start") >= F.to_timestamp(F.lit(args.stream_start))))
            n_truth = truth.count()
        except Exception as e:
            fail("reading the ground truth", f"cannot read {gt_path} ({str(e).splitlines()[0][:200]})",
                 "load it with: bash scripts/load_raw_zone.sh")
        try:
            alerts = spark.read.parquet(f"{args.base}/alerts").drop("source", "batch")
            n_alerts = alerts.count()
        except Exception as e:
            fail("reading the alerts", f"no alerts at {args.base}/alerts ({str(e).splitlines()[0][:200]})",
                 "run the streaming job first (README, section 3)")
        print(f"  {n_truth} injected anomalies in the live stream, {n_alerts} alerts raised")

    #time the alert was ready: emitted_ts from 12 November on, else the micro-batch start
    ready_col = "emitted_ts" if "emitted_ts" in alerts.columns else "alert_ts"
    print(f"  latency measured to: {ready_col}")
    alerts = alerts.withColumn("latency_s", F.col(ready_col).cast("double") - F.col("last_ingest_ts").cast("double"))

    t, a = truth.alias("t"), alerts.alias("a")
    match_cond = ((F.col("t.meter_id") == F.col("a.meter_id")) &
                  (F.col("t.anomaly_type") == F.col("a.alert_type")) &
                  (F.col("a.window_start") <= F.col("t.end") + F.expr("INTERVAL 1 HOUR")) &
                  (F.col("a.window_end") >= F.col("t.start")))

    matched = t.join(a, match_cond, "left")
    #latency of the FIRST alert for each anomaly (the one an operator would act on),
    #not the fastest of its repeats
    per_anomaly = (matched.groupBy("t.anomaly_id", "t.meter_id", "t.anomaly_type", "t.start")
                   .agg(F.count("a.alert_type").alias("alerts"),
                        F.min("a.window_end").alias("first_alert_window_end"),
                        F.expr(f"min_by(a.latency_s, a.{ready_col})").alias("latency_s"))
                   .withColumn("detected", F.col("alerts") > 0)
                   .withColumn("delay_min", (F.col("first_alert_window_end").cast("long") -
                                             F.col("t.start").cast("long")) / 60))

    #real time from the anomaly starting to its first alert: the simulated delay is real
    #time (one simulated minute is one minute in production); silent also waits for the
    #watermark to pass the end of its window, which the squeezed replay hides
    waits = F.col("anomaly_type") == "silent"
    per_anomaly = per_anomaly.withColumn(
        "real_min", F.col("delay_min") + F.when(waits, F.lit(args.watermark_min)).otherwise(0) +
        F.col("latency_s") / 60)

    summary = (per_anomaly.groupBy(F.col("anomaly_type").alias("type"))
               .agg(F.count("*").alias("injected"),
                    F.sum(F.col("detected").cast("int")).alias("detected"),
                    F.expr("percentile_approx(delay_min, 0.5)").alias("delay_sim_min"),
                    F.expr("percentile_approx(latency_s, 0.5)").alias("latency_real_s"),
                    F.expr("percentile_approx(real_min, 0.5)").alias("real_min")))

    false_alerts = (a.join(t, match_cond, "left_anti")
                    .groupBy(F.col("alert_type").alias("type")).agg(F.count("*").alias("false_alerts")))

    #latency over every alert record, not only each anomaly's first: a window check raises
    #many records per anomaly, so this median rests on far more samples (used by the
    #replay-speed test, where a difference of one interval has to be told from noise)
    per_record = (alerts.groupBy(F.col("alert_type").alias("type"))
                  .agg(F.count("*").alias("alert_records"),
                       F.expr("percentile_approx(latency_s, 0.5)").alias("latency_all_s")))

    report = (summary.join(false_alerts, "type", "full_outer")
              .fillna(0, ["injected", "detected", "false_alerts"])
              .withColumn("detection_pct", F.round(100 * F.col("detected") / F.col("injected"), 1))
              .withColumn("waits", F.when(F.col("type") == "silent", "yes").otherwise("no"))
              .select("type", "injected", "detected", "detection_pct", "false_alerts", "waits",
                      F.round("delay_sim_min", 0).alias("median_delay_sim_min"),
                      F.round("latency_real_s", 2).alias("median_latency_real_s"),
                      F.round("real_min", 0).alias("median_real_time_min"))
              .orderBy("type"))

    print("\nDetection results (live part of the stream)")
    report.show(truncate=False)
    print("waits: the check needs later readings before it can fire (silent: until the "
          f"{args.watermark_min:g}-minute watermark passes the end of its window).")
    print("median_real_time_min: real time from the anomaly starting to its first alert.")

    print("\nLatency over every alert record (each window that fired, not only the first per anomaly)")
    per_record.select("type", "alert_records", F.round("latency_all_s", 2).alias("median_latency_all_s")) \
        .orderBy("type").show(truncate=False)

    incidents = alerts.select("meter_id", "alert_type").distinct().count()
    print(f"\n{n_alerts:,} alert records (one per window), {incidents:,} incidents "
          f"(one per meter and type, as in the HBase open-alerts table) for {n_truth} injected anomalies")
    dupes = n_alerts - alerts.select("alert_type", "meter_id", "window_start").distinct().count()
    print(f"Repeated alert records for the same window: {dupes:,}")

    #empty premises (generator --vacant-rate): normal behaviour that looks like a stuck water meter
    try:
        vacant = spark.read.option("header", True).csv(f"{args.base}/raw/vacancies")
        n_vacant = vacant.count()
        v = (alerts.join(vacant.select("water_meter_id", "electricity_meter_id",
                                       F.to_timestamp("start_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("v_start"),
                                       F.to_timestamp("end_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("v_end")),
                         ((F.col("meter_id") == F.col("water_meter_id")) |
                          (F.col("meter_id") == F.col("electricity_meter_id"))) &
                         (F.col("window_end") >= F.col("v_start")) & (F.col("window_start") <= F.col("v_end"))))
        print(f"Empty premises in the data: {n_vacant:,}; alerts raised on them while empty: {v.count():,}")
    except Exception:
        print("Empty premises in the data: none listed (generated with --vacant-rate 0, or an older dataset)")

    missed = (per_anomaly.where(~F.col("detected"))
              .select("anomaly_id", "meter_id", "anomaly_type", "start").orderBy("start"))
    print("Missed anomalies:")
    missed.show(50, truncate=False)

    out = args.save_to or f"{args.base}/evaluation/summary"
    with step("saving the summary"):
        (report.join(per_record.select("type", "alert_records",
                                       F.round("latency_all_s", 2).alias("median_latency_all_s")), "type", "left")
         .coalesce(1).write.mode("overwrite").option("header", True).csv(out))
    print(f"Summary saved to {out}")
    spark.stop()


if __name__ == "__main__":
    run(main)
