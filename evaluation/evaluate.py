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
  latency (real) median real seconds from the last reading's arrival to the
                 alert (the processing delay: requirement R2)

An alert matches an anomaly when it is for the same meter and type and its
window overlaps the anomaly (with 1 hour of slack after the anomaly ends).

Run inside the container, from /home/hadoop/dewa, after the streaming job:
    spark-submit evaluation/evaluate.py
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa")
    p.add_argument("--ground-truth", help="ground truth CSV path (default: {base}/raw/ground_truth)")
    p.add_argument("--stream-start", default="2026-07-02 00:00:00")
    args = p.parse_args()

    spark = (SparkSession.builder.appName("dewa-evaluation")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    gt_path = args.ground_truth or f"{args.base}/raw/ground_truth"
    truth = (spark.read.option("header", True).csv(gt_path)
             .withColumn("start", F.to_timestamp("start_ts", "yyyy-MM-dd'T'HH:mm:ss"))
             .withColumn("end", F.to_timestamp("end_ts", "yyyy-MM-dd'T'HH:mm:ss"))
             .where(F.col("start") >= F.to_timestamp(F.lit(args.stream_start))))

    alerts = spark.read.parquet(f"{args.base}/alerts").drop("source")

    t, a = truth.alias("t"), alerts.alias("a")
    match_cond = ((F.col("t.meter_id") == F.col("a.meter_id")) &
                  (F.col("t.anomaly_type") == F.col("a.alert_type")) &
                  (F.col("a.window_start") <= F.col("t.end") + F.expr("INTERVAL 1 HOUR")) &
                  (F.col("a.window_end") >= F.col("t.start")))

    matched = t.join(a, match_cond, "left")
    per_anomaly = (matched.groupBy("t.anomaly_id", "t.anomaly_type", "t.start")
                   .agg(F.count("a.alert_type").alias("alerts"),
                        F.min("a.window_end").alias("first_alert_window_end"),
                        F.min(F.col("a.alert_ts").cast("double") - F.col("a.last_ingest_ts").cast("double"))
                        .alias("latency_s"))
                   .withColumn("detected", F.col("alerts") > 0)
                   .withColumn("delay_min", (F.col("first_alert_window_end").cast("long") -
                                             F.col("t.start").cast("long")) / 60))

    summary = (per_anomaly.groupBy(F.col("anomaly_type").alias("type"))
               .agg(F.count("*").alias("injected"),
                    F.sum(F.col("detected").cast("int")).alias("detected"),
                    F.expr("percentile_approx(delay_min, 0.5)").alias("delay_sim_min"),
                    F.expr("percentile_approx(latency_s, 0.5)").alias("latency_real_s")))

    false_alerts = (a.join(t, match_cond, "left_anti")
                    .groupBy(F.col("alert_type").alias("type")).agg(F.count("*").alias("false_alerts")))

    report = (summary.join(false_alerts, "type", "full_outer")
              .fillna(0, ["injected", "detected", "false_alerts"])
              .withColumn("detection_pct", F.round(100 * F.col("detected") / F.col("injected"), 1))
              .select("type", "injected", "detected", "detection_pct", "false_alerts",
                      F.round("delay_sim_min", 0).alias("median_delay_sim_min"),
                      F.round("latency_real_s", 2).alias("median_latency_real_s"))
              .orderBy("type"))

    print("\nDetection results (live part of the stream)")
    report.show(truncate=False)

    missed = per_anomaly.where(~F.col("detected")).select("anomaly_id", "anomaly_type", "start").orderBy("start")
    print("Missed anomalies:")
    missed.show(50, truncate=False)

    report.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.base}/evaluation/summary")
    print(f"Summary saved to {args.base}/evaluation/summary")
    spark.stop()


if __name__ == "__main__":
    main()
