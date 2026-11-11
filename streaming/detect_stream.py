"""
Speed layer: near-real-time anomaly detection on the live meter readings.

Reads readings from Kafka (topic meter-readings), and runs five checks with
Spark Structured Streaming:

  leak      water meter with continuous flow in the quiet night hours
            (01:00 to 04:00): every reading in a 1-hour window above 0.5 gallons.
            This is "minimum night flow" analysis, the standard method utilities
            use: at night a healthy home uses almost nothing.
  spike     one reading more than 4 times the meter's normal maximum (its 99th
            percentile from the batch layer, curated/meter_baseline)
  negative  a negative reading (meter fault)
  stuck     no consumption at all for 6 hours (meter stopped recording)
  silent    readings missing: fewer than 4 readings in a 1-hour window

Every alert is written to the data lake (alerts zone, Parquet) and, with
--kafka-alerts, to the Kafka topic "alerts" for an operations dashboard.
A copy of every reading is also landed in the lake (landing zone, Parquet).

Run inside the container, from /home/hadoop/dewa (see README for the full steps):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
        streaming/detect_stream.py --kafka-alerts

Test without Kafka (reads CSV files written by replay.py --to-dir):
    spark-submit streaming/detect_stream.py --source files --input /tmp/stream_in --once
"""

import argparse

from pyspark.sql import SparkSession, functions as F

READING_DDL = ("meter_id STRING, meter_type STRING, premise_id STRING, area STRING, ts STRING, "
               "interval_value DOUBLE, unit STRING, cumulative_value DOUBLE, signal_quality INT")

# Detection thresholds (state these in the report)
LEAK_MIN_FLOW = 0.5          # gallons per 15 minutes, every reading in the window
NIGHT_HOURS = (1, 3)         # readings from 01:00 to 03:45
SPIKE_FACTOR = 4.0           # times the meter's 99th percentile (3.0 gave false alerts on busy days)
STUCK_HOURS = 6
WATERMARK = "15 minutes"     # how late a reading may arrive (event time)

ALERT_COLUMNS = ["alert_type", "meter_id", "meter_type", "area", "window_start", "window_end",
                 "value", "detail", "last_ingest_ts", "alert_ts"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="meter-readings")
    p.add_argument("--input", help="folder of CSV files (with --source files)")
    p.add_argument("--base", default="/user/hadoop/dewa", help="data lake root")
    p.add_argument("--stream-start", default="2026-07-02 00:00:00",
                   help="first timestamp of the live stream (silent check ignores windows before it)")
    p.add_argument("--kafka-alerts", action="store_true", help="also publish alerts to the Kafka topic 'alerts'")
    p.add_argument("--trigger", default="2 seconds", help="micro-batch interval")
    p.add_argument("--once", action="store_true", help="process what is available, then stop (testing)")
    p.add_argument("--idle-stop", type=int, default=30,
                   help="stop by itself after this many seconds without new readings (0 = never)")
    p.add_argument("--quiet", action="store_true", help="do not print alerts to the console")
    return p.parse_args()


def read_stream(spark, args):
    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", args.topic)
               .option("startingOffsets", "earliest")
               .load())
        parsed = (raw.select(F.from_csv(F.col("value").cast("string"), READING_DDL).alias("r"),
                             F.col("timestamp").alias("ingest_ts"))
                  .select("r.*", "ingest_ts"))
    else:
        parsed = (spark.readStream.schema(READING_DDL)
                  .option("maxFilesPerTrigger", 4)          # one simulated hour per micro-batch
                  .csv(args.input)
                  .withColumn("ingest_ts", F.current_timestamp()))

    return (parsed
            .withColumn("event_ts", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))
            .withColumn("dt", F.to_date("event_ts"))
            .where(F.col("event_ts").isNotNull())
            .withWatermark("event_ts", WATERMARK))


def window_alert(df, alert_type, value_col, detail):
    return df.select(
        F.lit(alert_type).alias("alert_type"), "meter_id", "meter_type", "area",
        F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
        F.col(value_col).cast("double").alias("value"), F.lit(detail).alias("detail"),
        "last_ingest_ts", F.current_timestamp().alias("alert_ts"))


def build_checks(spark, readings, args):
    checks = {}
    keys = ["meter_id", "meter_type", "area"]

    # Leak: minimum night flow on water meters
    night = readings.where((F.col("meter_type") == "water") &
                           F.hour("event_ts").between(*NIGHT_HOURS))
    leak = (night.groupBy(F.window("event_ts", "1 hour", "15 minutes"), *keys)
            .agg(F.count("*").alias("n"), F.min("interval_value").alias("min_flow"),
                 F.max("ingest_ts").alias("last_ingest_ts"))
            .where((F.col("n") >= 4) & (F.col("min_flow") > LEAK_MIN_FLOW)))
    checks["leak"] = window_alert(leak, "leak", "min_flow",
                                  f"night flow above {LEAK_MIN_FLOW} IG in every reading for 1 hour")

    # Stuck: no consumption for STUCK_HOURS
    stuck = (readings.groupBy(F.window("event_ts", f"{STUCK_HOURS} hours", "1 hour"), *keys)
             .agg(F.count("*").alias("n"), F.max("interval_value").alias("max_value"),
                  F.max("ingest_ts").alias("last_ingest_ts"))
             .where((F.col("n") >= STUCK_HOURS * 4 - 4) & (F.col("max_value") == 0)))
    checks["stuck"] = window_alert(stuck, "stuck", "max_value", f"zero consumption for {STUCK_HOURS} hours")

    # Silent: readings missing (only windows that start inside the live stream)
    silent = (readings.groupBy(F.window("event_ts", "1 hour", "15 minutes"), *keys)
              .agg(F.count("*").alias("n"), F.max("ingest_ts").alias("last_ingest_ts"))
              .where((F.col("n") < 4) &
                     (F.col("window.start") >= F.to_timestamp(F.lit(args.stream_start)))))
    checks["silent"] = window_alert(silent, "silent", "n", "fewer than 4 readings in 1 hour")

    # Negative and spike: checked reading by reading, no window needed
    def point_alert(df, alert_type, detail):
        return df.select(F.lit(alert_type).alias("alert_type"), *keys,
                         F.col("event_ts").alias("window_start"), F.col("event_ts").alias("window_end"),
                         F.col("interval_value").alias("value"), detail.alias("detail"),
                         F.col("ingest_ts").alias("last_ingest_ts"), F.current_timestamp().alias("alert_ts"))

    negative = point_alert(readings.where(F.col("interval_value") < 0), "negative", F.lit("negative reading"))

    baseline_path = f"{args.base}/curated/meter_baseline"
    try:
        baseline = spark.read.parquet(baseline_path).select("meter_id", "p99_value")
        spikes = (readings.join(F.broadcast(baseline), "meter_id")   # stream-static join
                  .where((F.col("p99_value") > 0) &
                         (F.col("interval_value") > SPIKE_FACTOR * F.col("p99_value"))))
        spike = point_alert(spikes, "spike",
                            F.concat(F.lit("reading above 4 x normal maximum ("),
                                     F.round("p99_value", 2).cast("string"), F.lit(")")))
        checks["point"] = negative.unionByName(spike)
    except Exception:
        print(f"WARNING: no baseline at {baseline_path}; spike check off. Run streaming/baseline.py first.")
        checks["point"] = negative

    return checks


def make_sink(args, name):
    # One folder per query: queries run in parallel, and two jobs appending to the
    # same folder at once clash over its _temporary files. Reading {base}/alerts
    # still returns every alert (source and alert_type come from the folder names).
    alerts_path = f"{args.base}/alerts/source={name}"

    def emit(batch, batch_id):
        batch = batch.select(*ALERT_COLUMNS).persist()
        n = batch.count()
        if n:
            batch.write.mode("append").partitionBy("alert_type").parquet(alerts_path)
            if args.kafka_alerts:
                (batch.select(F.col("meter_id").alias("key"),
                              F.to_json(F.struct(*ALERT_COLUMNS)).alias("value"))
                 .write.format("kafka")
                 .option("kafka.bootstrap.servers", args.bootstrap)
                 .option("topic", "alerts").save())
            if not args.quiet:
                print(f"\n[{name}] batch {batch_id}: {n} alert(s)")
                batch.orderBy("window_start", "meter_id").show(10, truncate=False)
        batch.unpersist()

    return emit


def main():
    args = parse_args()
    spark = (SparkSession.builder.appName("dewa-anomaly-detection")
             .config("spark.sql.session.timeZone", "UTC")      # timestamps are Dubai local time as written
             .config("spark.sql.shuffle.partitions", "4")      # small single-machine setup
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    readings = read_stream(spark, args)
    checks = build_checks(spark, readings, args)
    trigger = {"availableNow": True} if args.once else {"processingTime": args.trigger}

    queries = []
    # Land every reading in the lake
    queries.append(readings.drop("ts").writeStream
                   .format("parquet").partitionBy("dt")
                   .option("path", f"{args.base}/landing/readings")
                   .option("checkpointLocation", f"{args.base}/checkpoints/landing")
                   .outputMode("append").trigger(**trigger).queryName("landing").start())

    for name, df in checks.items():
        queries.append(df.writeStream
                       .foreachBatch(make_sink(args, name))
                       .option("checkpointLocation", f"{args.base}/checkpoints/{name}")
                       .outputMode("append").trigger(**trigger).queryName(name).start())

    print(f"Started {len(queries)} streaming queries: " + ", ".join(q.name for q in queries))
    if args.once:
        for q in queries:
            q.awaitTermination()
        print("All available data processed.")
    else:
        wait_until_idle(spark, queries, args.idle_stop)


def wait_until_idle(spark, queries, idle_stop):
    """Run until no new readings have arrived for idle_stop seconds (after the
    first readings), then stop every query cleanly. idle_stop 0 = run forever."""
    import time
    seen_data, last_data = False, time.time()
    while not spark.streams.awaitAnyTermination(5):
        if idle_stop <= 0:
            continue
        if any((q.lastProgress or {}).get("numInputRows", 0) > 0 for q in queries):
            seen_data, last_data = True, time.time()
        elif seen_data and time.time() - last_data >= idle_stop:
            print(f"\nNo new readings for {idle_stop} seconds: stopping.")
            break
    for q in queries:
        q.stop()
    print("All queries stopped.")
    spark.stop()


if __name__ == "__main__":
    main()
