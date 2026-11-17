"""
speed layer: near-real-time anomaly detection on the live meter readings.

reads readings from kafka (topic meter-readings), and runs five checks with
spark structured streaming:

  leak      water meter with continuous flow in the quiet night hours
            (01:00 to 04:00): every reading in a 1-hour window above 0.5 gallons.
            this is "minimum night flow" analysis, the standard method utilities
            use: at night a healthy home uses almost nothing.
  spike     one reading more than 4 times the meter's normal maximum (its 99th
            percentile from the batch layer, curated/meter_baseline)
  negative  a negative reading (meter fault)
  stuck     electricity meter: zero for 6 hours (even an empty home draws power).
            water meter: zero for 24 hours while the premise's electricity shows
            it is occupied. a water meter reading zero is normal in an empty home,
            so water alone cannot tell a stuck meter from a vacant one.
  silent    readings missing: fewer than 4 readings in a 1-hour window

when alerts fire. leak and stuck run in update mode: a window raises its alert
in the micro-batch that brings its last qualifying reading, without waiting for
any later reading. spike and negative are checked reading by reading. silent is
different by nature: a missing reading only shows once later readings have
arrived, so it runs in append mode and fires when the watermark passes the end
of its window. the watermark (30 minutes) is how late a reading may arrive and
still be counted; it delays only the silent alerts.

outputs:
  alerts zone (parquet)      every alert, for evaluation and reporting
  kafka topic "alerts"       with --kafka-alerts, for an operations dashboard
  hbase (serving layer)      with --hbase: every reading in meter_readings
                             (row key: meter id + reversed timestamp, newest first)
                             and the open alerts in meter_alerts (row key: meter id
                             + alert type; repeats of the same alert become versions)
  landing zone (parquet)     a copy of every reading

before starting it checks kafka, the baseline and (with --hbase) hbase, and
stops with a clear message if one is missing. it stops by itself 30 seconds
after the readings stop arriving.

run inside the container, from /home/hadoop/dewa (see README for the full steps):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
        streaming/detect_stream.py --kafka-alerts --hbase

test without kafka (reads csv files written by replay.py --to-dir):
    spark-submit streaming/detect_stream.py --source files --input /tmp/stream_in --once
"""

import argparse
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, functions as F  #noqa: E402
from pyspark.sql.streaming import StreamingQueryListener  #noqa: E402

from common.errors import banner, fail, run, step  #noqa: E402
import hbase_rest  #noqa: E402

READING_DDL = ("meter_id STRING, meter_type STRING, premise_id STRING, area STRING, ts STRING, "
               "interval_value DOUBLE, unit STRING, cumulative_value DOUBLE, signal_quality INT")

#detection thresholds (state these in the report)
LEAK_MIN_FLOW = 0.5          #gallons per 15 mins, every reading in the window
NIGHT_HOURS = (1, 3)         #readings from 01:00 to 03:45
SPIKE_FACTOR = 4.0           #times the meter's 99th percentile (3.0 gave false alerts on busy days)
STUCK_HOURS = 6              #electricity meters
STUCK_WATER_HOURS = 24       #water meters (an empty home reads zero for days)
OCCUPIED_MIN = 0.6           #premise electricity over the day, as a share of its normal: below this it looks empty
WATERMARK = "30 minutes"     #how late a reading may arrive and still be counted (event time)

#how each check outputs its alerts: update = as soon as the window qualifies,
#append = only when the watermark has passed the end of the window
OUTPUT_MODE = {"leak": "update", "stuck": "update", "stuck_water": "update", "silent": "append", "point": "append"}

ALERT_COLUMNS = ["alert_type", "meter_id", "meter_type", "area", "window_start", "window_end",
                 "value", "detail", "last_ingest_ts", "alert_ts"]
#emitted_ts: when the micro-batch had finished computing the alert, just before it is written
#out. alert_ts is when that micro-batch started. latency = emitted_ts - last_ingest_ts
OUTPUT_COLUMNS = ALERT_COLUMNS + ["emitted_ts"]

#queries run in parallel threads: the lock stops their console output from interleaving
PRINT_LOCK = threading.Lock()


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
    p.add_argument("--hbase", action="store_true", help="also write readings and alerts to HBase")
    p.add_argument("--hbase-url", default="http://localhost:8080", help="HBase REST server")
    p.add_argument("--trigger", default="2 seconds", help="micro-batch interval")
    p.add_argument("--once", action="store_true", help="process what is available, then stop (testing)")
    p.add_argument("--idle-stop", type=int, default=30,
                   help="stop by itself after this many seconds without new readings (0 = never)")
    p.add_argument("--quiet", action="store_true", help="do not print alerts to the console")
    p.add_argument("--watermark", default=WATERMARK, help="how late a reading may arrive and still be counted")
    return p.parse_args()


#---------------------------------------------------------------------- checks

def preflight(spark, args):
    """stop early, with a clear message, if something the job needs is missing."""
    if args.source == "kafka" or args.kafka_alerts:
        host, port = args.bootstrap.split(":")
        try:
            socket.create_connection((host, int(port)), timeout=5).close()
        except OSError as e:
            fail("pre-flight: Kafka", f"Kafka is not reachable at {args.bootstrap} ({e})",
                 "start it with: bash scripts/start_kafka.sh")
    if args.source == "files" and not args.input:
        fail("pre-flight: input", "--source files needs --input FOLDER")

    baseline_path = f"{args.base}/curated/meter_baseline"
    try:
        n, reason = spark.read.parquet(baseline_path).count(), "it is empty"
    except Exception as e:
        n, reason = 0, str(e).splitlines()[0][:200]
    if n == 0:
        fail("pre-flight: baseline", f"no meter baseline at {baseline_path}: {reason} (the spike check needs it)",
             "build it with: spark-submit streaming/baseline.py (if HDFS is down: docker restart hadoop)")
    print(f"  baseline found: {n:,} meters")

    if args.hbase:
        err = hbase_rest.check(args.hbase_url)
        if err:
            fail("pre-flight: HBase", f"HBase REST server not reachable at {args.hbase_url} ({err})",
                 "start it with: bash scripts/start_hbase.sh")
        for table in ("meter_readings", "meter_alerts"):
            if not hbase_rest.table_exists(args.hbase_url, table):
                fail("pre-flight: HBase", f"HBase table '{table}' does not exist",
                     "create it with: bash scripts/start_hbase.sh")
        print("  HBase REST server and tables found")


#---------------------------------------------------------------------- stream

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
        #arrival time = when replay.py wrote the file, so latency is measured as with kafka
        parsed = (spark.readStream.schema(READING_DDL)
                  .csv(args.input)
                  .select("*", F.col("_metadata.file_modification_time").alias("ingest_ts")))

    #no watermark here: each check sets its own (the landing copy and the point
    #checks keep no state, so they need none)
    return (parsed
            .withColumn("event_ts", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))
            .withColumn("dt", F.to_date("event_ts"))
            .where(F.col("event_ts").isNotNull()))


def window_alert(df, alert_type, value_col, detail):
    return df.select(
        F.lit(alert_type).alias("alert_type"), "meter_id", "meter_type", "area",
        F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
        F.col(value_col).cast("double").alias("value"), F.lit(detail).alias("detail"),
        "last_ingest_ts", F.current_timestamp().alias("alert_ts"))


def build_checks(spark, readings, args):
    checks = {}
    keys = ["meter_id", "meter_type", "area"]
    timed = readings.withWatermark("event_ts", args.watermark)
    #a window is complete once its last 15-minute reading has arrived
    complete = F.col("last_event") >= F.col("window.end") - F.expr("INTERVAL 15 MINUTES")

    #leak: minimum night flow on water meters. fires on the 4th reading of the window
    night = timed.where((F.col("meter_type") == "water") & F.hour("event_ts").between(*NIGHT_HOURS))
    leak = (night.groupBy(F.window("event_ts", "1 hour", "15 minutes"), *keys)
            .agg(F.count("*").alias("n"), F.min("interval_value").alias("min_flow"),
                 F.max("ingest_ts").alias("last_ingest_ts"))
            .where((F.col("n") >= 4) & (F.col("min_flow") > LEAK_MIN_FLOW)))
    checks["leak"] = window_alert(leak, "leak", "min_flow",
                                  f"night flow above {LEAK_MIN_FLOW} IG in every reading for 1 hour")

    #stuck, electricity: zero for STUCK_HOURS. an empty home still draws power
    #(fridge, a/c, standby), so exact zero means the meter has stopped
    elec = timed.where(F.col("meter_type") == "electricity")
    stuck = (elec.groupBy(F.window("event_ts", f"{STUCK_HOURS} hours", "1 hour"), *keys)
             .agg(F.count("*").alias("n"), F.max("interval_value").alias("max_value"),
                  F.max("event_ts").alias("last_event"), F.max("ingest_ts").alias("last_ingest_ts"))
             .where((F.col("n") >= STUCK_HOURS * 4 - 4) & (F.col("max_value") == 0) & complete))
    checks["stuck"] = window_alert(stuck, "stuck", "max_value", f"zero consumption for {STUCK_HOURS} hours")

    #stuck, water: zero for STUCK_WATER_HOURS while the premise is occupied. occupancy =
    #the premise's electricity over the same window as a share of its normal (baseline
    #average): about 1 in a lived-in home, far lower in an empty one
    baseline = spark.read.parquet(f"{args.base}/curated/meter_baseline")
    water = F.col("meter_type") == "water"
    premise = (timed.join(F.broadcast(baseline.select("meter_id", "avg_value")), "meter_id", "left")
               .groupBy(F.window("event_ts", f"{STUCK_WATER_HOURS} hours", "1 hour"), "premise_id")
               .agg(F.max(F.when(water, F.col("meter_id"))).alias("meter_id"),
                    F.max(F.when(water, F.col("area"))).alias("area"),
                    F.count(F.when(water, 1)).alias("n"),
                    F.max(F.when(water, F.col("interval_value"))).alias("max_value"),
                    F.max(F.when(water, F.col("event_ts"))).alias("last_event"),
                    F.max(F.when(water, F.col("ingest_ts"))).alias("last_ingest_ts"),
                    F.max(F.when(~water, F.col("event_ts"))).alias("last_elec_event"),
                    F.avg(F.when(~water & (F.col("avg_value") > 0),
                                 F.col("interval_value") / F.col("avg_value"))).alias("occupancy"))
               #complete for both meters, so the alert fires once, with the final occupancy
               .where((F.col("n") >= STUCK_WATER_HOURS * 4 - 4) & (F.col("max_value") == 0) & complete &
                      (F.col("last_elec_event") >= F.col("window.end") - F.expr("INTERVAL 15 MINUTES")) &
                      (F.col("occupancy") >= OCCUPIED_MIN))
               .withColumn("meter_type", F.lit("water")))
    checks["stuck_water"] = window_alert(premise, "stuck", "occupancy",
                                         f"zero water for {STUCK_WATER_HOURS} hours in an occupied premise "
                                         f"(value = electricity as a share of normal)")

    #silent: readings missing (only windows that start inside the live stream).
    #a fully silent hr has no rows at all, so it is the partial windows at the start
    #and end of an outage (1 to 3 readings) that raise the alert. append mode: the
    #count is only final once the watermark has passed the end of the window
    silent = (timed.groupBy(F.window("event_ts", "1 hour", "15 minutes"), *keys)
              .agg(F.count("*").alias("n"), F.max("ingest_ts").alias("last_ingest_ts"))
              .where((F.col("n") < 4) &
                     (F.col("window.start") >= F.to_timestamp(F.lit(args.stream_start)))))
    checks["silent"] = window_alert(silent, "silent", "n", "fewer than 4 readings in 1 hour")

    #negative and spike: checked reading by reading, no window needed
    def point_alert(df, alert_type, detail):
        return df.select(F.lit(alert_type).alias("alert_type"), *keys,
                         F.col("event_ts").alias("window_start"), F.col("event_ts").alias("window_end"),
                         F.col("interval_value").alias("value"), detail.alias("detail"),
                         F.col("ingest_ts").alias("last_ingest_ts"), F.current_timestamp().alias("alert_ts"))

    negative = point_alert(readings.where(F.col("interval_value") < 0), "negative", F.lit("negative reading"))
    spikes = (readings.join(F.broadcast(baseline.select("meter_id", "p99_value")), "meter_id")   #stream-static join
              .where((F.col("p99_value") > 0) &
                     (F.col("interval_value") > SPIKE_FACTOR * F.col("p99_value"))))
    spike = point_alert(spikes, "spike",
                        F.concat(F.lit(f"reading above {SPIKE_FACTOR:g} x normal maximum ("),
                                 F.round("p99_value", 2).cast("string"), F.lit(")")))
    checks["point"] = negative.unionByName(spike)
    return checks


#---------------------------------------------------------------------- sinks

def alert_sink(args, name):
    #one folder per query: queries run in parallel, and two jobs appending to the
    #same folder at once clash over its _temporary files. inside it, one folder per
    #micro-batch, overwritten rather than appended: if the job stops mid-batch, spark
    #runs that batch again with the same batch id on restart, and it is written once,
    #not twice. reading {base}/alerts still returns every alert (source, batch and
    #alert_type come from the folder names)
    alerts_path = f"{args.base}/alerts/source={name}"

    def emit(batch, batch_id):
        batch = batch.select(*ALERT_COLUMNS).persist()
        try:
            n = batch.count()   #runs the micro-batch's computation
            if n:
                out = batch.withColumn("emitted_ts", F.lit(time.time()).cast("timestamp")).persist()
                out.write.mode("overwrite").partitionBy("alert_type").parquet(f"{alerts_path}/batch={batch_id}")
                if args.kafka_alerts:
                    (out.select(F.col("meter_id").alias("key"),
                                F.to_json(F.struct(*OUTPUT_COLUMNS)).alias("value"))
                     .write.format("kafka")
                     .option("kafka.bootstrap.servers", args.bootstrap)
                     .option("topic", "alerts").save())
                if args.hbase:
                    #cell version = the window's start, so each window is one version of the
                    #alert's history, and a batch re-run after a restart overwrites it
                    hbase_rest.put_rows(args.hbase_url, "meter_alerts", [
                        (f"{r.meter_id}#{r.alert_type}",
                         {"a:meter_type": r.meter_type, "a:area": r.area,
                          "a:window_start": r.window_start, "a:window_end": r.window_end,
                          "a:value": r.value, "a:detail": r.detail, "a:alert_ts": r.alert_ts},
                         r.version_ms)
                        for r in out.withColumn("version_ms", F.unix_timestamp("window_start") * 1000).collect()])
                if not args.quiet:
                    with PRINT_LOCK:
                        print(f"\n[{name}] batch {batch_id}: {n} alert(s)")
                        out.select(*ALERT_COLUMNS).orderBy("window_start", "meter_id").show(10, truncate=False)
                out.unpersist()
        except Exception as e:
            banner(f"streaming: writing '{name}' alerts (batch {batch_id})", str(e).splitlines()[0][:500],
                   "check that HDFS, Kafka and HBase are all running (jps), then reset and rerun")
            raise
        finally:
            batch.unpersist()

    return emit


def hbase_readings_sink(args):
    """every reading into hbase: row key = meter id + reversed timestamp, so a
    prefix scan on a meter returns its newest readings first."""
    def emit(batch, batch_id):
        try:
            rows = (batch.select(
                        F.concat("meter_id", F.lit("#"),
                                 F.lpad((F.lit(9999999999) - F.unix_timestamp("event_ts")).cast("string"), 10, "0"))
                        .alias("rk"),
                        F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss").alias("ts"),
                        "interval_value", "unit", "cumulative_value", "meter_type", "area")
                    .collect())
            hbase_rest.put_rows(args.hbase_url, "meter_readings",
                                [(r.rk, {"r:ts": r.ts, "r:value": r.interval_value, "r:unit": r.unit,
                                         "r:cumulative": r.cumulative_value, "r:meter_type": r.meter_type,
                                         "r:area": r.area}) for r in rows])
        except Exception as e:
            banner(f"streaming: writing readings to HBase (batch {batch_id})", str(e).splitlines()[0][:500],
                   "check HBase is running: jps should list HMaster and RESTServer (bash scripts/start_hbase.sh)")
            raise

    return emit


#---------------------------------------------------------------------- main

class LateReadings(StreamingQueryListener):
    """counts readings each check ignored because they arrived later than the
    watermark allows (spark drops them from its windows)."""
    def __init__(self):
        self.dropped = {}

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        n = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        self.dropped[p.name] = self.dropped.get(p.name, 0) + n

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def wait_until_idle(spark, queries, idle_stop):
    """run until no new readings have arrived for idle_stop seconds (after the
    first readings), then stop every query cleanly. idle_stop 0 = run forever."""
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


def main():
    args = parse_args()
    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-anomaly-detection")
                 .config("spark.sql.session.timeZone", "UTC")      #timestamps are dubai local time as written
                 .config("spark.sql.shuffle.partitions", "4")      #small single-machine setup
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    with step("pre-flight checks"):
        preflight(spark, args)

    with step("building the detection queries"):
        readings = read_stream(spark, args)
        checks = build_checks(spark, readings, args)
        trigger = {"availableNow": True} if args.once else {"processingTime": args.trigger}

    with step("starting the streaming queries"):
        queries = [readings.drop("ts").writeStream
                   .format("parquet").partitionBy("dt")
                   .option("path", f"{args.base}/landing/readings")
                   .option("checkpointLocation", f"{args.base}/checkpoints/landing")
                   .outputMode("append").trigger(**trigger).queryName("landing").start()]
        if args.hbase:
            queries.append(readings.writeStream.foreachBatch(hbase_readings_sink(args))
                           .option("checkpointLocation", f"{args.base}/checkpoints/hbase_readings")
                           .outputMode("append").trigger(**trigger).queryName("hbase_readings").start())
        late = LateReadings()
        spark.streams.addListener(late)
        for name, df in checks.items():
            queries.append(df.writeStream
                           .foreachBatch(alert_sink(args, name))
                           .option("checkpointLocation", f"{args.base}/checkpoints/{name}")
                           .outputMode(OUTPUT_MODE[name]).trigger(**trigger).queryName(name).start())
        print(f"  started {len(queries)} queries: " + ", ".join(q.name for q in queries))
        print("  waiting for readings (start the replay in another terminal)")

    with step("streaming"):
        if args.once:
            for q in queries:
                q.awaitTermination()
            print("All available data processed.")
        else:
            wait_until_idle(spark, queries, args.idle_stop)
    time.sleep(1)   #let the last progress events arrive
    print("Readings dropped as later than the watermark (" + args.watermark + "): " +
          ", ".join(f"{q} {late.dropped.get(q, 0):,}" for q in checks))
    spark.stop()


if __name__ == "__main__":
    run(main)
