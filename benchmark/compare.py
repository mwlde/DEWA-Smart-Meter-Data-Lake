"""
Benchmark: compares the timed runs and checks every version found the same leaks.

For MapReduce, Hive and Spark batch it reads the timings (logs/benchmark_times.csv,
written by scripts/run_benchmark.sh) and each tool's output, and checks the
leaking meter-nights against the injected leaks (ground truth). For Spark
Streaming it uses the leak alerts and the latency from the last streaming run
(scripts/run_stream.sh), if there is one.

Prints the benchmark table and saves it to logs/benchmark_results.md.

Run by scripts/run_benchmark.sh, or alone with:
    spark-submit benchmark/compare.py 2>/dev/null
"""

import argparse
import csv
import os
import statistics
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, functions as F  #noqa: E402

from common.errors import fail, run, step  #noqa: E402

TOOLS = [("mapreduce", "MapReduce (Hadoop Streaming, Python)"),
         ("hive", "Hive (HiveQL on MapReduce)"),
         ("spark_batch", "Spark batch (DataFrames, local mode)"),
         ("spark_batch_yarn", "Spark batch (DataFrames, on YARN)")]
CODE = {"mapreduce": ["benchmark/leak_mapper.py", "benchmark/leak_reducer.py"],
        "hive": ["benchmark/leak_check.hql"],
        "spark_batch": ["benchmark/spark_batch.py"],
        "spark_batch_yarn": ["benchmark/spark_batch.py"]}
FMT = "%Y-%m-%dT%H:%M:%S"


def code_lines(paths):
    """Lines of code without blank lines, comments and docstrings (rough, for comparison)."""
    n = 0
    for p in paths:
        in_doc = False
        for line in open(p):
            s = line.strip()
            quotes = s.count('"""')
            if in_doc:
                in_doc = quotes % 2 == 0   #the docstring ends on the line with its closing quotes
                continue
            if s.startswith('"""'):
                in_doc = quotes == 1       #one-line docstrings open and close on the same line
                continue
            if not s or s.startswith("#") or s.startswith("--"):
                continue
            n += 1
    return n


def expected_nights(truth_rows, days):
    """Meter-nights where an injected leak covers a full hour between 01:00 and 03:45."""
    out = set()
    for meter, start, end in truth_rows:
        s, e = datetime.strptime(start, FMT), datetime.strptime(end, FMT)
        for d in days:
            first = max(s, datetime.strptime(f"{d}T01:00:00", FMT))
            last = min(e, datetime.strptime(f"{d}T03:45:00", FMT))
            if last - first >= timedelta(minutes=45):
                out.add((meter, d))
    return out


def read_found(spark, path):
    rows = spark.read.text(path).collect()
    found = set()
    for r in rows:
        parts = [x.strip() for x in r.value.split(",")]
        if len(parts) >= 2 and parts[0]:
            found.add((parts[0], parts[1]))
    return found


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa")
    p.add_argument("--times", default="logs/benchmark_times.csv")
    p.add_argument("--out", default="logs/benchmark_results.md")
    p.add_argument("--no-streaming", action="store_true",
                   help="leave out the streaming row (when the stream did not run on this data)")
    args = p.parse_args()

    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-benchmark-compare")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    with step("reading the timings and the ground truth"):
        if not os.path.exists(args.times):
            fail("reading the timings", f"no timings file at {args.times}", "run: bash scripts/run_benchmark.sh")
        times = {}
        for r in csv.DictReader(open(args.times)):
            times.setdefault(r["tool"], []).append(float(r["seconds"]))
        truth = [(r.meter_id, r.start_ts, r.end_ts) for r in
                 spark.read.option("header", True).csv(f"{args.base}/raw/ground_truth")
                 .where(F.col("anomaly_type") == "leak").collect()]
        path = spark._jvm.org.apache.hadoop.fs.Path(f"{args.base}/raw/readings")
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        days = sorted(s.getPath().getName()[3:] for s in fs.listStatus(path)
                      if s.getPath().getName().startswith("dt="))
        expected = expected_nights(truth, days)
        print(f"  {len(truth)} injected leaks, {len(days)} days: {len(expected)} leaking meter-nights expected")

    rows = []
    with step("checking each tool's leaks"):
        for tool, label in TOOLS:
            if tool not in times:
                print(f"  {label}: not run, skipped")
                continue
            try:
                found = read_found(spark, f"{args.base}/benchmark/{tool}")
            except Exception as e:
                fail(f"checking {tool}", f"no output at {args.base}/benchmark/{tool} ({str(e).splitlines()[0][:200]})",
                     "rerun: bash scripts/run_benchmark.sh")
            runtime = statistics.median(times[tool])
            rows.append([label, f"{runtime:.1f} s", f"{min(times[tool]):.1f} to {max(times[tool]):.1f} s",
                         str(len(found)), str(len(found & expected)), str(len(expected - found)),
                         str(len(found - expected)), f"about 2 h + {runtime:.0f} s",
                         str(code_lines(CODE[tool]))])

        #spark streaming: the leak alerts and latency of the last streaming run, if any
        try:
            if args.no_streaming:
                raise LookupError("left out (--no-streaming)")
            alerts = (spark.read.parquet(f"{args.base}/alerts").where(F.col("alert_type") == "leak")
                      .select("meter_id", F.date_format("window_start", "yyyy-MM-dd").alias("night"))
                      .distinct().collect())
            found = {(r.meter_id, r.night) for r in alerts}
            live = {(m, d) for m, d in expected if d != days[0]}   #day 1 is history, not streamed
            latency = "n/a"
            try:
                summ = (spark.read.option("header", True).csv(f"{args.base}/evaluation/summary")
                        .where(F.col("type") == "leak").collect())
                if summ:
                    latency = f"{float(summ[0].median_latency_real_s):.1f} s"
            except Exception:
                pass
            rows.append(["Spark Structured Streaming", "continuous", "-", str(len(found)),
                         str(len(found & live)), str(len(live - found)), str(len(found - live)),
                         latency, f"{code_lines(['streaming/detect_stream.py'])} (all 5 checks)"])
        except LookupError as e:
            print(f"  Spark Streaming: {e}")
        except Exception:
            print("  Spark Streaming: no alerts in HDFS (run bash scripts/run_stream.sh first), row skipped")

    head = ["Approach", "Runtime (median)", "Range", "Leak nights found", "Correct", "Missed",
            "Extra", "Time to alert", "Lines of code"]
    widths = [max(len(r[i]) for r in [head] + rows) for i in range(len(head))]
    print("\nBenchmark: night-flow leak check on the raw zone\n")
    for k, r in enumerate([head] + rows):
        print("  " + "  ".join(v.ljust(w) for v, w in zip(r, widths)))
        if k == 0:
            print("  " + "  ".join("-" * w for w in widths))
    print("\n  Runtime: wall-clock time of the whole command, including start-up, median of "
          f"{max(len(v) for v in times.values())} runs.")
    print("  Time to alert, batch tools: a leak first shows when its first 1-hour night window closes")
    print("  (02:00 at the earliest); a nightly batch can only run after the night window ends (04:00),")
    print("  so the alert comes about 2 hours plus the runtime later. Streaming: median seconds from the")
    print("  revealing reading's arrival to the alert (scripts/run_stream.sh).")
    print("  MapReduce, Hive and Spark on YARN all start their jobs through YARN; Spark local mode runs")
    print("  inside the container without YARN, so it skips that start-up cost.")
    print("  Spark Streaming is checked against the live days only (day 1 is history).")

    with step("saving the table"):
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            f.write("| " + " | ".join(head) + " |\n|" + "---|" * len(head) + "\n")
            for r in rows:
                f.write("| " + " | ".join(r) + " |\n")
        print(f"Table saved to {args.out}")
    spark.stop()


if __name__ == "__main__":
    run(main)
