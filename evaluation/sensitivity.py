"""
Sensitivity test: how small a leak can the night-flow rule catch, and what does a
lower alarm threshold cost in false alerts?

For each leak size range it generates a fresh dataset (same meters, same anomaly
times, only the leak sizes differ), then runs the leak rule at several thresholds.
The rule is the one in benchmark/spark_batch.py, which found exactly the same leak
nights as the streaming job in both benchmarks, so this is a fast batch run of
the same check rather than one streaming run per setting.

Reported for every leak size and threshold:
  detection    share of leaks flagged on at least one night after they started
  nights       share of the nights a leak was running that were flagged
and for every threshold:
  false alerts meter-nights flagged where no leak was running

Run inside the container, from /home/hadoop/dewa (Hadoop not needed; about 3 to 5 mins):
    spark-submit evaluation/sensitivity.py 2>/dev/null
The datasets go to /tmp/dewa_sensitivity (local disk, deleted at the end); the table
is printed and saved to logs/sensitivity_results.md.
"""

import argparse
import csv
import os
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)  #repo root

from pyspark.sql import SparkSession, functions as F  #noqa: E402

from benchmark.compare import expected_nights  #noqa: E402
from benchmark.spark_batch import READING_SCHEMA, leak_nights  #noqa: E402
from common.errors import fail, run, step  #noqa: E402

#leak sizes in gallons per 15 mins (the normal runs use 1 to 6)
LEAK_SIZES = [(0.1, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, 2.0), (1.0, 6.0)]
THRESHOLDS = [0.2, 0.3, 0.5, 1.0]   #0.5 is the threshold the pipeline uses


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--meters", type=int, default=2000)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--anomaly-rate", type=float, default=0.3,
                   help="high, so each run has many leaks (about 150 with the defaults)")
    p.add_argument("--work", default="/tmp/dewa_sensitivity")
    p.add_argument("--out", default="logs/sensitivity_results.md")
    args = p.parse_args()

    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-sensitivity")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "8").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    shutil.rmtree(args.work, ignore_errors=True)
    results, false_alerts, n_leaks = {}, {t: 0 for t in THRESHOLDS}, None
    for lo, hi in LEAK_SIZES:
        label = f"{lo:g} to {hi:g}"
        folder = os.path.join(args.work, f"leak_{lo:g}_{hi:g}")
        with step(f"leaks of {label} gallons per 15 mins: generating the data"):
            cmd = [sys.executable, os.path.join(ROOT, "data_generator", "generate.py"),
                   "--meters", str(args.meters), "--days", str(args.days),
                   "--anomaly-rate", str(args.anomaly_rate),
                   "--leak-min", str(lo), "--leak-max", str(hi), "--out", folder]
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.returncode != 0:
                fail("generating the data", (r.stderr or r.stdout).strip().splitlines()[-1][:300])

        with step(f"leaks of {label} gallons per 15 mins: running the leak rule"):
            truth = [(row["meter_id"], row["start_ts"], row["end_ts"])
                     for row in csv.DictReader(open(os.path.join(folder, "ground_truth.csv")))
                     if row["anomaly_type"] == "leak"]
            days = sorted(d[3:] for d in os.listdir(os.path.join(folder, "readings")) if d.startswith("dt="))
            expected = expected_nights(truth, days)
            leak_meters = {m for m, _, _ in truth}
            n_leaks = len(truth)
            raw = (spark.read.option("header", True).schema(READING_SCHEMA)
                   .csv("file://" + os.path.join(folder, "readings")).cache())
            for t in THRESHOLDS:
                found = {(row.meter_id, row.night) for row in leak_nights(raw, t).collect()}
                caught = {m for m, d in found & expected}
                results[(label, t)] = (100 * len(caught) / len(leak_meters),
                                       100 * len(found & expected) / max(len(expected), 1))
                false_alerts[t] = max(false_alerts[t], len(found - expected))
            raw.unpersist()
        shutil.rmtree(folder, ignore_errors=True)

    head = ["Leak size (gal / 15 min)"] + [f"threshold {t:g}" for t in THRESHOLDS]
    rows = [[label] + [f"{results[(label, t)][0]:.0f}% ({results[(label, t)][1]:.0f}% of nights)"
                       for t in THRESHOLDS]
            for label in [f"{lo:g} to {hi:g}" for lo, hi in LEAK_SIZES]]
    rows.append(["False alerts (meter-nights)"] + [str(false_alerts[t]) for t in THRESHOLDS])
    widths = [max(len(r[i]) for r in [head] + rows) for i in range(len(head))]

    print(f"\nSensitivity: share of leaks detected ({args.meters:,} meters, {args.days} days, "
          f"{n_leaks} leaks per row; the pipeline uses threshold 0.5)\n")
    for k, r in enumerate([head] + rows):
        print("  " + "  ".join(v.ljust(w) for v, w in zip(r, widths)))
        if k == 0 or k == len(rows) - 1:
            print("  " + "  ".join("-" * w for w in widths))
    print("\n  Each cell: leaks flagged on at least one night (and the share of all leak nights flagged).")
    print("  False alerts: meter-nights flagged where no leak was running, out of "
          f"{args.meters // 2 * args.days:,} water meter-nights.")

    with step("saving the table"):
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            f.write("| " + " | ".join(head) + " |\n|" + "---|" * len(head) + "\n")
            for r in rows:
                f.write("| " + " | ".join(r) + " |\n")
        print(f"Table saved to {args.out}")
    shutil.rmtree(args.work, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    run(main)
