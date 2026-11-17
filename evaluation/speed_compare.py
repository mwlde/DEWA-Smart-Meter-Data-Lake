"""
replay-speed test: compares two evaluation summaries made at different replay speeds,
to show which checks wait for later readings and what that means in real time.

the replay squeezes 15 minutes into --interval real seconds. a check that fires on
the reading that reveals the anomaly has the same latency at any replay speed (it is
only processing time). a check that has to wait for later readings gets slower when
the replay slows down, by the number of intervals it waits times the extra seconds
per interval. in production an interval is 15 real minutes, so:

    intervals waited = (latency slow - latency fast) / (interval slow - interval fast)
    real-world wait  = intervals waited x 15 minutes

run by scripts/speed_test.sh, or alone:
    spark-submit evaluation/speed_compare.py --fast /user/hadoop/dewa/speed_test/fast \
        --slow /user/hadoop/dewa/speed_test/slow --fast-interval 1 --slow-interval 4
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession  #noqa: E402

from common.errors import fail, run, step  #noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fast", required=True, help="summary folder of the fast run")
    p.add_argument("--slow", required=True, help="summary folder of the slow run")
    p.add_argument("--fast-interval", type=float, default=1)
    p.add_argument("--slow-interval", type=float, default=4)
    p.add_argument("--out", default="logs/speed_test_results.md")
    args = p.parse_args()

    with step("reading the two summaries"):
        spark = SparkSession.builder.appName("dewa-speed-compare").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        try:
            fast = {r.type: r for r in spark.read.option("header", True).csv(args.fast).collect()}
            slow = {r.type: r for r in spark.read.option("header", True).csv(args.slow).collect()}
        except Exception as e:
            fail("reading the two summaries", str(e).splitlines()[0][:200],
                 "run bash scripts/speed_test.sh (it makes both)")

    step_s = args.slow_interval - args.fast_interval
    lines = [f"| Check | Alert records | Latency at {args.fast_interval:g} s per interval | "
             f"Latency at {args.slow_interval:g} s per interval | Change | Intervals waited | "
             "Waits for later readings | Real time from anomaly start to alert (median) |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for t in sorted(set(fast) & set(slow)):
        #median over every alert record (many samples) when the summary has it; older
        #summaries only have each anomaly's first alert
        col = "median_latency_all_s" if "median_latency_all_s" in slow[t].asDict() else "median_latency_real_s"
        lf, ls = float(fast[t][col]), float(slow[t][col])
        n = slow[t].asDict().get("alert_records", "")
        #a check that waits for k later intervals gets k x step_s slower; processing time
        #does not grow when the replay slows down (it usually shrinks: less load)
        waited = (ls - lf) / step_s
        waits = waited >= 0.5
        real = float(slow[t].median_real_time_min)
        lines.append(f"| {t} | {n} | {lf:.2f} s | {ls:.2f} s | {ls - lf:+.2f} s | {waited:.1f} | "
                     f"{'yes' if waits else 'no'} | {real:.0f} min |")
    text = "\n".join(lines)
    print("\nReplay-speed test\n")
    print(text)
    print(f"\nIntervals waited = change / {step_s:g} extra seconds per interval. A check that fires on the "
          "reading that reveals the anomaly waits 0 intervals, so its replay latency is its real-world "
          "latency. Silent waits by nature: its real-world wait is in the last column (its 15-minute "
          "window plus the 30-minute watermark).")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        f.write("# Replay-speed test\n\n" + text + "\n")
    print(f"Saved to {args.out}")
    spark.stop()


if __name__ == "__main__":
    run(main)
