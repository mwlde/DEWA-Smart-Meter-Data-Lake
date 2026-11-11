"""
Replays the generated meter readings as a live stream.

Readings are sent in time order, one 15-minute interval at a time: all meters'
readings for 00:00, then a pause, then all readings for 00:15, and so on. The
pause sets the speed: with --interval 1, one simulated day passes in 96 seconds.

Day 1 is skipped by default: it is the "history" the batch layer uses to build
each meter's normal range (streaming/baseline.py). Days 2 onwards arrive live.

Each line is printed as  meter_id|reading-as-csv  so kafka-console-producer can
use the meter ID as the message key (all readings of one meter go to the same
Kafka partition, in order).

Usage, inside the container, from /home/hadoop/dewa:
    python3 streaming/replay.py --interval 1 | kafka-console-producer.sh \
        --bootstrap-server localhost:9092 --topic meter-readings \
        --property parse.key=true --property key.separator='|'

Or write to files instead of Kafka (used for testing without Kafka):
    python3 streaming/replay.py --interval 0 --to-dir /tmp/stream_in
"""

import argparse
import csv
import glob
import os
import sys
import time
from collections import defaultdict


def day_files(data_dir, skip_days):
    files = sorted(glob.glob(os.path.join(data_dir, "readings", "dt=*", "readings.csv")))
    return files[skip_days:]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", default="data", help="folder made by generate.py")
    p.add_argument("--interval", type=float, default=1.0, help="real seconds per simulated 15 minutes")
    p.add_argument("--skip-days", type=int, default=1, help="days kept back as history (default 1)")
    p.add_argument("--days", type=int, default=0, help="stop after this many days (0 = all)")
    p.add_argument("--to-dir", help="write one CSV file per interval into this folder instead of stdout")
    args = p.parse_args()

    files = day_files(args.data, args.skip_days)
    if args.days:
        files = files[: args.days]
    if not files:
        sys.exit(f"No reading files found under {args.data}/readings. Run data_generator/generate.py first.")
    if args.to_dir:
        os.makedirs(args.to_dir, exist_ok=True)

    sent = 0
    for path in files:
        #one day is small enough to hold in memory (96 readings x meters)
        by_ts = defaultdict(list)
        with open(path, newline="") as f:
            reader = csv.reader(f)
            next(reader)  #header
            for row in reader:
                by_ts[row[4]].append(row)  #column 4 = ts

        for ts in sorted(by_ts):
            rows = by_ts[ts]
            if args.to_dir:
                tmp = os.path.join(args.to_dir, f".{ts.replace(':', '')}.tmp")
                with open(tmp, "w", newline="") as out:
                    csv.writer(out).writerows(rows)
                os.rename(tmp, os.path.join(args.to_dir, f"{ts.replace(':', '')}.csv"))
            else:
                out = sys.stdout
                for row in rows:
                    out.write(row[0] + "|" + ",".join(row) + "\n")
                out.flush()
            sent += len(rows)
            print(f"sent {ts}  ({sent:,} readings so far)", file=sys.stderr, flush=True)
            if args.interval > 0:
                time.sleep(args.interval)

    print(f"Replay finished: {sent:,} readings", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
