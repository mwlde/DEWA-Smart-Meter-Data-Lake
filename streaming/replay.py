"""
replays the generated meter readings as a live stream.

readings are sent in time order, one 15-minute interval at a time: all meters'
readings for 00:00, then a pause, then all readings for 00:15, and so on. the
pause sets the speed: with --interval 1, one simulated day passes in 96 seconds.

day 1 is skipped by default: it is the "history" the batch layer uses to build
each meter's normal range (streaming/baseline.py). days 2 onwards arrive live.

each line is printed as  meter_id|reading-as-csv  so kafka-console-producer can
use the meter id as the message key (all readings of one meter go to the same
kafka partition, in order).

usage, inside the container, from /home/hadoop/dewa:
    python3 streaming/replay.py --interval 1 | kafka-console-producer.sh \
        --bootstrap-server localhost:9092 --topic meter-readings \
        --property parse.key=true --property key.separator='|'

or write to files instead of kafka (used for testing without kafka):
    python3 streaming/replay.py --interval 0 --to-dir /tmp/stream_in

late readings (--late-share): real meter networks do not deliver every reading
on time. with --late-share 0.01 --late-max 2, 1% of readings are held back and
sent 1 or 2 intervals (15 or 30 minutes) later, after newer readings of the same
meter: out of order, as a retrying meter would send them.
"""

import argparse
import csv
import glob
import os
import random
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
    p.add_argument("--late-share", type=float, default=0.0, help="share of readings sent late (0 = none)")
    p.add_argument("--late-max", type=int, default=2, help="most intervals a late reading is held back")
    p.add_argument("--seed", type=int, default=7, help="random seed for picking late readings")
    args = p.parse_args()
    rng = random.Random(args.seed)

    files = day_files(args.data, args.skip_days)
    if args.days:
        files = files[: args.days]
    if not files:
        sys.exit(f"No reading files found under {args.data}/readings. Run data_generator/generate.py first.")
    if args.to_dir:
        os.makedirs(args.to_dir, exist_ok=True)

    sent, late_sent, slot = 0, 0, 0
    held = defaultdict(list)   #slot number -> late readings due then

    def send(label, rows):
        nonlocal sent
        if args.to_dir:
            name = label.replace(":", "")
            tmp = os.path.join(args.to_dir, f".{name}.tmp")
            with open(tmp, "w", newline="") as out:
                csv.writer(out).writerows(rows)
            os.rename(tmp, os.path.join(args.to_dir, f"{name}.csv"))
        else:
            out = sys.stdout
            for row in rows:
                out.write(row[0] + "|" + ",".join(row) + "\n")
            out.flush()
        sent += len(rows)
        print(f"sent {label}  ({sent:,} readings so far)", file=sys.stderr, flush=True)
        if args.interval > 0:
            time.sleep(args.interval)

    for path in files:
        #one day is small enough to hold in memory (96 readings x meters)
        by_ts = defaultdict(list)
        with open(path, newline="") as f:
            reader = csv.reader(f)
            next(reader)  #header
            for row in reader:
                by_ts[row[4]].append(row)  #column 4 = ts

        for ts in sorted(by_ts):
            rows = []
            for row in by_ts[ts]:
                if args.late_share > 0 and rng.random() < args.late_share:
                    held[slot + rng.randint(1, args.late_max)].append(row)   #arrives later
                else:
                    rows.append(row)
            due = held.pop(slot, [])
            late_sent += len(due)
            send(ts, rows + due)   #late readings after the newer ones: out of order
            slot += 1

    while held:   #late readings due after the last interval
        due = held.pop(min(held))
        late_sent += len(due)
        send(f"late-{slot}", due)
        slot += 1

    if args.late_share > 0:
        print(f"Late readings sent: {late_sent:,}", file=sys.stderr)
    print(f"Replay finished: {sent:,} readings", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
