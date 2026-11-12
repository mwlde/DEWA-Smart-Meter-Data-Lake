#!/usr/bin/env python3
"""
Benchmark, MapReduce version (Hadoop Streaming): the reducer.

Receives one meter's night readings (sorted by key, so all readings of one
meter and night arrive together), puts them in time order and counts the
1-hour windows (4 consecutive 15-minute readings) where every reading is
above 0.5 gallons. Output, one line per leaking meter and night:
    meter_id,night,leak_windows
"""

import sys
from datetime import datetime, timedelta

MIN_FLOW = 0.5
SPAN = timedelta(minutes=45)   #first to last reading of a 4-reading window


def check(key, rows):
    rows.sort()
    times = [datetime.strptime(t, "%Y-%m-%dT%H:%M:%S") for t, _ in rows]
    values = [v for _, v in rows]
    windows = sum(1 for i in range(len(rows) - 3)
                  if times[i + 3] - times[i] == SPAN and min(values[i:i + 4]) > MIN_FLOW)
    if windows:
        print(f"{key},{windows}")


current, rows = None, []
for line in sys.stdin:
    key, val = line.rstrip("\n").split("\t", 1)
    ts, value = val.split(",")
    if key != current:
        if current is not None:
            check(current, rows)
        current, rows = key, []
    rows.append((ts, float(value)))
if current is not None:
    check(current, rows)
