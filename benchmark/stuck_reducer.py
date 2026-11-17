#!/usr/bin/env python3
"""
micro-batch experiment, mapreduce version of the electricity stuck check: the reducer.

receives one meter's readings for the trailing 6 hours (the job's input is the last
24 interval files). output, one line per stuck meter:  meter_id,readings
mapreduce keeps nothing between runs, so every run reads the whole 6 hours again.
"""

import sys

MIN_READINGS = 20   #6 hrs = 24 readings; up to 4 may be missing, as in the streaming job


def check(meter, values):
    if len(values) >= MIN_READINGS and max(values) == 0:
        print(f"{meter},{len(values)}")


current, values = None, []
for line in sys.stdin:
    key, val = line.rstrip("\n").split("\t", 1)
    if key != current:
        if current is not None:
            check(current, values)
        current, values = key, []
    values.append(float(val.split(",")[1]))
if current is not None:
    check(current, values)
