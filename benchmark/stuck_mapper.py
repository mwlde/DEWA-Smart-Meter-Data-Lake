#!/usr/bin/env python3
"""
Micro-batch experiment, MapReduce version of the electricity stuck check: the mapper.

Same rule as the streaming job: an electricity meter with zero consumption in every
reading for 6 hours. The mapper sends each electricity reading to the reducer for
its meter:  key = meter_id   value = ts,value
"""

import sys

for line in sys.stdin:
    f = line.rstrip("\n").split(",")
    #columns: meter_id, meter_type, premise_id, area, ts, interval_value, ...
    if len(f) < 6 or f[0] == "meter_id" or f[1] != "electricity":
        continue
    try:
        value = float(f[5])
    except ValueError:
        continue
    print(f"{f[0]}\t{f[4]},{value}")
