#!/usr/bin/env python3
"""
Benchmark, MapReduce version (Hadoop Streaming): the mapper.

Same leak rule as the streaming job: a water meter whose readings stay above
0.5 imperial gallons for 4 readings in a row (one hour) between 01:00 and 03:45.

The mapper keeps only night readings of water meters and sends each one to the
reducer for its meter and night:  key = meter_id,night   value = ts,value
"""

import sys

NIGHT_HOURS = ("01", "02", "03")

for line in sys.stdin:
    f = line.rstrip("\n").split(",")
    #columns: meter_id, meter_type, premise_id, area, ts, interval_value, ...
    if len(f) < 6 or f[0] == "meter_id":   #header line of each daily file
        continue
    if f[1] != "water" or f[4][11:13] not in NIGHT_HOURS:
        continue
    try:
        value = float(f[5])
    except ValueError:
        continue
    print(f"{f[0]},{f[4][:10]}\t{f[4]},{value}")
