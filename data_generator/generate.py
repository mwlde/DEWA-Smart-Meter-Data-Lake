"""
synthetic dewa smart meter readings since we dont have access to the real data :(

generates 15-minute interval readings for a set of water and electricity
meters, injects anomalies at known times, and writes a ground-truth file so
detection can be measured exactly.

standard library only: no pip install needed.

quick test (1,000 meters, 7 days, about 670,000 readings):
    python3 data_generator/generate.py

full project run (10,000 meters, 30 days, about 28.8 million readings):
    python3 data_generator/generate.py --meters 10000 --days 30

output (in --out, default ./data):
    meters.csv                              one row per meter
    readings/dt=YYYY-MM-DD/readings.csv     one file per day (HDFS/Hive friendly)
    ground_truth.csv                        every injected anomaly
    vacancies.csv                           every empty spell (normal behaviour, not an anomaly)

empty premises (--vacant-rate, default 5% of premises): the residents are away for
3 to 14 days, so the water meter reads exactly zero and the electricity meter only
shows a base load (fridge, a/c, standby: 30% of normal). these are not anomalies:
they are in the data to test that the detection rules do not flag normal behaviour
that looks like a fault (a stuck water meter also reads zero). premises with an
injected anomaly are never made empty. they use their own random generator, so
every other reading is identical to data made with --vacant-rate 0.

all timestamps are dubai local time (gulf standard time, utc+4).
water is in imperial gallons, the unit of dewa's annual statistics (dewa bills
in cubic metres since march 2025; 1 m3 = 219.9692 IG). electricity is in kWh.
"""

import argparse
import csv
import glob
import os
import random
import shutil
from datetime import datetime, timedelta


#average daily consumption per premise type, calibrated to the dewa annual statistics
#booklet 2025 (consumption by category / customers by category / 365):
#  residential water 92,857 MIG / 986,291 = 258 IG per account per day
#  commercial water 42,548 MIG / 164,683 = 708 IG per account per day
#  residential electricity 19,252 GWh / 1,003,334 = 52.6 kWh per account per day
#  commercial electricity 30,050 GWh / 244,973 = 336 kWh per account per day
#the values below give those yearly averages once the per-premise factor (avg 1.05)
#and the seasonal factors (avg 0.967 water, 0.733 electricity) are applied.
#dewa does not split homes into apartments and villas: the 60/25 mix and the villa to
#apartment ratio (4x water, 4.8x electricity) are our own assumptions
DAILY_WATER_IG = {"apartment": 135, "villa": 540, "commercial": 700}
DAILY_ELEC_KWH = {"apartment": 32, "villa": 155, "commercial": 440}

PREMISE_MIX = [("apartment", 0.60), ("villa", 0.25), ("commercial", 0.15)]

AREAS = [
    "Al Barsha", "Deira", "Jumeirah", "Dubai Marina", "Al Quoz",
    "Mirdif", "Business Bay", "International City", "Al Nahda", "Jebel Ali",
]

#electricity seasonality (a/c): 1.0 in peak summer
ELEC_SEASON = {1: 0.45, 2: 0.45, 3: 0.55, 4: 0.7, 5: 0.85, 6: 0.95,
               7: 1.0, 8: 1.0, 9: 0.95, 10: 0.8, 11: 0.6, 12: 0.5}
WATER_SEASON = {1: 0.85, 2: 0.85, 3: 0.9, 4: 0.95, 5: 1.0, 6: 1.05,
                7: 1.1, 8: 1.1, 9: 1.05, 10: 1.0, 11: 0.9, 12: 0.85}

#relative usage for each hr of the day (index = hr), per profile
WATER_HOME = [0.05, 0.02, 0.02, 0.02, 0.05, 0.4, 1.6, 2.0, 1.5, 0.9, 0.7, 0.7,
              0.9, 0.9, 0.7, 0.7, 0.9, 1.2, 1.7, 2.0, 1.8, 1.3, 0.7, 0.2]
WATER_COMMERCIAL = [0.05, 0.02, 0.02, 0.02, 0.02, 0.1, 0.5, 1.2, 1.8, 1.8, 1.7, 1.6,
                    1.8, 1.7, 1.6, 1.6, 1.5, 1.3, 0.8, 0.4, 0.2, 0.1, 0.05, 0.05]
ELEC_HOME = [0.6, 0.55, 0.5, 0.5, 0.5, 0.6, 0.8, 1.0, 0.9, 0.9, 1.0, 1.1,
             1.3, 1.4, 1.5, 1.5, 1.5, 1.5, 1.6, 1.6, 1.4, 1.2, 0.9, 0.7]
ELEC_COMMERCIAL = [0.4, 0.4, 0.4, 0.4, 0.4, 0.5, 0.8, 1.3, 1.7, 1.8, 1.8, 1.8,
                   1.8, 1.8, 1.8, 1.7, 1.6, 1.4, 1.0, 0.7, 0.5, 0.45, 0.4, 0.4]

INTERVALS_PER_DAY = 96  #one reading every 15 mins


def profile_for(meter_type, premise_type):
    commercial = premise_type == "commercial"
    if meter_type == "water":
        raw = WATER_COMMERCIAL if commercial else WATER_HOME
    else:
        raw = ELEC_COMMERCIAL if commercial else ELEC_HOME
    total = sum(raw)
    #normalise so the 96 interval weights sum to 1 over a day
    return [raw[i // 4] / total / 4 for i in range(INTERVALS_PER_DAY)]


def make_meters(n, rng):
    meters = []
    for i in range(n):
        meter_type = "water" if i % 2 == 0 else "electricity"
        r, acc, premise_type = rng.random(), 0.0, "apartment"
        for name, share in PREMISE_MIX:
            acc += share
            if r <= acc:
                premise_type = name
                break
        area = rng.choice(AREAS)
        if meter_type == "electricity":
            #same premise as the water meter before it, so same type and area
            #(the draws above still happen, so every other random value is unchanged)
            premise_type, area = meters[-1]["premise_type"], meters[-1]["area"]
        prefix = "W" if meter_type == "water" else "E"
        meters.append({
            "meter_id": f"{prefix}{i:07d}",
            "meter_type": meter_type,
            "premise_id": f"P{i // 2:07d}",  #one water and one electricity meter per premise
            "premise_type": premise_type,
            "area": area,
            #each premise uses a bit more or less than its type's average
            "scale": rng.uniform(0.6, 1.5),
        })
    return meters


def plan_anomalies(meters, start, days, rate, rng, leak_min=1.0, leak_max=6.0):
    """pick meters and inject one anomaly each, with known start and end."""
    total_intervals = days * INTERVALS_PER_DAY
    plans = {}
    for m in meters:
        if rng.random() >= rate:
            continue
        #leaks are the project's main use case, so water meters get them most often
        if m["meter_type"] == "water":
            kinds, weights = ["leak", "spike", "stuck", "silent", "negative"], [50, 15, 15, 10, 10]
        else:
            kinds, weights = ["spike", "stuck", "silent", "negative"], [30, 30, 20, 20]
        kind = rng.choices(kinds, weights)[0]
        start_idx = rng.randrange(INTERVALS_PER_DAY, total_intervals - INTERVALS_PER_DAY)
        if kind == "leak":
            length = total_intervals - start_idx            #leaks run until repaired (end of data)
            extra = rng.uniform(leak_min, leak_max)         #gallons added per 15 mins
        elif kind == "stuck":
            length, extra = rng.randrange(24, 192), 0.0     #6 to 48 hrs
        elif kind == "silent":
            length, extra = rng.randrange(8, 48), 0.0       #2 to 12 hrs of no data
        else:                                               #spike, negative: one reading
            length, extra = 1, rng.uniform(10, 30)
        plans[m["meter_id"]] = {
            "kind": kind, "start_idx": start_idx,
            #stuck and silent spells can run past the last day: end the ground truth there
            "end_idx": min(start_idx + length - 1, total_intervals - 1), "extra": extra,
        }
    return plans


VACANT_ELEC_SHARE = 0.3   #an empty home's electricity, as a share of its normal average


def plan_vacancies(meters, plans, days, rate, seed):
    """pick premises that are empty for a while. own random generator, so the
    main readings do not change."""
    vrng = random.Random(seed + 1000)
    total = days * INTERVALS_PER_DAY
    vacancies = {}
    for w, e in zip(meters[0::2], meters[1::2]):   #water meter, then its premise's electricity meter
        if vrng.random() >= rate or w["meter_id"] in plans or e["meter_id"] in plans:
            continue
        length = vrng.randrange(3 * INTERVALS_PER_DAY, 14 * INTERVALS_PER_DAY + 1)
        start_idx = vrng.randrange(INTERVALS_PER_DAY, total)   #day 1 stays normal (baseline history)
        vacancies[w["premise_id"]] = {"start_idx": start_idx, "end_idx": min(start_idx + length, total) - 1,
                                      "water": w["meter_id"], "electricity": e["meter_id"]}
    return vacancies


def ts_at(start, idx):
    return start + timedelta(minutes=15 * idx)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--meters", type=int, default=1000)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--start", default="2026-07-01", help="first day, YYYY-MM-DD")
    p.add_argument("--seed", type=int, default=42, help="same seed gives identical data")
    p.add_argument("--anomaly-rate", type=float, default=0.03, help="share of meters with an anomaly")
    p.add_argument("--leak-min", type=float, default=1.0, help="smallest leak, gallons per 15 mins")
    p.add_argument("--leak-max", type=float, default=6.0, help="largest leak, gallons per 15 mins")
    p.add_argument("--vacant-rate", type=float, default=0.05,
                   help="share of premises left empty for 3 to 14 days (0 = none, as in runs 1 to 5)")
    p.add_argument("--out", default="data")
    args = p.parse_args()
    if args.days < 3:
        p.error("--days must be at least 3 (day 1 is history, anomalies start on day 2 and end before the last day)")

    rng = random.Random(args.seed)
    start = datetime.strptime(args.start, "%Y-%m-%d")
    meters = make_meters(args.meters, rng)
    plans = plan_anomalies(meters, start, args.days, args.anomaly_rate, rng, args.leak_min, args.leak_max)
    vacancies = plan_vacancies(meters, plans, args.days, args.vacant_rate, args.seed)
    profiles = {}
    os.makedirs(args.out, exist_ok=True)
    #remove day folders from an earlier, longer dataset: the loader and the replay take
    #every dt=... folder, so 7 new days next to 30 old ones would mix two datasets
    old_days = glob.glob(os.path.join(args.out, "readings", "dt=*"))
    for folder in old_days:
        shutil.rmtree(folder)
    if old_days:
        print(f"removed {len(old_days)} day folder(s) from the previous dataset")

    #meters.csv
    with open(os.path.join(args.out, "meters.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["meter_id", "meter_type", "premise_id", "premise_type", "area"])
        for m in meters:
            w.writerow([m["meter_id"], m["meter_type"], m["premise_id"], m["premise_type"], m["area"]])

    #ground_truth.csv
    with open(os.path.join(args.out, "ground_truth.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["anomaly_id", "meter_id", "anomaly_type", "start_ts", "end_ts"])
        for n, (meter_id, a) in enumerate(sorted(plans.items()), 1):
            w.writerow([f"A{n:05d}", meter_id, a["kind"],
                        ts_at(start, a["start_idx"]).isoformat(),
                        ts_at(start, a["end_idx"]).isoformat()])

    #vacancies.csv
    with open(os.path.join(args.out, "vacancies.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["vacancy_id", "premise_id", "water_meter_id", "electricity_meter_id", "start_ts", "end_ts"])
        for n, (premise_id, v) in enumerate(sorted(vacancies.items()), 1):
            w.writerow([f"V{n:05d}", premise_id, v["water"], v["electricity"],
                        ts_at(start, v["start_idx"]).isoformat(), ts_at(start, v["end_idx"]).isoformat()])

    cumulative = {m["meter_id"]: round(rng.uniform(1000, 50000), 2) for m in meters}
    total_rows = 0

    for day in range(args.days):
        date = start + timedelta(days=day)
        #"dt" rather than "date": date is a reserved word in hive
        folder = os.path.join(args.out, "readings", f"dt={date:%Y-%m-%d}")
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, "readings.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["meter_id", "meter_type", "premise_id", "area", "ts",
                        "interval_value", "unit", "cumulative_value", "signal_quality"])
            for m in meters:
                key = (m["meter_type"], m["premise_type"])
                if key not in profiles:
                    profiles[key] = profile_for(*key)
                weights = profiles[key]
                water = m["meter_type"] == "water"
                daily = (DAILY_WATER_IG if water else DAILY_ELEC_KWH)[m["premise_type"]]
                season = (WATER_SEASON if water else ELEC_SEASON)[date.month]
                base = daily * season * m["scale"]
                plan = plans.get(m["meter_id"])
                vac = vacancies.get(m["premise_id"])

                for i in range(INTERVALS_PER_DAY):
                    idx = day * INTERVALS_PER_DAY + i
                    in_anomaly = plan and plan["start_idx"] <= idx <= plan["end_idx"]

                    if in_anomaly and plan["kind"] == "silent":
                        continue  #no reading arrives

                    value = base * weights[i] * max(0.0, rng.gauss(1.0, 0.25))
                    #homes often use no water at all in a quiet night interval
                    if water and weights[i] < 0.002 and rng.random() < 0.6:
                        value = 0.0

                    if in_anomaly:
                        kind = plan["kind"]
                        if kind == "leak":
                            value += plan["extra"] * rng.uniform(0.9, 1.1)
                        elif kind == "spike":
                            #a meter glitch: 10 to 30 times the meter's normal peak
                            #(multiplying the current value would hide night spikes,
                            #when a water meter reads about zero)
                            value = base * max(weights) * plan["extra"]
                        elif kind == "stuck":
                            value = 0.0
                        elif kind == "negative":
                            value = -abs(value) - 1.0

                    if vac and vac["start_idx"] <= idx <= vac["end_idx"]:
                        #empty premise: replaces the value above after its random draws, so every
                        #other meter's readings stay the same. electricity keeps the same noise
                        #around a flat base load
                        noise = value / (base * weights[i]) if base * weights[i] else 1.0
                        value = 0.0 if water else base / INTERVALS_PER_DAY * VACANT_ELEC_SHARE * noise

                    value = round(value, 3)
                    if value > 0:
                        cumulative[m["meter_id"]] = round(cumulative[m["meter_id"]] + value, 3)

                    w.writerow([m["meter_id"], m["meter_type"], m["premise_id"], m["area"],
                                ts_at(start, idx).isoformat(), value,
                                "IG" if water else "kWh",
                                cumulative[m["meter_id"]],
                                rng.randint(70, 100)])
                    total_rows += 1
        print(f"{date:%Y-%m-%d}: done")

    print(f"\n{len(meters):,} meters, {args.days} days, {total_rows:,} readings")
    print(f"{len(plans):,} anomalies injected (see {args.out}/ground_truth.csv)")
    print(f"{len(vacancies):,} premises empty for a while (see {args.out}/vacancies.csv; not anomalies)")


if __name__ == "__main__":
    main()
