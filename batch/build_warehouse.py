"""
reporting layer: builds the data warehouse from the curated zone and the alerts zone.

it is a star schema with two fact tables sharing their dimensions (a constellation):

  dim_date           one row per day: day of week, weekend flag, week, month, quarter, year
  dim_meter          one row per meter with its premise and area (the meter, premise and
                     area hierarchy flattened into one table, as in a star schema)
  dim_alert_type     the five checks: what each detects, when it fires, who acts on it
  fact_daily_usage   one row per meter per day (the grain), partitioned by month
  fact_alert         one row per meter, alert type and day

surrogate keys: date_id (yyyymmdd as a number), meter_id and alert_type_id (numbers
from 1). the meter's own number (W0000001) is kept as meter_number. each fact table's
primary key is the combination of its foreign keys.

the hive tables over these files are in hive/03_warehouse.sql, the reporting queries
in hive/04_reports.sql. data made before the generator fix of 12 november (premise
type and area per premise) gives wrong results by premise and area: regenerate first.

run inside the container, from /home/hadoop/dewa, after batch/build_zones.py (and after
a streaming run, for fact_alert):
    spark-submit batch/build_warehouse.py 2>/dev/null
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  #repo root

from pyspark.sql import SparkSession, Window, functions as F  #noqa: E402

from common.errors import fail, run, step  #noqa: E402

INTERVALS_PER_DAY = 96

#the five checks (streaming/detect_stream.py)
ALERT_TYPES = [
    (1, "leak", "continuous night flow on a water meter (minimum night flow)",
     "on the reading that completes a qualifying 1-hour night window", "customer care: contact the customer"),
    (2, "spike", "one reading above 4 times the meter's normal maximum",
     "on the reading itself", "meter maintenance: check the meter"),
    (3, "negative", "a negative reading", "on the reading itself", "meter maintenance: check the meter"),
    (4, "stuck", "zero for 6 hours (electricity) or 24 hours in an occupied premise (water)",
     "on the reading that completes the window", "meter maintenance: replace the meter"),
    (5, "silent", "fewer than 4 readings in an hour",
     "after the watermark passes the end of the hour", "network operations: check communications"),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="/user/hadoop/dewa", help="data lake root")
    args = p.parse_args()
    base, out = args.base, f"{args.base}/warehouse"

    with step("starting Spark"):
        spark = (SparkSession.builder.appName("dewa-build-warehouse")
                 .config("spark.sql.session.timeZone", "UTC")   #timestamps are dubai local time as written
                 .config("spark.sql.shuffle.partitions", "8")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

    with step("reading the curated zone"):
        try:
            usage = spark.read.parquet(f"{base}/curated/daily_meter_usage")
            meters = spark.read.parquet(f"{base}/curated/dim_meter")
            n_usage = usage.count()
        except Exception as e:
            fail("reading the curated zone", f"cannot read {base}/curated ({str(e).splitlines()[0][:200]})",
                 "build it with: spark-submit batch/build_zones.py 2>/dev/null")
        if n_usage == 0:
            fail("reading the curated zone", "daily_meter_usage is empty",
                 "build it with: spark-submit batch/build_zones.py 2>/dev/null")

    with step("building the dimensions"):
        #dim_date: every day in the data
        days = usage.select(F.col("usage_date").alias("full_date")).distinct()
        dim_date = days.select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_id"),
            "full_date",
            F.dayofmonth("full_date").alias("day_of_month"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            #uae weekend: saturday and sunday since 2022
            F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
            F.weekofyear("full_date").alias("week_of_year"),
            F.month("full_date").alias("month_num"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.quarter("full_date").alias("quarter"),
            F.year("full_date").alias("year"))

        #dim_meter: surrogate key in meter number order
        dim_meter = (meters.withColumnRenamed("meter_id", "meter_number")
                     .withColumn("meter_id", F.row_number().over(Window.orderBy("meter_number")))
                     .select("meter_id", "meter_number", "meter_type",
                             F.when(F.col("meter_type") == "water", "IG").otherwise("kWh").alias("unit"),
                             F.col("premise_id").alias("premise_number"),
                             "premise_type", "area"))

        dim_alert_type = spark.createDataFrame(
            ALERT_TYPES, "alert_type_id INT, alert_type STRING, description STRING, fires STRING, handled_by STRING")

    with step("building fact_daily_usage"):
        keys = dim_meter.select("meter_id", "meter_number")
        fact_usage = (usage.withColumnRenamed("meter_id", "meter_number").join(keys, "meter_number")
                      .select(F.date_format("usage_date", "yyyyMMdd").cast("int").alias("date_id"),
                              "meter_id",
                              F.col("total_value").alias("consumption"),
                              F.col("readings"),
                              (F.lit(INTERVALS_PER_DAY) - F.col("readings")).alias("missing_readings"),
                              F.col("max_interval").alias("peak_interval"),
                              F.col("min_night_flow"),
                              F.date_format("usage_date", "yyyy-MM").alias("usage_month")))

    with step("building fact_alert"):
        try:
            alerts = spark.read.parquet(f"{base}/alerts").drop("source", "batch")
            n_alert_rows = alerts.count()
        except Exception:
            alerts, n_alert_rows = None, 0
        if alerts is None or n_alert_rows == 0:
            print("  no alerts in the alerts zone (run bash scripts/run_stream.sh first): fact_alert left empty")
            fact_alert = spark.createDataFrame(
                [], "date_id INT, meter_id INT, alert_type_id INT, alert_windows BIGINT, "
                    "first_window_start TIMESTAMP, last_window_end TIMESTAMP")
        else:
            types = dim_alert_type.select("alert_type_id", "alert_type")
            fact_alert = (alerts
                          .withColumn("date_id", F.date_format("window_start", "yyyyMMdd").cast("int"))
                          .groupBy("date_id", F.col("meter_id").alias("meter_number"), "alert_type")
                          .agg(F.countDistinct("window_start").alias("alert_windows"),
                               F.min("window_start").alias("first_window_start"),
                               F.max("window_end").alias("last_window_end"))
                          .join(keys, "meter_number")
                          .join(types, "alert_type")
                          .select("date_id", "meter_id", "alert_type_id", "alert_windows",
                                  "first_window_start", "last_window_end"))

    with step("writing the warehouse tables"):
        dim_date.write.mode("overwrite").parquet(f"{out}/dim_date")
        dim_meter.write.mode("overwrite").parquet(f"{out}/dim_meter")
        dim_alert_type.write.mode("overwrite").parquet(f"{out}/dim_alert_type")
        fact_usage.write.mode("overwrite").partitionBy("usage_month").parquet(f"{out}/fact_daily_usage")
        fact_alert.write.mode("overwrite").parquet(f"{out}/fact_alert")

    with step("checking the results"):
        print(f"\nWarehouse written to {out}")
        for name in ["dim_date", "dim_meter", "dim_alert_type", "fact_daily_usage", "fact_alert"]:
            print(f"  {name:<18} {spark.read.parquet(f'{out}/{name}').count():>10,} rows")
        n_fact = spark.read.parquet(f"{out}/fact_daily_usage").count()
        if n_fact != n_usage:
            fail("checking the results", f"fact_daily_usage has {n_fact:,} rows, daily_meter_usage {n_usage:,}",
                 "a meter in the readings is missing from dim_meter: rebuild the zones")
        print("Next: hive -f hive/03_warehouse.sql 2>/dev/null, then hive -f hive/04_reports.sql 2>/dev/null")
    spark.stop()


if __name__ == "__main__":
    run(main)
