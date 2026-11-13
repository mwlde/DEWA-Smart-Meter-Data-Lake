# Results

Every measured result, with the conditions it came from. Report sections 5.5 and 7.1 take their figures from here. Add a new run at the top; keep the old ones. Screenshots are in `screenshots/mac/`; those from runs before the 13 November fix (runs 1 to 5, the old benchmark screenshots and the first data loads) are in `screenshots/mac/superseded/`, kept as the record behind the older entries below.

## Restart test (13 November 2026)

A check of `scripts/restart_test.sh` before it runs in the container: the same steps in file mode (no Kafka or HDFS), 1,000 meters, 3 days (seed 42, 22 empty premises, 1% of readings late), one interval per second. Run A without interruption; in run B the detection job was killed with `kill -9` 110 seconds into the replay and restarted from its checkpoint. Compared with `evaluation/compare_runs.py`.

| Check | Injected | Detected A | Detected B | Alert windows A | Alert windows B | Only in A | Only in B |
| --- | --- | --- | --- | --- | --- | --- | --- |
| leak | 11 | 11 | 11 | 99 | 99 | 0 | 0 |
| negative | 3 | 3 | 3 | 3 | 3 | 0 | 0 |
| silent | 6 | 6 | 6 | 36 | 36 | 0 | 0 |
| spike | 3 | 3 | 3 | 3 | 3 | 0 | 0 |
| stuck | 6 | 6 | 6 | 67 | 67 | 0 | 0 |

- No false alerts in either run; 209 alert records each (one repeated record for the same window in each, from a late reading).
- Landing zone against the raw zone (191,824 readings on the streamed days): both runs 191,824 stored, 0 missing, 0 extra.
- Median first-alert latency 1.0 to 1.9 s higher in run B than in A. Run B includes the anomalies that arrived while the job was down, which waited for the restart.
- To repeat in the container with Kafka: `bash scripts/restart_test.sh`.

## Benchmark 4: the leak check on the run 6 data (13 November 2026)

Benchmark 2 again, on the final full-size data (the run 6 dataset: 10,000 meters, 30 days, 2.2 GB, with 226 empty premises), with the streaming row from run 6. `bash scripts/run_benchmark.sh`, three runs of each batch tool, Maria's MacBook Air, one Docker container.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 30.8 s | 28.3 to 33.5 s | 1370 | 1370 | 0 | 0 | about 2 h + 31 s | 36 |
| Hive (HiveQL on MapReduce) | 45.3 s | 43.6 to 45.4 s | 1370 | 1370 | 0 | 0 | about 2 h + 45 s | 16 |
| Spark batch (DataFrames, local mode) | 9.8 s | 9.3 to 10.0 s | 1370 | 1370 | 0 | 0 | about 2 h + 10 s | 39 |
| Spark batch (DataFrames, on YARN) | 25.2 s | 24.2 to 25.5 s | 1370 | 1370 | 0 | 0 | about 2 h + 25 s | 39 |
| Spark Structured Streaming | continuous | - | 1370 | 1370 | 0 | 0 | 1.9 s | 280 (all 5 checks) |

- Every approach found the same 1,370 leaking meter-nights the ground truth expects (91 injected leaks), none missed and none extra, as in benchmark 2. The empty premises did not add any.
- Against benchmark 2 (same size, old data): MapReduce 30.3 to 30.8 s, Hive 44.6 to 45.3 s, Spark on YARN 22.2 to 25.2 s, Spark local 9.9 to 9.8 s. Run-to-run noise on one laptop; the ranking is the same.
- Streaming: 5.8 to 1.9 s, because leak no longer waits two replay intervals (update mode). 237 to 280 lines: the water stuck rule, the late-readings listener and the restart-safe alert writes.
- Screenshot: `5-5_benchmark_run6.png`. It replaces `5-5_benchmark_full_size.png` (benchmark 2), now in `screenshots/mac/superseded/`.

## Run 6: full-size run on the new code, final results (13 November 2026)

**These are the headline figures for the report**, replacing run 5.

| | |
| --- | --- |
| Machine | Maria's MacBook Air (Apple Silicon), one Docker container, HBase off |
| Data | 10,000 meters, 30 days (1 to 30 July), 28,799,091 readings, seed 42, calibrated generator, 226 empty premises (`--vacant-rate 0.05`); raw zone 2.2 GB |
| Live stream | Days 2 to 30, one interval per second, 1% of readings 15 or 30 minutes late |
| Detection job | 13 November code: leak and stuck in update mode, 30-minute watermark, water stuck needs 24 hours of zero in an occupied premise |
| Anomalies | 300: 91 leak, 42 negative, 35 silent, 75 spike, 57 stuck (38 electricity, 19 water) |

**Detection** (`evaluation/evaluate.py`, first alert per anomaly, latency to `emitted_ts`)

| Type | Injected | Detected | Detection | False alerts | Waits for later readings | Median delay, simulated | Median latency, real | Median real time from start to alert |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| leak | 91 | 91 | 100% | 0 | no | 525 min | 1.92 s | 525 min |
| negative | 42 | 42 | 100% | 0 | no | 0 min | 2.01 s | 0 min |
| silent | 35 | 35 | 100% | 0 | yes | 15 min | 8.47 s | 45 min |
| spike | 75 | 75 | 100% | 0 | no | 0 min | 2.06 s | 0 min |
| stuck | 57 | 47 | 82.5% | 0 | no | 390 min | 2.60 s | 390 min |
| **All** | **300** | **290** | **96.7%** | **0** | | | | |

**Latency over every alert record**: leak 12,289 records, median 2.69 s; negative 42, 2.01 s; silent 210, 8.42 s; spike 75, 2.06 s; stuck 944, 2.52 s.

- **The 10 missed anomalies are exactly the 10 water meters stuck for less than 24 hours** (of 19 water stuck spells; the other 9, all over a day, were caught). All 38 electricity stuck meters were caught. This is the water rule's design: a water meter reading zero is normal in an empty home, so it needs a full day of zero while the premise's electricity shows it is occupied.
- **Empty premises**: 226 in the data, no alert raised on any of them while empty.
- **Late readings**: none dropped (`Readings dropped as later than the watermark (30 minutes): leak 0, stuck 0, stuck_water 0, silent 0, point 0`); 988 alert batches; Spark's error log clean.
- 13,560 alert records, 290 incidents (one per meter and type), 6 repeated records for the same window (late readings changing a window that had already fired; the alerts zone keeps both, HBase would overwrite).
- **First leak alert of the run** (`grep -m 1 -A 14 "\[leak\]" logs/stream.log`): W0007004 in Jebel Ali, window 01:30 to 02:30 on 2 July, raised in the micro-batch that started 2.0 s after the window's last reading arrived (09:35:42.024 to 09:35:44.014); the next window fired 0.9 s after its last reading. The same figure in run 5 showed 4.1 s, two replay intervals of waiting.
- **Against run 5** (old code): leak 5.77 to 1.92 s and stuck 6.55 to 2.60 s, because they no longer wait two intervals (30 minutes in real time); silent 7.05 to 8.47 s, now waiting for a 30-minute watermark. Detection 300 to 290 of 300 (the short water stuck spells, by design), with 226 empty premises and late readings added.

**Storage per zone** (`hdfs dfs -du -h /user/hadoop/dewa`, after the run):

| Zone | Size | Note |
| --- | --- | --- |
| Raw (CSV) | 2.2 GB | as received |
| Landing (Parquet) | 536.8 MB | every streamed reading |
| Cleansed (Parquet) | 338.1 MB | 6.5 times smaller than the raw CSV |
| Checkpoints | 752.1 MB | up from 443.1 MB in run 5: one more query, and 24-hour windows for the water stuck check |
| Alerts | 5.4 MB | 13,560 alerts |
| Curated | 5.0 MB | daily usage, meters, baseline |
| Warehouse | 3.9 MB | star schema |
| Quarantine | 59.8 KB | the 42 injected negative readings |

**Warehouse** (`batch/build_warehouse.py`, `hive/03_warehouse.sql`): `fact_daily_usage` 300,000 rows, `fact_alert` 1,615, `dim_meter` 10,000, `dim_date` 30, `dim_alert_type` 5.

**Reports** (`hive/04_reports.sql`, all six ran in Hive):

- *Roll-up*: 252,799.1 m3 of water in July, from 23,172.8 (Al Barsha) to 26,155.1 m3 (Mirdif) per area, with the month subtotal and grand total rows.
- *Cube*: average daily use per premise 370.7 IG; apartment 155.8, villa 619.1, commercial 791.9 IG (consistent with the calibration with July's seasonal factor); weekday and weekend almost equal (370.8 and 370.5 IG), because the generator has no weekly pattern (a real utility's data would show one).
- *Grouping sets*: 1,615 alert meter-days and 13,554 alert windows in total; leak 1,370 meter-days (exactly the 1,370 leaking meter-nights every tool found in benchmark 2), 12,289 windows; negative 42; silent 44 meter-days; spike 75; stuck 84 meter-days, 938 windows. Per area, for example Al Nahda 205 leak meter-days.
- *Billing extract*: one row per premise for July, water in m3 and electricity in kWh side by side (for example P0000000, villa, Al Barsha: 102.19 m3, 5,851.3 kWh, no missing readings).
- *Window function*: the three biggest water users in each area, nearly all commercial (150 to 160 m3 in the month); the outliers, Jebel Ali P0003190 (189.7 m3) and P0000743 (173.9 m3) and the villa P0001737 in Al Quoz (157.3 m3), stand out from their peers.
- *Dice*: missing readings on weekdays by area; at most 0.013% (International City electricity, 133 readings).

**Screenshots** (in `screenshots/mac/`): `5-3-4_generate_run6.png`, `5-3-4_raw_zone_run6.png`, `5-3-2_run6_leak_alerts.png`, `7-1_evaluation_run6.png`, `5-3-5_warehouse_counts_rollup_cube.png`, `5-3-5_reports_grouping_sets_billing.png`, `5-3-5_reports_rank_and_missing.png`, `5-3-4_zone_sizes_run6.png`.

## Replay-speed test on Kafka (13 November 2026)

`bash scripts/speed_test.sh`: 1,000 meters, 3 days (2 live days, 191,824 readings, 1,906 late), 29 anomalies, 22 empty premises, HBase off, the same data at 1 and at 4 seconds per interval. **These are the figures for report section 5.5.**

Both runs: 29 of 29 detected, no false alerts, no alerts on the empty premises, no late readings dropped (2 and 3 repeated records for one window).

| Check | Alert records | Latency at 1 s per interval | Latency at 4 s per interval | Change | Intervals waited | Real time from anomaly start to alert (median) |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 99 | 2.25 s | 2.20 s | -0.05 s | 0.0 | 930 min |
| negative | 3 | 2.07 s | 1.59 s | -0.48 s | -0.2 | 0 min |
| silent | 36 | 8.10 s | 16.96 s | +8.86 s | 3.0 | 45 min |
| spike | 3 | 2.99 s | 3.83 s | +0.84 s | 0.3 | 0 min |
| stuck | 69 | 2.05 s | 3.03 s | +0.98 s | 0.3 | 1,440 min |

Latency is the median over every alert record (intervals waited = change / 3 extra seconds per interval). Leak, stuck, spike and negative waited no interval: they fire on the reading that reveals the anomaly, so their replay latency (about 2 to 4 seconds) is their real-world latency. Silent waited 3 intervals, as designed: 45 minutes after the first missing reading in real time. The last column is set by the rules: a leak shows only at night (in this 2-day stream, mostly the night after the next), a stuck water meter only after 24 hours.

First-alert medians (11 leaks, 6 stuck): fast run leak 2.25, negative 2.07, silent 8.19, spike 2.99, stuck 2.62 s; slow run leak 2.70, negative 1.59, silent 19.95, spike 3.83, stuck 3.01 s.

**Screenshots** (in `screenshots/mac/`): `5-5_speed_test_fast.png`, `5-5_speed_test_slow_and_comparison.png`.

## Micro-batch experiment: MapReduce every 15 minutes (13 November 2026)

`bash scripts/run_microbatch.sh` on the same 1,000-meter, 3-day data: 8 cycles (data up to 01:45 to 03:30 on 3 July), each cycle running the leak check on the last 4 interval files and the electricity stuck check on the last 24 as Hadoop Streaming jobs on YARN.

| Check | Runtime per cycle (median) | Range | Intervals read per cycle | New intervals per cycle | Data read per cycle |
| --- | --- | --- | --- | --- | --- |
| leak | 15.1 s | 11.1 to 15.2 s | 4 | 1 | 0.3 MB |
| stuck (electricity, 6 hours) | 19.2 s | 19.1 to 24.6 s | 24 | 1 | 1.9 MB |

- The leak job found the same 11 leaking meters every cycle; no electricity meter was stuck for a full 6 hours in that window, so the stuck job found none (correct).
- **MapReduce can meet R2 this way**: triggered as each interval lands, an alert would come 15 to 20 seconds later, inside 5 minutes. Run nightly it is about 2 hours.
- **What rules it out is cost**: about 34 seconds of YARN jobs per cycle for two checks, almost all of it job start-up (a cycle reads under 2 MB); a new job per check every 15 minutes, 480 a day for five checks; and no state between runs, so each reading is read 4 times by the leak check, 24 times by the 6-hour stuck check and would be 96 times by the 24-hour water rule. At DEWA's scale (2.44 million readings an interval, about 80 bytes each as CSV) a 6-hour window is about 4.7 GB read every 15 minutes to add 0.2 GB. The streaming job reads each reading once, keeps its windows in memory and alerts in about 2 seconds.
- Single-machine figures: they show the pattern (start-up per job, re-reading per cycle), not runtimes on a cluster.

**Screenshot** (in `screenshots/mac/`): `5-5_microbatch_mapreduce.png`.

## First replay-speed test on Kafka: method too noisy, redone (13 November 2026)

`bash scripts/speed_test.sh` (1,000 meters, 3 days, 1 and 3 seconds per interval). Both runs detected 29 of 29 anomalies with no false alerts, no alerts on the 22 empty premises and no late readings dropped. But the comparison used each anomaly's first alert only (11 leaks, 6 stuck meters, 3 spikes, 3 negatives), and with a 2-second trigger those medians move by a second or two by chance: leak +1.66 s, stuck +1.19 s, negative +0.36 s, spike -1.44 s, silent +7.33 s. The script's threshold (half of the 2-second step) labelled leak and stuck "waits", which the spread both ways shows is noise (in the local test both fell instead). Silent's +7.33 s is the real wait, about 4 intervals. A check that waited even one interval would grow by a whole 2 seconds every time.

Changed: the evaluation also reports the median latency over every alert record (99 leak and 68 stuck records in the local test instead of 11 and 6), the comparison uses it and reports intervals waited, and the slow run is 4 seconds per interval (a one-interval wait then adds 3 seconds). The figures above are not used in the report.

## Run 6a: 1,000-meter run on the new code, Kafka and HBase (13 November 2026)

**These are the figures to quote for the 1,000-meter dataset**, replacing run 4. Same setup as run 4 (MacBook Air, one Docker container, HBase on, `bash scripts/run_stream.sh`), with the 13 November code: leak and stuck in update mode, 30-minute watermark, water stuck needs 24 hours of zero in an occupied premise.

| | |
| --- | --- |
| Data | 1,000 meters, 7 days, 671,792 readings, seed 42, calibrated generator, 20 empty premises (`--vacant-rate 0.05`) |
| Live stream | Days 2 to 7: 575,792 readings, one interval per second; 5,732 sent 15 or 30 minutes late (1%) |
| Anomalies | 30: 8 leak, 4 negative, 6 silent, 4 spike, 8 stuck (5 electricity, 3 water) |
| Zones | 671,792 raw, 671,788 cleansed, 4 quarantined (the 4 injected negatives) |

**Detection** (`evaluation/evaluate.py`, first alert per anomaly, latency to `emitted_ts`)

| Type | Injected | Detected | Detection | False alerts | Waits for later readings | Median delay, simulated | Median latency, real | Median real time from start to alert |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | no | 735 min | 1.99 s | 735 min |
| negative | 4 | 4 | 100% | 0 | no | 0 min | 2.03 s | 0 min |
| silent | 6 | 6 | 100% | 0 | yes | 15 min | 8.44 s | 45 min |
| spike | 4 | 4 | 100% | 0 | no | 0 min | 2.01 s | 0 min |
| stuck | 8 | 5 | 62.5% | 0 | no | 390 min | 2.20 s | 390 min |
| **All** | **30** | **27** | **90%** | **0** | | | | |

- **The 3 missed anomalies are water meters stuck for less than a day**, which the water rule no longer catches by design (it needs 24 hours of zero so that empty homes are not flagged): W0000886 for 11.25 hours, W0000376 for 6.25 hours, W0000564 for 12.75 hours. The one water spell longer than a day (W0000286, 39 hours) was caught, and all 5 electricity stuck meters.
- **Empty premises**: 20 in the data, no alerts raised on them while empty.
- **Late readings**: none of the 5,732 dropped (`Readings dropped as later than the watermark (30 minutes): leak 0, stuck 0, stuck_water 0, silent 0, point 0`).
- 317 alert records, 27 incidents (one per meter and type), 1 repeated record for the same window, 129 alert batches.
- **Against run 4** (old code, same data apart from the empty premises): leak 6.46 to 1.99 s and stuck 6.02 to 2.20 s, because they no longer wait two intervals; silent 6.50 to 8.44 s, because it now waits for a 30-minute watermark instead of 15. In real time, leak and stuck now alert within seconds of the revealing reading; silent 45 minutes after the first missing reading.

**HBase** (`bash scripts/hbase_queries.sh`): `meter_alerts` holds 27 rows, one per incident (count in 0.06 s); a 10-row scan of open alerts in 0.02 s; meter W0000108's 4 newest readings newest first in 0.01 s (7.4 to 7.9 gallons per 15 minutes late at night: the leak). The alert history now uses the window start as each version, so `get 'meter_alerts', 'W0000108#leak'` lists the windows 6 July 03:00 and 7 July 01:00 to 03:00 by their own times.

**Screenshots** (in `screenshots/mac/`): `5-3-4_zone_build_run6a.png`, `5-3-2_run6a_stream_start.png`, `7-1_evaluation_run6a.png`, `5-3-3_hbase_run6a_open_alerts.png`, `5-3-3_hbase_run6a_readings_and_history.png`.

## First container attempt on the new code: not valid (13 November 2026)

The 1,000-meter quick test mixed two datasets: the generator wrote days 1 to 7 but left days 8 to 30 of the earlier 10,000-meter dataset in `data/readings`, and the loader and replay take every day folder. The raw zone held 22,750,987 readings (1.7 GB); 19,871,272 were quarantined as unknown meters. The detection job stopped soon after start-up with nothing in `logs/stream.log` (its errors went to `/dev/null`), most likely overloaded by the old days' 10,000 readings a second with HBase on. The HBase output from that session was still the 12 November run (the reset only empties HBase when it is already running).

Fixed: the generator removes old day folders first; `run_stream.sh` starts HBase before the reset (so the tables are emptied), keeps Spark's errors in `logs/stream_spark.log`, writes `logs/stream.log` unbuffered, and stops the detection job if the script fails; `reset_stream.sh` stops any detection job left running. No figures from this attempt are used.

## Latency fix, hard negatives and restart safety: tests here (13 November 2026)

**What was wrong.** Every check ran in append mode with a 15-minute watermark: a window's alert only fired once a reading 15 minutes past the window's end had arrived. The replay squeezes 15 minutes into 1 second, so window alerts looked 6 to 7 seconds fast (runs 2 to 5), but each waited for two later intervals: about **30 minutes in real time**. Only spike and negative really alerted in seconds. Visible in figure 7 of the draft: the leak window ending 02:30 was raised 4 seconds after its last reading, two replay intervals plus the trigger.

**What changed** (`streaming/detect_stream.py`):

- Leak and stuck run in **update mode** and fire in the micro-batch that brings the reading completing their window. Silent stays in append mode: a missing reading only shows once later readings arrive. Watermark 30 minutes (late readings up to 30 minutes are still counted; it delays only silent).
- **Water stuck** now needs 24 hours of zero while the premise's electricity over the same window is at least 60% of its normal (occupied). Electricity stuck stays at 6 hours (an empty home still draws power).
- **Restart-safe writes**: each micro-batch's alerts overwrite their own folder; HBase alert cells carry the window start as their version.
- File-mode tests take each reading's arrival time from the file's modification time, so latency is measured as with Kafka.

**Test data**: 1,000 meters, 3 days (2 live days), 29 anomalies, 22 empty premises (`--vacant-rate 0.05`), 1% of readings sent 15 or 30 minutes late (1,906 readings). Detection job in file mode on a 2-core machine, so latencies are higher than on the MacBook.

**Replay-speed test** (same data at 1 and 3 seconds per interval):

| Check | Latency at 1 s per interval | Latency at 3 s per interval | Waits for later readings | Real time from anomaly start to alert (median) |
| --- | --- | --- | --- | --- |
| leak | 3.93 s | 1.73 s | no | 930 min (first night after the leak starts) |
| negative | 5.99 s | 2.50 s | no | 0 min |
| spike | 4.67 s | 2.41 s | no | 0 min |
| stuck | 4.27 s | 3.25 s | no | 1,440 min (water needs 24 hours) |
| silent | 12.55 s | 17.35 s | yes | 45 min (15-minute window + 30-minute watermark) |

Latency did not grow for leak, stuck, spike and negative when the replay slowed down (it fell: less load per second), so they fire on the revealing reading and their replay latency is their real latency. Silent grew by 4.8 s over 2 extra seconds per interval: it waits for later readings, as designed.

**Results of the four runs**

| Run | What | Detected | False alerts | Alerts on empty premises | Late readings dropped | Repeated records for one window |
| --- | --- | --- | --- | --- | --- | --- |
| t1 | 1 s per interval | 29 of 29 | 0 | 0 of 22 premises | 0 of 1,906 | 0 |
| t2 | 3 s per interval | 29 of 29 | 0 | 0 | 0 | 0 |
| t3 | killed with `kill -9` after 70 s, restarted from the checkpoints | 29 of 29 | 0 | 0 | 0 | 1 (see below) |
| t4 | final code, 1 s per interval | 29 of 29 | 0 | 0 of 22 premises | 0 of 1,906 | 1 (see below) |

Median latencies in t4: leak 2.69 s, stuck 3.74 s, spike 4.71 s, negative 5.64 s, silent 14.07 s (2-core machine at full replay load). 209 alert records, 29 incidents (one per meter and type) for 29 anomalies.

**The one repeated record** (t3 and t4, the same window both times, so not caused by the restart): the water stuck alert for W0000526 fired when the window's last readings arrived, and fired again one micro-batch later when a late electricity reading for that premise, still inside the window, changed its occupancy figure (0.9904 to 0.9891). Update mode re-sends a window whenever a reading changes it. HBase stores it as the same version (overwritten); the alerts zone keeps both rows and the Kafka topic gets both messages, so consumers should treat meter, type and window start as the alert's key. (The check also waits until both meters' last readings are in, which stops the same thing happening with on-time readings split across two micro-batches.)

**Batch layer from the landing zone**: after t3 (which included the kill), `build_zones.py --source landing` cleansed 191,821 readings for the two streamed days; from the raw zone, the same days give 191,821. The stream's landing copy lost nothing and repeated nothing across the crash.

**Warehouse**: `build_warehouse.py` and both Hive scripts run through Spark SQL in Hive mode on this data: 3 days, 1,000 meters, 3,000 daily usage facts, 33 alert facts; all six reports return results. Not yet run in Hive itself.

## Generator fix: one premise, one type and one area (12 November 2026)

Found while preparing the star schema: the generator drew the premise type and area separately for each meter, so a premise's water and electricity meters usually disagreed (4,772 of 5,000 premises in a 10,000-meter test). The electricity meter now takes its premise's type and area from the water meter. The random draws happen in the same order as before, so:

- **water readings and every injected anomaly are byte-identical** to the earlier data: the leak results and all benchmark figures (water only) are unaffected;
- only electricity consumption levels change (each meter now follows its premise's real type); the share of apartments, villas and commercial premises is unchanged, so the calibration still holds;
- **detection unchanged**: on the 10,000-meter, 3-day test data the fixed generator gives exactly the scale-test result below (leak, negative, silent, spike 100%, stuck 62 of 65 with the same 3 short spells missed, no false alerts).

Runs 1 to 5 used the earlier data. Their detection figures stand; anything that groups by premise or area (the star schema, reports by area) must use data generated after this fix.

## Benchmark 3: batch tools on three times the data (12 November 2026)

`bash scripts/run_benchmark.sh --no-streaming` on 30,000 meters for 30 days (86,396,079 readings, 6.5 GB raw CSV, calibrated generator, 868 anomalies of which 237 leaks). Docker had 10 CPUs and 12 GB of memory; Kafka and HBase were not running. The stream was not run on this data, so there is no streaming row.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 61.4 s | 60.7 to 62.5 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 61 s | 36 |
| Hive (HiveQL on MapReduce) | 71.4 s | 71.2 to 75.1 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 71 s | 16 |
| Spark batch (DataFrames, local mode) | 18.5 s | 17.4 to 20.2 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 19 s | 39 |
| Spark batch (DataFrames, on YARN) | 42.8 s | 42.3 to 43.6 s | 3,366 | 3,366 | 0 | 0 | about 2 h + 43 s | 39 |

**How the tools scale** (all three benchmarks: 54 MB, 2.2 GB and 6.5 GB of raw data; a straight line fits each tool closely):

| Approach | Fixed start-up | Extra time per GB | Runtime at 54 MB / 2.2 GB / 6.5 GB |
| --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | about 15 s | about 7.2 s | 15.1 / 30.3 / 61.4 s |
| Hive (HiveQL on MapReduce) | about 32 s | about 6.0 s | 33.0 / 44.6 / 71.4 s |
| Spark on YARN | about 12 s | about 4.8 s | - / 22.2 / 42.8 s (two sizes) |
| Spark local mode | about 4.5 s | about 2.2 s | 4.2 / 9.9 / 18.5 s |

**What it shows**

- **All four agree exactly** again: 3,366 leaking meter-nights from 237 injected leaks.
- **Two costs, not one.** Each tool has a fixed start-up cost and a cost per gigabyte. Hive has the highest start-up (about 32 s, compiling the query and starting several MapReduce jobs) but processes each extra gigabyte slightly faster than our MapReduce job, which pipes every line through a Python mapper and reducer (Hadoop Streaming) rather than running compiled Java. Spark on YARN has a lower start-up than either and the lowest cost per gigabyte of the three YARN tools.
- **Like for like on YARN at 6.5 GB**: Spark 42.8 s, MapReduce 61.4 s (Spark 30% faster), Hive 71.4 s. The gap grows slowly with the data: Spark's lead over MapReduce went from 27% at 2.2 GB to 30% at 6.5 GB.
- **Why Spark local mode is fastest**: it uses all 10 CPUs directly, while YARN in this container is set to 4 GB of memory for containers, so MapReduce, Hive and Spark on YARN run only a few tasks at once. On a real cluster YARN would have many machines' worth of resources; the local-mode figure shows what this one machine can do without that limit, not a fair comparison with the YARN tools.
- **These are single-machine figures.** They show how the tools behave as the data grows here; they do not predict runtimes on DEWA's data on a cluster, and the report should not extrapolate them that way.
- **Time to alert is unchanged**: whatever the tool, a nightly batch reports a leak about 2 hours after it shows. Streaming (benchmarks 1 and 2) did it in about 6 seconds.

**Screenshot** (in `screenshots/mac/`): `5-5_benchmark_3x.png`.

**Note**: the raw zone now holds this 30,000-meter data; the cleansed, curated and streaming results in HDFS are still from run 5 (10,000 meters). Regenerate with `--meters 10000 --days 30` and reload before running anything that combines them.

## Sensitivity test: smallest detectable leak (12 November 2026)

`spark-submit evaluation/sensitivity.py`: six datasets of 2,000 meters for 7 days (calibrated generator, seed 42, anomaly rate 0.3), identical except for the leak size, 156 leaks each. The leak rule (the one in `benchmark/spark_batch.py`, identical in results to the streaming job) was run at four thresholds. **Run again in the container on 12 November with identical results** (screenshot `7-1_sensitivity_test.png`).

Share of leaks detected (in brackets: share of all nights a leak was running that were flagged):

| Leak size (gallons per 15 min) | Threshold 0.2 | Threshold 0.3 | **Threshold 0.5 (pipeline)** | Threshold 1.0 |
| --- | --- | --- | --- | --- |
| 0.1 to 0.25 | 47% (34%) | 9% (3%) | 1% (0%) | 0% (0%) |
| 0.25 to 0.5 | 100% (100%) | 87% (79%) | 13% (7%) | 0% (0%) |
| 0.5 to 0.75 | 100% (100%) | 100% (100%) | 99% (98%) | 1% (0%) |
| 0.75 to 1 | 100% (100%) | 100% (100%) | 100% (100%) | 15% (9%) |
| 1 to 2 | 100% (100%) | 100% (100%) | 100% (100%) | 100% (99%) |
| 1 to 6 (normal runs) | 100% (100%) | 100% (100%) | 100% (100%) | 100% (100%) |
| **False alerts** (meter-nights, out of 7,000 water meter-nights) | **33** | 0 | 0 | 0 |

**What it shows**

- **The detection limit equals the threshold.** At the pipeline's 0.5 gallons per 15 minutes, leaks of 0.5 and above are caught almost every night; below it, detection collapses (13% for 0.25 to 0.5, 1% below 0.25). A leak of 0.5 gallons per 15 minutes is 48 gallons (about 218 litres) a day, around a fifth of an average Dubai home's use (258 gallons a day, DEWA 2025). Slower drips are invisible to this method at this threshold.
- **The trade-off.** Lowering the threshold to 0.3 catches most 0.25 to 0.5 leaks (87%) with no false alerts on this data; 0.2 catches even smaller ones but starts flagging healthy meters (33 meter-nights, about 0.5%), mostly villas and commercial premises whose normal night use is highest.
- **Why the pipeline keeps 0.5.** The clean result at 0.3 is partly because the same team wrote the generator: real night use is noisier (irrigation, pool pumps, night-shift businesses). Choosing 0.3 from this table would fit the rule to our own synthetic data. In a real deployment the threshold would be set per meter from its own night history (future work), not one value for all.
- Normal-size leaks (1 to 6 gallons, all the main runs) are caught at every threshold up to 1.0, so the main results do not depend on the exact threshold.

## Run 5: full-size run on the old code, superseded by run 6 (12 November 2026)

**Replaced by run 6 as the headline.** Its detection figures stand for the old code, but its 6 to 7 seconds for window checks came from waiting two replay intervals (see the replay-speed test above). Its screenshots are in `screenshots/mac/superseded/`.

**Setup**

| | |
| --- | --- |
| Machine | Maria's MacBook Air (Apple Silicon), one Docker container |
| Data | 10,000 meters (5,000 water, 5,000 electricity), 30 days (1 to 30 July), 28,799,091 readings, calibrated generator (DEWA 2025 statistics), seed 42 |
| History | Day 1, used for the spike baseline (rebuilt automatically: 10,000 meters) |
| Live stream | Days 2 to 30: 27,839,091 readings replayed into Kafka, one 15-minute interval per second (10,000 readings a second), about 46 minutes |
| Detection job | Spark Structured Streaming, micro-batch every 2 seconds, watermark 15 minutes, HBase off (see below) |
| Anomalies injected | 300: 91 leak, 42 negative, 35 silent, 75 spike, 57 stuck |

**Batch layer** (`batch/build_zones.py`): 28,799,091 raw readings, 28,799,049 cleansed, 42 quarantined (exactly the 42 injected negative readings), 0 duplicates, 300,000 curated daily usage rows (10,000 meters x 30 days). Hive registered all 30 daily partitions; the raw zone holds 480,000 readings per meter type per day.

**Detection** (`evaluation/evaluate.py`, latency to `emitted_ts`, first alert per anomaly)

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 91 | 91 | 100% | 0 | 525 min | 5.77 s |
| negative | 42 | 42 | 100% | 0 | 0 min | 1.97 s |
| silent | 35 | 35 | 100% | 0 | 15 min | 7.05 s |
| spike | 75 | 75 | 100% | 0 | 0 min | 2.06 s |
| stuck | 57 | 57 | 100% | 0 | 375 min | 6.55 s |
| **All** | **300** | **300** | **100%** | **0** | | |

- 13,775 alerts raised (repeats from sliding windows), 1,005 alert batches printed.
- **The job kept up with 10 times the load of run 4**: latencies are within about a second of run 4 (1,000 meters), so processing did not fall behind the 10,000 readings a second.
- Every alert was ready within about 7 seconds of the revealing reading reaching Kafka: far under the 5-minute requirement (R2).
- **Stuck 57 of 57** here, but this is partly luck: none of the 57 stuck spells happened to fall in the 6 to 6.75-hour range that the on-the-hour windows can miss (see the scale test below, where 3 of 65 did). The limitation stands and goes in the report.
- **HBase was off for this run.** HBase was shown working in runs 3 and 4 (30 open-alert rows for 30 anomalies, prefix scans in 0.02 s). Writing 10,000 readings a second through its REST server from one laptop would have measured that single-machine bottleneck rather than the pipeline; at DEWA's scale the HBase-Spark connector or bulk loading would write from the cluster's executors instead.

**Storage per zone** (`hdfs dfs -du -h /user/hadoop/dewa`, after the run):

| Zone | Size | Note |
| --- | --- | --- |
| Raw (CSV) | 2.2 GB | the data as received |
| Cleansed (Parquet) | 338.7 MB | the same 28.8 million readings, **6.5 times smaller**: Parquet stores each column together and compresses it |
| Landing (Parquet) | 527.5 MB | every streamed reading; larger than cleansed partly because the stream writes many small files (one set per 2-second micro-batch), which compress less well. Compacting them (for example nightly) is future work |
| Checkpoints | 443.1 MB | the streaming job's offsets and window state, what lets it restart without losing or repeating data |
| Alerts | 6.0 MB | 13,775 alerts |
| Curated | 5.0 MB | 300,000 daily usage rows, dimensions and the baseline |
| Quarantine | 59.7 KB | the 42 rejected readings |

**Hive on the curated zone**: a plain SQL query on `daily_meter_usage` (meters whose lowest night flow stayed above 0.5 gallons) lists the leaking meters from the day after each leak starts, for example W0000504 in Dubai Marina from 3 July at about 3.3 gallons per 15 minutes. The batch layer finds the same leaks the next day; the stream finds them within seconds.

**Screenshots** (in `screenshots/mac/`): `5-3-4_generate_full_size.png`, `5-3-4_zone_build_full_size.png`, `5-3-4_zone_sizes_full_size.png`, `5-3-4_hive_leak_query_full_size.png`, `5-3-2_full_size_run_stream_start.png`, `5-3-2_full_size_leak_alerts.png` (the first leak alerts of the run, W0007004 in Jebel Ali), `7-1_evaluation_full_size.png`.

## Benchmark 2: the leak check in five tools, full size (12 November 2026)

`bash scripts/run_benchmark.sh` on the run 5 data (28,799,091 readings, about 2.3 GB in the raw zone), 3 runs of each tool.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 30.3 s | 28.3 to 31.3 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 30 s | 36 |
| Hive (HiveQL on MapReduce) | 44.6 s | 42.8 to 45.4 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 45 s | 16 |
| Spark batch (DataFrames, local mode) | 9.9 s | 9.4 to 10.7 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 10 s | 37 |
| Spark batch (DataFrames, on YARN) | 22.2 s | 21.5 to 23.5 s | 1,370 | 1,370 | 0 | 0 | about 2 h + 22 s | 37 |
| Spark Structured Streaming | continuous | - | 1,370 | 1,370 | 0 | 0 | 5.8 s | 237 (all 5 checks) |

**What it shows**

- **All five agree exactly** on the 1,370 leaking meter-nights the 91 injected leaks produce.
- Lines of code: the screenshots show 219 for the streaming job; the counter skipped part of that file by mistake (fixed 12 November, checked against a count made with Python's own parser). The correct figure, 237, is in both tables here.
- **Like for like on YARN**: Spark 22.2 s, MapReduce 30.3 s, Hive 44.6 s. Spark is about 27% faster than hand-written MapReduce and twice as fast as Hive. Spark in local mode (9.9 s) shows how much of each figure is YARN job start-up.
- **Scaling** from benchmark 1 (1,000 meters, 7 days) to here (about 43 times the data): MapReduce took 2.0 times as long (15.1 to 30.3 s), Hive 1.35 times (33.0 to 44.6 s), Spark local 2.4 times (4.2 to 9.9 s). Runtimes grew far less than the data, so at 2.3 GB every tool is still dominated by fixed start-up costs. This data is too small to show the gap that large-scale processing would; the report should say so rather than extrapolate.
- **Time to alert decides the question.** Whatever the tool, a nightly batch reports a leak about 2 hours after it first shows; streaming reported it in 5.8 seconds. Batch runtimes of 10 to 45 seconds hardly matter next to the batch schedule. MapReduce and Hive suit the reporting side of the lake (Hive's 16 lines of SQL are the easiest to write and maintain); only the streaming path meets the alerting requirement.

**Screenshot** (in `screenshots/mac/`): `5-5_benchmark_full_size.png`.

## Scale test before the full-size run (12 November 2026)

10,000 meters, 3 days (calibrated generator), streaming job in file mode on a 2-core machine, HBase off.

- **Throughput**: the 2 live days (192 intervals, 1.92 million readings) took about 145 seconds of processing: 0.75 seconds per 15-minute interval. The replay sends one interval per second, so the job keeps up even on 2 cores.
- **Detection**: 300 anomalies, no false alerts. Leak, negative, silent and spike 100%; **stuck 62 of 65 (95.4%)**.
- **Why 3 stuck meters were missed**: the stuck rule uses 6-hour windows that start on the hour. A meter that records zeros for exactly 6 hours but starts at, say, 20:45 never fills a whole on-the-hour window. Detection is certain only for spells of at least 6 hours 45 minutes; the generator makes spells from 6 hours up, and the 3 missed ones lasted 6.0 to 6.25 hours. (The 1,000-meter data had no stuck spell that short.)
- **The fix and its cost**: sliding the stuck windows every 15 minutes instead of every hour caught all 65, but the job then needed about 1.2 seconds per interval on this machine, slower than the replay. The rule was kept as it is for the full-size run; this is a limitation to report (section 6): shorter windows or a finer slide catch more, at the cost of more state and processing.

## Benchmark 1: the leak check in four tools, 1,000 meters (12 November 2026)

`bash scripts/run_benchmark.sh` on the same data as run 4 (1,000 meters, 7 days, 671,792 readings in the raw zone, uncalibrated generator), 3 runs of each tool. Runtime is the wall-clock time of the whole command, start-up included.

| Approach | Runtime (median) | Range | Leak nights found | Correct | Missed | Extra | Time to alert | Lines of code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MapReduce (Hadoop Streaming, Python) | 15.1 s | 12.4 to 15.2 s | 19 | 19 | 0 | 0 | about 2 h + 15 s | 36 |
| Hive (HiveQL on MapReduce) | 33.0 s | 32.9 to 34.0 s | 19 | 19 | 0 | 0 | about 2 h + 33 s | 16 |
| Spark batch (DataFrames, local mode) | 4.2 s | 4.2 to 4.4 s | 19 | 19 | 0 | 0 | about 2 h + 4 s | 36 |
| Spark Structured Streaming | continuous | - | 19 | 19 | 0 | 0 | 6.5 s | 237 (all 5 checks) |

**What it shows**

- **All four agree exactly**: each found the 19 leaking meter-nights the 8 injected leaks produce, nothing missed, nothing extra. The differences below are about the tools, not about what they compute.
- **Hive is the slowest** (33 s, about twice MapReduce): it compiles the query into several MapReduce jobs, each started on YARN. It is the shortest to write (16 lines of SQL), which is why it suits reporting rather than alerting.
- **MapReduce** (15 s) pays one YARN job start-up and writes intermediate results to disk between map and reduce; the rule itself takes 36 lines of hand-written mapper and reducer.
- **Spark batch** (4.2 s) was fastest, but it ran in local mode, inside the container without YARN, so part of its lead is the missing YARN start-up. The benchmark now also runs Spark on YARN (from benchmark 2 on) for a like-for-like comparison.
- **Time to alert** is the decisive column for leak detection. A nightly batch can only report a leak after the night window ends (04:00), about 2 hours after the leak first shows (02:00), whatever the tool. Streaming reported it 6.5 seconds after the revealing reading arrived. The tool's runtime (4 to 33 s) hardly matters next to the batch schedule.
- At 1,000 meters every tool finishes in seconds, so runtime differences are mostly start-up overhead. The full-size run (10,000 meters, 30 days, about 43 times the data) repeats the benchmark to show how each tool scales.

**Screenshot** (in `screenshots/mac/`): `5-5_benchmark_1000_meters.png`.

## Generator calibrated to DEWA's 2025 statistics (12 November 2026)

The generator's daily averages now match DEWA's Annual Statistics Booklet 2025 (details and sources in README, "How the synthetic data is generated"). **Runs 1 to 4 used the earlier, uncalibrated averages**: homes used about 25% too little water and commercial premises about 15% too little; electricity was also lower. Anomalies are unchanged (same meters, same times), because they do not depend on the consumption levels.

Checks after the change:

- **Yearly averages** (a 3-day sample of each month, 2,000 meters, no anomalies): home water 252 gallons a day (DEWA 258), commercial water 707 (708), home electricity 55.1 kWh (52.6), commercial electricity 343 kWh (336). All within 5%.
- **Detection** (300 meters, 4 days, 61 anomalies, the local test from run 1): 61 of 61 detected, no false alerts.
- **Night-flow margin** (default 1,000-meter dataset): the highest one-hour minimum night flow of any healthy water meter is 0.055 gallons for apartments, 0.234 for villas and 0.294 for commercial premises, against the 0.5-gallon leak threshold. Healthy meters stay well clear of it.

## Run 4: one-command run with the corrected latency measure (12 November 2026)

Same setup and data as runs 2 and 3, HBase on, started with `bash scripts/run_stream.sh` (first real run of that script). Latency is now measured to when each anomaly's first alert was computed and ready to publish (`emitted_ts`). **These are the figures to quote** for the 1,000-meter dataset (uncalibrated consumption; the full-size run will use the calibrated generator).

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | 735 min | 6.46 s |
| negative | 4 | 4 | 100% | 0 | 0 min | 2.02 s |
| silent | 6 | 6 | 100% | 0 | 15 min | 6.50 s |
| spike | 4 | 4 | 100% | 0 | 0 min | 1.03 s |
| stuck | 8 | 8 | 100% | 0 | 375 min | 6.02 s |
| **All** | **30** | **30** | **100%** | **0** | | |

- 348 alerts raised, 136 alert batches printed, the same as runs 2 and 3: the result repeats exactly.
- Compared with run 3 re-evaluated (first alert, measured to the micro-batch start), every type rose by about 0.2 to 1.7 seconds: the time Spark takes to compute a micro-batch, as predicted in the correction below.
- Point checks (spike, negative) alert in 1 to 2 seconds; window checks (leak, silent, stuck) in about 6 seconds, because they also wait for their window to close. All are far under the 5-minute requirement (R2).

**Screenshot** (in `screenshots/mac/`): `7-1_evaluation_run4_one_command.png`.

## Correction to the latency figures (12 November 2026)

The "median latency, real" figures in runs 2 and 3 below are too low, for two reasons found in the mid-project review:

1. **Fastest alert, not first alert.** `evaluate.py` took the fastest of each anomaly's alerts. A leak raises an alert for every window that shows it, so this picked the quickest repeat and hid slower first alerts. The clearest case: in run 2 the first silent alert took about 26 seconds (Spark was still starting), but the table shows 4.35 s for silent. The script now takes each anomaly's **first** alert.
2. **Measured to the start of the micro-batch.** `alert_ts` is when the micro-batch that raised the alert started, before Spark had computed it. The streaming job now also records `emitted_ts`, when the alert was computed and ready to publish, and latency is measured to that. In a test here, the gap between the two was about 1.3 to 1.7 seconds per alert.

Reason 2 is fixed from run 4 on (old alerts have no `emitted_ts`, so the script falls back to `alert_ts` and says so). Detection rates and false alert counts are not affected.

**Run 3 re-evaluated with the first alert per anomaly** (same alerts, latency still to the micro-batch start, so add roughly 1.5 s for the true time to alert):

| Type | Median latency, fastest alert (old) | Median latency, first alert (corrected) |
| --- | --- | --- |
| leak | 4.35 s | 4.79 s |
| negative | 1.00 s | 1.00 s |
| silent | 4.24 s | 6.26 s |
| spike | 0.56 s | 0.56 s |
| stuck | 5.25 s | 5.36 s |

Silent moved most: one silent anomaly began while Spark was still warming up at the start of the stream, so its first alert was slow. All types are still about 6 seconds or less, far under the 5-minute requirement. Screenshot: `7-1_evaluation_run3_first_alert.png`.

## Run 3: full pipeline with HBase (11 November 2026)

Same setup and data as run 2, with the streaming job also writing to HBase (`--hbase`) and the batch zones built beforehand. This is the first run of every layer together: Kafka, Spark Structured Streaming, HDFS zones, Hive and HBase.

**Detection** (`evaluation/evaluate.py`)

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | 735 min | 4.35 s |
| negative | 4 | 4 | 100% | 0 | 0 min | 1.00 s |
| silent | 6 | 6 | 100% | 0 | 15 min | 4.24 s |
| spike | 4 | 4 | 100% | 0 | 0 min | 0.56 s |
| stuck | 8 | 8 | 100% | 0 | 375 min | 5.25 s |
| **All** | **30** | **30** | **100%** | **0** | | |

348 alerts raised, the same as run 2: the same data gives the same alerts, so the pipeline is repeatable.

**Effect of writing to HBase**: latencies are within about 0.1 seconds of run 2 for four types; stuck rose from 4.48 to 5.25 seconds. Writing every reading and alert to HBase adds little or nothing to the time to alert. Requirement R2 (under 5 minutes) is met by a wide margin either way.

**HBase serving layer** (`bash scripts/hbase_queries.sh`):

- `meter_alerts` holds **30 rows for 30 anomalies**: the 348 repeat alerts from sliding windows collapse to one row per meter and alert type, which is what an operator's "open problems" list needs. The count query took 0.05 seconds.
- A prefix scan on `W0000108#` in `meter_readings` returned that meter's 4 newest readings (23:45 back to 23:00 on 7 July) in 0.02 seconds, newest first, because the row key holds a reversed timestamp. The readings show the leak: about 7 gallons per 15 minutes late at night.
- `get 'meter_alerts', 'W0000108#leak'` with 10 versions shows the leak's history across two nights (6 and 7 July, 01:00 to 03:00). Two limits to state in the report: the table keeps only the 10 newest versions, and alerts written in the same micro-batch become one version. The complete alert history is in the HDFS alerts zone; HBase is for fast lookups, not the archive.

**HDFS** after the run: raw, cleansed, quarantine, curated, landing, alerts and checkpoints folders under `/user/hadoop/dewa`.

**Screenshots** (in `screenshots/mac/`): `5-3-2_streaming_with_hbase_start.png`, `5-3-2_streaming_with_hbase_stopped.png`, `5-3-3_hbase_count_open_alerts.png`, `5-3-3_hbase_open_alerts_scan.png`, `5-3-3_hbase_meter_readings_and_alert_history.png`, `7-1_evaluation_run3_and_hdfs_zones.png`.

## HBase serving layer, clean screenshots (12 November 2026)

`bash scripts/hbase_queries.sh` on the HBase tables still holding the run 4 data (1,000 meters; HBase was stopped before run 5, so they were not cleared). Same results as run 3: 30 open-alert rows for 30 anomalies (count in 0.06 s), a 10-row scan of open alerts in 0.02 s, meter W0000108's 4 newest readings newest-first in 0.02 s (about 7 gallons per 15 minutes late at night: the leak), and that leak's history as 10 versions across three nights (5 to 7 July). Screenshots: `5-3-3_hbase_queries_clean_1.png`, `5-3-3_hbase_queries_clean_2.png`.

## Batch zones and HBase start-up in the container (11 November 2026)

Same 1,000-meter, 7-day dataset as run 2.

**Zone build** (`batch/build_zones.py`):

| | Readings |
| --- | --- |
| Raw | 671,792 |
| Cleansed | 671,788 |
| Quarantined (negative value) | 4 |
| Duplicates removed | 0 |
| Curated daily usage rows | 7,000 (1,000 meters x 7 days) |

The 4 quarantined readings are exactly the 4 injected negative anomalies. Hive registered the cleansed partitions (7 days) and quarantine partitions (the 4 days with a negative reading) with `MSCK REPAIR TABLE`, and the quarantine query in Hive gives the same count.

**HBase**: `start_hbase.sh` started HBase 2.5.10 and the REST server and created both tables; the streaming job's pre-flight check found them and started 6 queries (landing, hbase_readings, leak, stuck, silent, point). Results in run 3 above.

**Screenshots** (in `screenshots/mac/`): `5-3-4_zone_build_summary.png`, `5-3-4_lake_zones_hive_partitions.png`, `5-3-3_hbase_ready_and_streaming_preflight.png`.

## Run 2: first live run through Kafka (11 November 2026)

**Setup**

| | |
| --- | --- |
| Machine | Maria's MacBook Air (Apple Silicon), one Docker container |
| Software | Hadoop 3.3.6, Hive 3.1.3, Spark 3.5.6, Kafka 3.9.1, Java 8 |
| Data | 1,000 meters (500 water, 500 electricity), 7 days, 671,792 readings, seed 42 |
| History | Day 1 (1 July), used for the spike baseline |
| Live stream | Days 2 to 7: 575,792 readings replayed into Kafka (3 partitions), one 15-minute interval per second |
| Detection job | Spark Structured Streaming, micro-batch every 2 seconds, watermark 15 minutes |
| Anomalies injected in the live days | 30: 8 leak, 4 negative, 6 silent, 4 spike, 8 stuck |

**Detection** (`evaluation/evaluate.py`)

| Type | Injected | Detected | Detection | False alerts | Median delay, simulated | Median latency, real |
| --- | --- | --- | --- | --- | --- | --- |
| leak | 8 | 8 | 100% | 0 | 735 min | 4.41 s |
| negative | 4 | 4 | 100% | 0 | 0 min | 1.09 s |
| silent | 6 | 6 | 100% | 0 | 15 min | 4.35 s |
| spike | 4 | 4 | 100% | 0 | 0 min | 0.45 s |
| stuck | 8 | 8 | 100% | 0 | 375 min | 4.48 s |
| **All** | **30** | **30** | **100%** | **0** | | |

**How to read the two time columns**

- **Latency, real** is the processing delay: from the moment the reading that reveals the anomaly reached Kafka to the moment the alert was raised (measured to the micro-batch start and from the fastest alert; see the correction at the top). This is requirement R2 (under 5 minutes). Every type was under 5 seconds. Point checks (spike, negative) take under about 1 second; window checks (leak, silent, stuck) take about 4 seconds, because they wait for their window to close (2 replay intervals of watermark plus the 2-second trigger).
- **Delay, simulated** is how long after the anomaly started the data could show it, and depends on the anomaly, not the system. A leak only stands out in the quiet night hours (minimum night flow), so leaks that start during the day are caught on their first night: 735 minutes (about 12 hours) median. "Zero for 6 hours" (stuck) can only be known after 6 hours. Spikes and negative readings are visible in the reading itself.

**Comparison with DEWA's published figure**: DEWA reports that its Hydro Insight system detects anomalies within one hour. Our pipeline raises an alert within seconds of the revealing reading. Both are still bound by when the anomaly becomes visible in the data (a leak at night).

**Other observations**

- The Kafka `alerts` topic received 348 alert messages for 30 anomalies. The extras are repeats: a sliding window raises one alert per position that still shows the problem (a leak shows in every 1-hour window from 01:00 to 03:00, a stuck meter in every 6-hour window). The HBase open-alerts table (TODO item 7) collapses these to one row per meter.
- The very first alerts took about 26 seconds, because Spark was still starting while readings queued in Kafka. Afterwards latency settled at the figures above.
- Silent meters show directly in the replay: terminal 2 sent 999 readings per interval instead of 1,000 while meter E0000899 was silent (2 July, 08:00 to 19:15).

**Screenshots** (in `screenshots/mac/`): `5-3-1_*`, `5-3-2_streaming_start_and_first_alerts.png`, `5-3-2_streaming_negative_and_silent_alerts.png`, `5-3-2_streaming_leak_alerts.png`, `5-3-2_streaming_stuck_alerts.png`, `7-1_evaluation_results_1000_meters.png` (crop the Ctrl+C traceback at the top before using it).

## Raw zone in HDFS and Hive (11 November 2026)

1,000 meters, 7 days, 671,792 readings. The Hive timings come from the first load, made with the first version of the generator (before leaks were weighted up): same size, different anomaly mix. Screenshots: `5-3-4_hdfs_raw_zone_load.png`, `5-3-4_hive_mapreduce_jobs.png` and `5-3-4_hive_readings_and_anomalies.png` are from that first load; the `_v2` ones are from the current data.

| Measure | Value |
| --- | --- |
| Raw zone size in HDFS | 51.7 MB (7 daily partitions of about 7.7 MB, plus meters and ground truth) |
| Hive: readings and totals per day and meter type (2 MapReduce jobs) | 31.6 seconds |
| Hive: anomalies per type (1 MapReduce job) | 16.2 seconds |

Hive took 16 to 32 seconds for simple counts on under a million rows, because every query starts MapReduce jobs on YARN. Fine for daily reports; far too slow for alerting. This is early evidence for the tool comparison (report section 4.3); the benchmark (TODO item 10) measures it properly.

Silent meters also show in plain Hive: some days have fewer than 48,000 readings per meter type (500 meters x 96).

## Batch zones and HBase writer: tests (11 and 12 November 2026)

Same 300-meter, 4-day test data as run 1. The container results are in run 3 and in "Batch zones and HBase start-up in the container" above.

**Benchmark test (12 November)**: on calibrated test data (300 meters, 4 days, 17 leaks), the MapReduce mapper and reducer (run through a pipe, as Hadoop Streaming does), the Hive query (run through Spark SQL, which accepts the same HiveQL) and the Spark batch job each found the same 27 leaking meter-nights, exactly the 27 the injected leaks should produce; the streaming job's alerts found the same 27. Timings here are not meaningful: they come from the container run.

**Validation test (12 November)**: 4 bad rows added to the raw data by hand: one with an unreadable timestamp, one from an unknown meter, one with a non-numeric reading, and one duplicate of an existing reading. Result: 115,012 raw, 114,997 cleansed, 14 quarantined (11 negative_value, 1 bad_timestamp, 1 unknown_meter, 1 missing_value), 1 duplicate removed. Every rule caught exactly its own row.

**Zone build** (`batch/build_zones.py`):

| | Readings |
| --- | --- |
| Raw | 115,008 |
| Cleansed | 114,997 |
| Quarantined (negative value) | 11 |
| Duplicates removed | 0 (the generator makes none) |
| Curated daily usage rows | 1,200 (300 meters x 4 days) |

The 11 quarantined readings are exactly the 11 injected negative anomalies: validation catches all of them and nothing else.

**HBase writer** (streaming job with `--hbase`, against a stand-in server that accepts the same REST requests as HBase and checks them):

- `meter_readings`: 86,208 rows, one per streamed reading. Row keys sort each meter's newest reading first, as intended.
- `meter_alerts`: 61 rows for 61 injected anomalies. The repeat alerts from sliding windows collapse into one row per meter and alert type, and the repeats are kept as versions (the alert's history).

**Failure handling**: tested with Kafka stopped, the baseline missing, HBase stopped before the start, HBase stopped in the middle of a run, and the raw zone missing. Each stopped with a banner naming the step, even with `2>/dev/null`; the mid-run case named the exact batch (`writing readings to HBase (batch 1)`).

## Run 1: logic test without Kafka (11 November 2026)

300 meters, 3 live days, 61 anomalies (17 leak, 11 negative, 9 silent, 15 spike, 9 stuck), CSV files instead of Kafka.

- First attempt: spikes 10 of 15 detected, 3 false spike alerts. Two causes found and fixed: night spikes were impossible to detect (the generator multiplied a near-zero night reading), and a threshold of 3 times the baseline flagged one meter's normal busy days. After generating spikes as glitches of 10 to 30 times the meter's peak and raising the threshold to 4 times: all 61 detected, no false alerts.
- Also found: parallel queries writing to the same HDFS folder crash each other; each query now writes to its own folder.

These fixes are worth a sentence in report section 6 (difficulties).

## Caveats (for report section 6)

- **Best-case figures.** The same team wrote the data generator and the detection rules, so the rules suit the data. Real meters are noisier, and real leaks can be smaller than the 1 to 6 gallons per 15 minutes simulated here.
- **Single-machine run.** The largest run is 10,000 meters on one laptop. DEWA had about 2.4 million customer accounts in 2025 (1.28 million electricity and 1.16 million water, Annual Statistics Booklet 2025), roughly 240 times more. The runs show the design works and keeps up at 10,000 readings a second; throughput at DEWA's scale would need a cluster and is not measured here.
- **Anomaly count.** 300 anomalies in the full-size run (30 in the smaller runs). Enough to show the method works across all five types, but each type has 35 to 91 cases, so detection rates are estimates, not guarantees.
- **Window alignment.** Stuck spells shorter than 6 hours 45 minutes can be missed, depending on when they start (scale test). A finer window slide fixes this at a processing cost.
- **HBase not used at full size** (see run 5).
