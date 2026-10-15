# Data cleaning and encoding report

Run date: 28 September 2026 (overnight). Every number below is copied from the job logs in
`logs/`, which are the evidence for each claim.

## 1. What was built

| Stage | HDFS output | Job | Runtime |
|---|---|---|---|
| Raw | `/data/raw` (day 1 CSV, day 2 JSONL) and `/data/truth` (day 2 original CSV) | uploaded by hand | — |
| Bronze | `/data/bronze` | `ingest/BuildBronze.scala` | 8.8 min |
| Silver | `/data/silver` | `ingest/BuildSilver.scala` | 45.1 min |
| Recovery score | `/data/reports/recovery_*` | `ingest/ScoreRecovery.scala` | 10.8 min |
| Features | `/data/features`, `/models/encoder` | `ingest/BuildFeatures.scala` | 11.2 min |

Shared code: `common/Schema.scala` (the single list of all 86 fields, with CSV header, JSON path and
type) and `common/Session.scala` (Spark session settings used by every job).
`scripts/run-cleaning.sh` runs the jobs in order and stops at the first failure.
`InspectRaw.scala` was deleted as planned; its log remains in `logs/InspectRaw.txt`.

Rerun everything from raw with:

```bash
cd ~/ddos-detection/spark && sbt assembly && ../scripts/run-cleaning.sh BuildBronze BuildSilver ScoreRecovery BuildFeatures
```

### Storage

| Stage | Size | Rows |
|---|---|---|
| Raw CSV + JSON | 51.2 GB | 70,427,637 |
| Bronze (Snappy Parquet) | 3.3 GB (15.5× smaller) | 70,427,637 |
| Silver | 4.3 GB | 69,944,838 |
| Features | 0.8 GB | 11,857,683 distinct vectors |

Silver's 69,944,838 rows and 109,424 benign flows match the old project exactly, which is an
independent cross-check of the whole chain.

## 2. Bronze: two formats, one schema

- Day 1 (11 CSV files, 50,063,112 rows) and day 2 (7 JSONL files, 20,364,525 rows) are read and
  mapped through `Schema` into the same 86 columns. Types are fixed up front, so nothing is guessed.
- No values are changed. The quality report in `logs/BuildBronze.txt` shows the variety clearly:
  - day 1 is almost complete;
  - day 2 has the converter's gaps: about 8% for packet counts, 10% for rates and timing, 20% for
    active/idle, and 75.8% for the TCP flag block, which is absent for every non-TCP flow.
- `SimillarHTTP` holds text (URLs), so it is kept as a string column, not a number.

## 3. Silver: cleaning

Each step, in order:

1. **Labels unified into 14 families.**
   - `DrDoS_DNS` and `DNS` become `DNS` (likewise for the other families).
   - `UDP-lag` becomes `UDPLag`, and `BENIGN` becomes `Benign`.
   - `is_attack` is added as a 0/1 column.
2. **Timestamps parsed**, with 0 failures on both days. The odd dates in CIC's own data are kept
   unchanged.
3. **TCP flags restored for non-TCP flows.** These fields are 0 by definition for UDP flows, and 0 is
   what the original CSV contains. This is an exact fill of 15,437,140 rows × 12 fields.
4. **Duplicate records merged (day 2).**
   - On the original data, rows that share a connection key (flow ID, timestamp, IPs, ports,
     protocol) are always complete copies. This was verified on the ground truth: 19,881,726
     distinct keys = 19,881,726 distinct full rows.
   - So day-2 copies were merged by key, **482,799 in total**, the same count the old project found.
   - Any value present in one copy is the true value for all copies, so merging **restored 6,272,462
     missing values exactly**.
   - Day 1 has no duplicate records: all 50,063,112 keys are distinct.
   - The pipeline itself never reads the ground truth. The truth was only used to validate the rule.
5. **Missing values recovered, in tiers:**
   - **Tier 1, exact rules.**
     - 35 arithmetic identities between CIC features were proposed. Examples: variance = std²;
       bytes/s = (fwd + bwd bytes) ÷ duration; duration = mean IAT × (packets − 1).
     - Each was tested on all 50 M complete day-1 rows, and only rules holding on ≥ 99.9% were used.
     - **32 passed at 100.0000%. 3 failed and were not used:**
       - `fwd_pkts_s` from packets ÷ duration: 97.28%;
       - `bwd_pkts_s`, the same: 97.28%;
       - `pkt_size_avg` = bytes ÷ packets: 4.18%.
     - The rules run in up to 5 rounds, because recovering one value can unlock another.
   - **Tier 2, per-protocol median** of the values actually observed on the same day.
   - **Tier 3, MLlib `Imputer`** (median) as a safety net. It filled 0 values, because every protocol
     group had a tier-2 median.
   - **Labels are never used for recovery.**
   - Each row records `missing_fields`, `tier1_fields`, `tier2_fields` and `tier3_fields`.
6. **NaN and Infinity.**
   - These come from dividing by a zero duration in `flow_byts_s` and `flow_pkts_s`.
     - Infinity: 1,114,212 and 1,363,236 on day 1; 710,239 and 710,254 on day 2.
     - Day 1 also has 249,024 blank `flow_byts_s` values, which is how CIC wrote 0/0. They were
       recomputed by rule as 0/0.
   - Rule: NaN becomes 0, and +Infinity becomes day 1's largest finite value of that column
     (2.944e9 bytes/s and 4e6 packets/s). The same cap is applied to both days.
7. **Result:** 0 null feature values remain, and the connection key is unique on both days.

## 4. How well the recovery worked (day 2, scored against the ground truth)

All 19,881,726 silver day-2 rows were matched one-to-one to their original CSV row.

| How the value came back | Values | Exactly right |
|---|---|---|
| Merged from a duplicate copy | 6,272,462 | 100% (copies are identical) |
| Tier 1, exact rule | 39,848,897 | **100.00%** |
| Tier 2, protocol median | 94,603,325 | 84.23% (see below) |
| Tier 3, Imputer | 0 | — |

In total, **46.1 M of the 140.7 M missing day-2 values (33%) were restored exactly**, and the rest
were estimated.

**The tier-2 figure of 84% is flattering, and should not be quoted alone.** Many fields are usually
0, so a median of 0 is trivially right. On fields that genuinely vary, tier 2 is weak:

| Field | Exact | Within 10% |
|---|---|---|
| `flow_byts_s` | 2.2% | 9.8% |
| `pkt_size_avg` | 11.9% | 30.7% |
| `pkt_len_mean`, `fwd_pkt_len_*` | ~21% | 40–49% |
| `fwd_header_len` | 23.9% | 23.9% |
| `flow_duration` (the part tier 1 could not rebuild) | 44.9% | 46.5% |
| `fwd_pkts_s` | 50.6% | 52.9% |
| `fwd_iat_*` | 62–72% | 62–72% |
| active/idle, bulk, `bwd_*` | 92–100% | 92–100% |

Honest summary for the presentation: *exact rules restore about a third of the gaps perfectly;
simple medians are a weak fallback for the fields that vary most.* The effect on detection accuracy
(training on true vs recovered day 2) is the next measurement to make, in the training phase.

## 5. Features: encoding for an honest model

### Inputs excluded because they identify the testbed, not the traffic

`flow_id`, `timestamp`, `src_ip`, `dst_ip`, `src_port`, `dst_port`, `inbound`, `init_fwd_win_byts`,
`init_bwd_win_byts`, `fwd_seg_size_min`, `similar_http`. The reasons are listed in
`/data/reports/feature_list`. They stay in Silver for the dashboard, but never reach the model.

### Dropped automatically, decided on the training split only

- **12 constant columns:**
  - 5 TCP flag fields: `bwd_psh_flags`, `fwd_urg_flags`, `bwd_urg_flags`, `fin_flag_cnt`,
    `psh_flag_cnt`;
  - `ece_flag_cnt`;
  - all 6 bulk features.
- **7 columns equal to another column on every row:**
  - `fwd_seg_size_avg` = `fwd_pkt_len_mean`, and `bwd_seg_size_avg` = `bwd_pkt_len_mean`;
  - `rst_flag_cnt` = `fwd_psh_flags`;
  - the 4 subflow columns = the 4 totals.

**55 model features remain**, listed in `logs/BuildFeatures.txt`.

### Repeated records collapsed

This is the key honesty step. **84% of day-1 flows have exactly the same 55 measurements as another
flow**, differing only in identifiers.
- Day 1: 50,063,112 flows became 8,209,191 distinct (vector, label) rows.
- Day 2: 19,881,726 flows became 3,648,492.
- An `occurrences` column keeps the count, so nothing is lost, and it can serve as a weight in
  training.

This follows the practice that produced NSL-KDD from KDD'99. With a random split and no collapse,
the test set would be mostly exact copies of training rows, and the model would be graded partly on
memory.

### Split

- 80/20, by a hash of the **measurement vector**, so identical vectors can never appear in both
  train and test.
- Benign share after collapsing: 0.55% of rows on day 1 and 1.2% on day 2, up from 0.16%.

### Encoder

`StringIndexer` (14 families) plus `VectorAssembler` (55 features), fitted on train and saved to
`/models/encoder` for the streaming job to reuse.

## 6. The most important finding: family labels are not in the traffic

Identical measurement vectors that carry different labels:

| Day | Vectors with more than one **family** | Flows affected | Vectors that are both **attack and benign** | Flows affected |
|---|---|---|---|---|
| 1 | 190,130 | **34,687,362 (69%)** | 74 | 4,707 |
| 2 | 24,890 | 7,738,541 (39%) | 32 | 1,083 |

- **Attack vs benign is almost fully determined by the measurements.** The ceiling is about
  99.99% of flows.
- **Attack family is not.** The same packet pattern is labelled DNS in one time slot and LDAP or SNMP
  in another. This is consistent with CIC labelling by time slot plus attacker IP, and it explains
  the old project's 0.80 ceiling on family attribution.
- **Recommendation:** make binary detection the headline result. Present family attribution
  together with this conflict table, so its ceiling is explained up front rather than discovered by
  the examiner.

## 7. Operational notes

1. **Empty folders after a Docker Desktop restart.**
   - When Docker Desktop restarts before Ubuntu is open, the Spark containers start with **empty
     shared folders**: no config, no jar. That's what caused the first two failed runs tonight.
   - Fix: once Ubuntu is open, run
     `docker compose restart spark-master spark-worker spark-history`.
   - A permanent fix would need a change to `docker-compose.yml`, which I did not touch.
2. **The "Code grows beyond 64 KB" warning** in `logs/BuildSilver.txt` is not an error. Spark falls
   back to a slower but correct execution mode for the very wide recovery step, which is why Silver
   takes 45 minutes.
3. **HDFS data** lives in Docker volumes and is unaffected by container restarts.

## 8. Decisions for you

1. **Headline metric:** binary detection, with family attribution as a secondary result that shows
   the conflict table?
2. **Evaluation:** the within-day 80/20 split is ready. A second, stricter test (train on one day,
   test on the other) is recommended; the data supports it without changes.
3. **The tier-3 Imputer** never fires on this data. Keep it as a safety net, or remove it to
   simplify the Silver job?
4. **The permanent bind-mount fix** in `docker-compose.yml`: allow it?
