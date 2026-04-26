# AKI Trajectory Pipeline — Corrected Architecture Flow
> DATA 228 · Group 6 — Nikitha, Karthik, Charvee, Nikhil, Naveen

---

## 🎯 Goal

Build a **real-time early warning system** that predicts progression from early AKI
(KDIGO Stage 1–2) to severe AKI (Stage 3) or dialysis within **48–72 hours**, using:

- **MIMIC-IV** → development dataset
- **eICU (synthetic demo)** → external validation dataset
- **Full KDIGO labels** = creatinine + urine output (not creatinine-only)

---

## 🏛️ Storage Architecture — Medallion Layers

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER   │  Raw GE-validated data  │  Apache Iceberg     │
├─────────────────────────────────────────────────────────────────┤
│  SILVER LAYER   │  Cleaned by dbt         │  Apache Iceberg     │
├─────────────────────────────────────────────────────────────────┤
│  GOLD LAYER     │  KDIGO-labeled features │  Apache Iceberg     │
└─────────────────────────────────────────────────────────────────┘
```

All three layers live in **Apache Iceberg** (versioned, ACID, time-travel capable).

---

## 🔁 Full End-to-End Architecture Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          RAW INPUT DATA                                      │
│   MIMIC-IV CSVs (330M+ events)        Synthetic eICU CSVs                   │
│   (labs, vitals, fluids, meds)        (labs, vitals, fluids, meds)           │
└──────────────────────────┬───────────────────────────┘
                           │
                           ▼
          ┌────────────────────────────────────┐
          │     GREAT EXPECTATIONS (GE)        │  ← Quality Gate #1
          │  ✔ creatinine in realistic range   │
          │  ✔ no negative urine output        │
          │  ✔ no missing patient IDs          │
          │  ✔ valid timestamps                │
          │  ✔ enough urine docs per hospital  │
          └────────────────┬───────────────────┘
                           │  reject bad rows
                           ▼
          ┌────────────────────────────────────┐
          │         BRONZE LAYER               │
          │   (raw but GE-validated data)      │
          │         Apache Iceberg             │
          └────────────────┬───────────────────┘
                           │
                           ▼
          ┌────────────────────────────────────┐
          │              dbt                   │
          │  - normalize units across datasets │
          │  - join tables (labs + vitals +    │
          │    fluids + meds)                  │
          │  - standardize timestamps          │
          │  - remove duplicates               │
          │  - map MIMIC ↔ eICU schemas        │
          └────────────────┬───────────────────┘
                           │
                           ▼
          ┌────────────────────────────────────┐
          │         SILVER LAYER               │
          │   (clean, trusted, joined data)    │
          │         Apache Iceberg             │
          └────────────────┬───────────────────┘
                           │
              ┌────────────┴─────────────┐
              │                          │
              ▼                          ▼
    ═══════════════════        ══════════════════════
    ║   BATCH PATH      ║      ║  STREAMING PATH    ║
    ═══════════════════        ══════════════════════
              │                          │
              ▼                          ▼
    ┌─────────────────┐        ┌──────────────────────┐
    │  dbt + Spark SQL│        │   Kafka Producer      │
    │                 │        │                       │
    │  - baseline     │        │  Replays Silver layer │
    │    creatinine   │        │  events in correct    │
    │  - creatinine   │        │  patient time order   │
    │    ratio/deltas │        │  (simulates live ICU) │
    │  - rolling urine│        │                       │
    │    windows      │        │  One topic per type:  │
    │  - KDIGO Stage  │        │  labs / vitals /      │
    │    0–3 labeling │        │  fluids / meds        │
    └────────┬────────┘        └──────────┬───────────┘
             │                            │
             ▼                            ▼
    ┌─────────────────┐        ┌──────────────────────┐
    │   GOLD LAYER    │        │ Spark Structured      │
    │                 │        │ Streaming Consumer    │
    │  KDIGO-labeled  │        │                       │
    │  hourly feature │        │  - reads Kafka events │
    │  store          │        │  - updates rolling    │
    │  Apache Iceberg │        │    creatinine/urine   │
    └────────┬────────┘        │    per patient/hour   │
             │                 │                       │
             ▼                 │  Streaming Algorithms:│
    ┌─────────────────┐        │  ✦ DGIM              │
    │  Model Training │        │    (urine output      │
    │  (Spark MLlib)  │        │     sliding window)   │
    │                 │        │  ✦ Flajolet-Martin   │
    │  - Logistic     │        │    (distinct lab      │
    │    Regression   │        │     draw sketches)    │
    │  - Gradient     │        │  ✦ Bloom Filter      │
    │    Boosted Trees│        │    (Kafka dedup)      │
    │  tracked via    │        │  ✦ Reservoir Sampling│
    │  MLflow         │        │    (balanced batches) │
    └────────┬────────┘        └──────────┬───────────┘
             │                            │
             ▼                            ▼
    ┌─────────────────┐        ┌──────────────────────┐
    │   EVALUATION    │        │  Live Risk Scores     │
    │                 │        │  (hourly, 0–1)        │
    │  - AUROC, AUPRC │        │  written back to      │
    │  - Calibration  │        │  Apache Iceberg       │
    │  - eICU 208-    │        └──────────────────────┘
    │    hospital     │
    │    validation   │
    │  - SHAP         │
    │  - Subgroup     │
    │    fairness     │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  GE OUTPUT CHECK│  ← Quality Gate #2
    │  (validate that │
    │   model outputs │
    │   are sane)     │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ PRIVACY &       │
    │ FAIRNESS        │
    │  - K-Anonymity  │
    │    (k ≥ 5)      │
    │  - Differential │
    │    Privacy      │
    │    (Google DP)  │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  FINAL REPORT   │
    │  SLIDES + DEMO  │
    └─────────────────┘
```

---

## 🔑 Key Clarifications (Common Misconceptions)

### ❌ WRONG: KDIGO labeling happens inside the streaming path
### ✅ RIGHT: KDIGO labeling happens in the **BATCH path** (dbt + Spark SQL → Gold layer)

The streaming path does **NOT** relabel KDIGO from scratch.
It reads events in real time, updates rolling features (creatinine/urine per hour),
and applies the **already-trained model** to compute a live risk score.

---

### ❌ WRONG: Great Expectations runs only once at the start
### ✅ RIGHT: GE runs at **two checkpoints**

| Checkpoint | When | What it checks |
|---|---|---|
| **GE #1 — Ingestion gate** | Before Bronze layer | Raw data quality (ranges, nulls, timestamps) |
| **GE #2 — Output gate** | After model scoring | That model outputs are valid and sane |

---

### ❌ WRONG: There are only 2 layers (raw + clean)
### ✅ RIGHT: There are **3 layers** (Medallion architecture)

| Layer | Content | Technology |
|---|---|---|
| **Bronze** | GE-validated raw data | Apache Iceberg |
| **Silver** | dbt-cleaned & joined | Apache Iceberg |
| **Gold** | KDIGO-labeled feature store | Apache Iceberg |

---

## 📦 4 Streaming Algorithms — Where & Why

These live **inside Spark Structured Streaming** (streaming path only):

| Algorithm | Purpose |
|---|---|
| **DGIM** | Count urine output events in a sliding time window without storing all events |
| **Flajolet-Martin** | Estimate distinct lab draw types per patient using sketching (memory efficient) |
| **Bloom Filter** | Deduplicate Kafka events — avoid reprocessing the same event twice |
| **Reservoir Sampling** | Build balanced mini-batches for online/incremental model training |

---

## 📊 Technology Stack Summary

| Component | Technology |
|---|---|
| Versioned storage | Apache Iceberg (on Parquet) |
| ETL / Cleaning | dbt → Spark SQL |
| Data Quality | Great Expectations (2 checkpoints) |
| Batch processing | Apache Spark SQL + DataFrame API |
| Streaming ingest | Apache Kafka (1 topic per event type) |
| Stream processing | Spark Structured Streaming |
| Modeling | Spark MLlib (LR + GBT) |
| Experiment tracking | MLflow (linked to dbt run ID) |
| Privacy | K-Anonymity (k≥5) + Differential Privacy (Google DP) |
| Fairness | SHAP + subgroup analysis |
| Environment | Docker + Conda + GitHub |

---

## ✅ What's Done vs ❌ What's Left

| Stage | Status |
|---|---|
| Synthetic eICU batch pipeline (clean → features → KDIGO) | ✅ Done |
| MIMIC-IV batch pipeline scripts | ✅ Written, needs to run |
| Kafka producer + topic setup | ✅ Written, needs live Kafka env |
| Spark Structured Streaming consumer (core) | ✅ Written, needs live Spark + Kafka |
| MLlib model scripts (LR + GBT) | ✅ Written, not yet trained |
| Evaluation metric code | ✅ Written, no results yet |
| Great Expectations full setup | ⚠️ Basic checks only |
| dbt models | ❌ Folder exists, nothing written |
| Apache Iceberg storage | ❌ Using plain CSVs currently |
| 4 Streaming algorithms (DGIM, FM, BF, RS) | ❌ Not yet added to streaming job |
| MLflow tracking | ❌ Not implemented |
| Privacy & fairness (K-Anon, DP, SHAP) | ❌ Not implemented |
| Final report / slides / Docker demo | ❌ Not done |
