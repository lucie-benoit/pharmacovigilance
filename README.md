# Drug Safety & Pharmacovigilance Analytics

Big Data analytics platform built with Apache Spark, MongoDB, and Docker to extract drug safety indicators from the FDA FAERS dataset.

**EFREI — Cloud Computing for Health | M. ZOGHLAMI**  
Anne-Bérékia ECHARD & Lucie BENOIT

---

## Overview

This project processes millions of adverse drug event reports from the FDA FAERS dataset and computes safety indicators such as risk scores, signal detection, and temporal trends. Results are stored in MongoDB and can be visualized with MongoDB Compass.

---

## Project Structure

```
pharmacovigilance/
├── docker-compose.yml         # Defines all services (Mongo, Spark batch, Spark streaming)
├── scripts/
│   ├── faers_schema.py        # Shared schema definition and column mapping
│   ├── pharma_batch.py        # Batch pipeline — core analyses
│   ├── pharma_streaming.py    # Streaming pipeline — real-time extension
│   └── split_dataset.py       # Splits the full dataset into chunks for streaming demo
└── data/
    ├── full/                  # Full FAERS CSV (used by batch)
    └── input/                 # Watched folder (used by streaming)
```

---

## Dataset

FDA Drug Adverse Event Reports 2015–2026 (FAERS) — available on [Kaggle](https://www.kaggle.com/datasets/kanchana1990/fda-drug-adverse-event-reports-2015-to-2026-faers).

Download the CSV and place it in `data/full/`.

Because the FAERS schema differs from the assignment example, `faers_schema.py` derives the required columns:

| New column | Source |
|---|---|
| `severity` | `"Severe"` if `serious == "Yes"` |
| `seriousness_score` | `is_fatal×4 + is_life_threat×3 + is_hospitalized×2 + is_disabling×1` |
| `adverse_event` | `primary_reaction` |
| `outcome` | `patient_recovered` |

---

## Data Setup

After downloading the CSV from Kaggle, place it in `data/full/`:
pharmacovigilance/
└── data/
├── full/
│   └── fda_adverse_events_2015_2026_CLEAN.csv
├── input/
└── input_chunks/

Create the folders manually if they don't exist:

```bash
mkdir -p pharmacovigilance/data/full
mkdir -p pharmacovigilance/data/input
mkdir -p pharmacovigilance/data/input_chunks
```

To generate the chunk files used by the streaming pipeline, run:

```bash
python scripts/split_dataset.py
```

This splits the full CSV into smaller files stored in `data/input_chunks/`. During the demo, move chunk files one by one into `data/input/` to simulate real-time ingestion.

---

## Architecture

Three Docker services defined in `docker-compose.yml`:

- **mongo** — MongoDB 7.0, port 27017, stores all analysis results
- **spark-batch** — one-shot container, runs `pharma_batch.py` on the full dataset and terminates
- **spark-streaming** — long-running container, runs `pharma_streaming.py`, watches `data/input/` for new files every 30 seconds

---

## Getting Started

### Prerequisites

- Docker Desktop installed and running
- FAERS CSV placed in `data/full/`

### Run the batch pipeline

```bash
docker compose up spark-batch
```

This computes all core analyses and writes results to MongoDB.

### Run the streaming pipeline

First split the dataset into chunks:

```bash
python scripts/split_dataset.py
```

Then start the streaming service:

```bash
docker compose up spark-streaming
```

Drop chunk files one by one into `data/input/` to simulate real-time ingestion. Spark picks up each new file within 30 seconds.

### Run everything

```bash
docker compose up
```

### View results

Open [MongoDB Compass](https://www.mongodb.com/products/compass) and connect to:

```
mongodb://localhost:27017
```

Open the `pharmacovigilance` database to browse all collections.

---

## Analyses

### Batch — core indicators

| Collection | Description |
|---|---|
| `top_drugs` | Top reported drugs |
| `number_of_reports` | Number of reports by drug |
| `severe_reports` | Number of severe reports by drug |
| `adverse_events` | Most frequent adverse events |
| `severe_adverse_events` | Most severe adverse events |
| `monthly_reports` | Monthly evolution of reports |
| `severe_monthly_reports` | Monthly evolution of severe cases |
| `risk_score` | Custom risk score per drug (40% volume, 60% severity) |
| `signal_detection` | Drug–reaction signal score (30% volume, 70% severity) |
| `hospitalization_by_drug` | Hospitalization rate per drug |
| `fatality_by_drug` | Fatality rate per drug |

### Streaming — real-time indicators

| Collection | Description |
|---|---|
| `population_at_risk` | Reports by age group |
| `mean_severity_by_age` | Mean severity by age group |
| `reports_by_manufacturer` | Reports by manufacturer |
| `risk_by_manufacturer` | Risk score by manufacturer |

---

## Tech Stack

- **Apache Spark 3.5.0** — distributed data processing (PySpark)
- **MongoDB 7.0** — NoSQL result storage
- **Docker & Docker Compose** — containerized deployment
- **mongo-spark-connector** — direct DataFrame writes to MongoDB