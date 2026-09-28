<div align="center">

# 🚀 Mini Data Analysis Pipeline

**An end-to-end data pipeline that turns raw data into analytics-ready tables using a Bronze → Silver → Gold architecture, PostgreSQL, and Apache Airflow.**

[🏗️ Architecture](#architecture) | [✨ Features](#key-features) | [🚀 Getting Started](#getting-started) | [🗺️ Roadmap](#roadmap)

![Status](https://img.shields.io/badge/status-in%20progress-yellow)
![Last commit](https://img.shields.io/github/last-commit/nithinarevankar/Mini-Data-Analysis-Pipeline-)
![Stars](https://img.shields.io/github/stars/nithinarevankar/Mini-Data-Analysis-Pipeline-?style=flat)

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)

</div>

---

## What is this?

A small end-to-end data engineering project that moves raw data through a medallion architecture (Bronze → Silver → Gold) into an analytics-ready star schema, orchestrated with Apache Airflow.

> **Status:** In progress. Transformations, data quality checks, and dashboards are being added incrementally.

---

## Overview

This project practices the core building blocks of a modern data pipeline: loading source data with Python, storing it in PostgreSQL, transforming it with SQL, and automating the whole flow with Airflow. The Gold layer is designed to feed BI tools such as Power BI.

## Architecture

```
Source Data
    │
    ▼
Python (extract + load)
    │
    ▼
Bronze  →  Silver  →  Gold
 raw       cleaned    star schema
                          │
                          ▼
                      Power BI
```

Airflow schedules and runs each stage in order: **load → Bronze to Silver → Silver to Gold**.

| Layer  | Purpose                                   |
| ------ | ----------------------------------------- |
| Bronze | Raw data, loaded as-is from the source    |
| Silver | Cleaned, typed, and transformed data      |
| Gold   | Analytics-ready tables (star schema)      |

## Key Features

- **Medallion architecture** with clear separation between raw, cleaned, and analytics layers
- **Incremental loading** using a watermark stored in a metadata table, so only new data is processed on each run
- **Idempotent runs**: re-running the pipeline does not create duplicates
- **Orchestration** with Airflow DAGs
- **Containerized setup** with Docker Compose

## Tech Stack

Python · Pandas · PostgreSQL · SQL · Apache Airflow · Docker · Power BI

## Project Structure

```
.
├── config/              # Configuration files
├── dags/                # Airflow DAG definitions
├── src/
│   └── airflow/         # Pipeline code (load and transform logic)
├── docker-compose.yaml  # Airflow + Postgres services
├── pyproject.toml       # Python dependencies (managed with uv)
└── README.md
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python (version in `.python-version`) and [uv](https://docs.astral.sh/uv/)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/nithinarevankar/Mini-Data-Analysis-Pipeline-.git
cd Mini-Data-Analysis-Pipeline-

# 2. Install dependencies
uv sync

# 3. Create a .env file with your database credentials
#    (see config/ for the expected variables)

# 4. Start the services
docker compose up -d
```

Then open the Airflow UI (by default at `http://localhost:8080`) and trigger the DAG from the `dags/` folder.

## How Incremental Loading Works

1. Read the last successful watermark from the metadata table
2. Load only records newer than that watermark
3. Update the watermark after a successful load

This keeps runs fast and safe to repeat.

## Roadmap

- [ ] Add data quality checks between layers
- [ ] Add more Silver and Gold transformations
- [ ] Publish a Power BI dashboard with screenshots
- [ ] Add automated tests

## What I Learned

ETL/ELT design, SQL transformations, star schema modeling, incremental loading, idempotency, Airflow orchestration, and Docker-based local environments.

## Author

**Nithin** · [GitHub](https://github.com/nithinarevankar)
