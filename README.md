
# 🚀 Mini Data Analysis Pipeline

A small end-to-end data engineering project built to understand how data moves from raw data to an analytics-ready format.

The main goal of this project is to practice **Python, PostgreSQL, SQL, Airflow, ETL/ELT, incremental loading, and data warehouse concepts**.

---

## 🏗️ Architecture

```text
              📁 Source Data
                   │
                   ▼
             🐍 Python
            Data Loading
                   │
                   ▼
            🥉 Bronze Layer
            Raw / Clean Data
                   │
                   ▼
            🥈 Silver Layer
          Transformed Data
                   │
                   ▼
             🥇 Gold Layer
          Analytics / Star Schema
                   │
                   ▼
             📊 Power BI

The pipeline can be automated using Apache Airflow.

⏰ Airflow Scheduler
        │
        ▼
   Load Source Data
        │
        ▼
   Bronze → Silver
        │
        ▼
   Silver → Gold
        │
        ▼
   📊 Analytics


---

🛠️ Tech Stack

🐍 Python

🐼 Pandas

🐘 PostgreSQL

🔄 Apache Airflow

🧮 SQL

📊 Power BI

🐳 Docker

🌱 Git & GitHub



---

📂 Project Structure

Mini-Data-Analysis-Pipeline/
│
├── config/              # Configuration files
│
├── dags/                # Airflow DAGs
│
├── src/
│   └── airflow/         # Python pipeline code
│
├── logs/                # Airflow logs (ignored by Git)
│
├── .env                 # Environment variables (not committed)
├── .gitignore
├── docker-compose.yaml
├── pyproject.toml
└── README.md

---

🔄 Data Pipeline

1️⃣ Extract

Source data is collected and loaded using Python.

2️⃣ Bronze

The raw data is stored in the Bronze layer.

3️⃣ Silver

Data is cleaned and transformed using Python/SQL.

4️⃣ Gold

The transformed data is organized for analytics and reporting.

5️⃣ Visualization

The Gold layer can be connected to Power BI for dashboards and analysis.


---

⚡ Incremental Loading

The pipeline uses a watermark / metadata approach to avoid processing the same data repeatedly.

The last successfully loaded value is stored in a metadata table.

New Data
   │
   ▼
Check Last Watermark
   │
   ▼
Load Only New Data
   │
   ▼
Update Watermark

This makes the pipeline more efficient and helps with idempotent processing.


---

🗄️ Database Layers

The project follows a simple medallion-style architecture:

Layer	Purpose

🥉 Bronze	Raw / initial data
🥈 Silver	Cleaned and transformed data
🥇 Gold	Analytics-ready data



---

📊 Analytics

The Gold layer is designed to provide data that can be easily used for:

📈 EDA

📊 Dashboards

🔎 Business analysis

📋 Reports

📌 KPI calculations



---

🎯 What I'm Learning

This project is mainly for learning and practicing:

ETL pipelines

Data cleaning

SQL transformations

PostgreSQL

Data warehouse concepts

Star schema

Incremental loading

Watermarks

Idempotency

Airflow orchestration

Docker

Git/GitHub



---

🚧 Current Status

🟢 Pipeline development in progress.

I'm gradually adding more transformations, automation, data quality checks, and analytics features.

👨‍💻 About

This is a personal learning project where I'm building a complete data pipeline from source data to analytics.

Built while learning Data Engineering and Analytics.

⭐ Feel free to explore the project!
