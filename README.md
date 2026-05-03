# BikeSpark

A modern data pipeline that ingests, transforms, and visualizes Citi Bike trip data using Apache Spark, Airflow, dbt, ClickHouse, and Metabase.

## Overview

BikeSpark is an end-to-end ELT (Extract, Load, Transform) pipeline that processes Citi Bike trip data. It downloads historical trip data, ingests it into ClickHouse using Apache Spark, orchestrates the workflow with Apache Airflow, transforms raw data into analytics-ready models using dbt, and provides interactive dashboards via Metabase.

## Tech Stack

| Component        | Technology                    |
| ---------------- | ----------------------------- |
| Orchestration    | Apache Airflow                |
| Processing       | Apache Spark 3.5.0            |
| Transformation   | dbt (with ClickHouse adapter) |
| Warehouse        | ClickHouse                    |
| Metadata Store   | PostgreSQL                    |
| BI/Visualization | Metabase                      |
| Containerization | Docker Compose                |
| Task Runner      | Just                          |

## Project Structure

```
bikespark/
├── compose.yml                 # Main Docker Compose entrypoint
├── justfile                    # Just task runner commands
├── airflow/
│   ├── compose.yml             # Airflow service
│   ├── Dockerfile              # Custom Airflow image
│   ├── requirements.txt        # Python dependencies
│   └── dags/
│       └── citibike_elt_dag.py # Main orchestration DAG
├── spark/
│   ├── compose.yml             # Spark master/worker services
│   ├── download_citibike.sh    # Script to download Citi Bike dataset
│   ├── jars/
│   │   └── clickhouse-jdbc-0.9.4-all.jar
│   ├── citibike_2014/          # Raw CSV data (organized by month)
│   └── jobs/
│       └── ingestion_job.py    # Spark job: CSV → ClickHouse
├── dbt/
│   ├── compose.yml             # dbt runner service
│   ├── Dockerfile              # Custom dbt image
│   ├── pyproject.toml          # Python project config
│   ├── profiles/
│   │   └── profiles.yml        # dbt connection profiles
│   └── citibike_project/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/
│           │   └── srg_trips.sql   # Staging: clean & rename columns
│           └── marts/
│               ├── fact_trips.sql  # Fact table: trip facts with surrogate keys
│               ├── dim_station.sql # Dimension: unique stations
│               └── dim_user.sql    # Dimension: user profiles
├── clickhouse/
│   └── compose.yml             # ClickHouse database service
├── postgres/
│   ├── compose.yml             # PostgreSQL service
│   └── metadata-init/
│       ├── airflow-init.sql    # Airflow metadata database
│       └── metabase-init.sql   # Metabase metadata database
└── metabase/
    └── compose.yml             # Metabase BI service
```

## Prerequisites

- **Docker** & **Docker Compose** (v2+)
- **Just** (task runner) — optional, Docker Compose commands can be used directly
- **GNU/Linux or macOS** (WSL2 supported on Windows)
- **wget** and **unzip** (for downloading the dataset)
- Minimum **8 GB RAM** recommended

## Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd bikespark
```

### 2. Configure Environment

Generate the Airflow environment file with your user ID and absolute project path:

```bash
echo -e "AIRFLOW_UID=$(id -u)\nABSOLUTE_PATH=$(pwd)" > airflow/.env
```

### 3. Download the Dataset

Run the download script to fetch the 2014 Citi Bike trip data and the ClickHouse JDBC driver:

```bash
chmod +x spark/download_citibike.sh
./spark/download_citibike.sh
```

### 4. Start the Pipeline

Launch all services:

```bash
just up
# or: docker compose up -d
```

### 5. Run the ELT Pipeline

Navigate to the Airflow UI (default: `http://localhost:8080`), find the `citibike_elt_dag` DAG, and trigger it manually. The pipeline will:

1. **Create** the `raw_trips` table in ClickHouse
2. **Ingest** all CSV files into ClickHouse via Spark
3. **Transform** raw data into staging and mart models via dbt

## Available Commands

| Command                  | Description                         |
| ------------------------ | ----------------------------------- |
| `just`                   | List all available commands         |
| `just up`                | Start all services                  |
| `just down`              | Stop all services                   |
| `just down-all`          | Stop services and remove volumes    |
| `just rebuild`           | Rebuild and restart all services    |
| `just logs <service>`    | Follow logs for a service           |
| `just restart <service>` | Restart a specific service          |
| `just shell <service>`   | Open a shell in a running container |
| `just ps`                | Show container status               |

## Data Pipeline Flow

```
CSV Files (Citi Bike 2014)
        │
        ▼
┌───────────────┐
│  Apache Spark │  ← Extract & Load (raw_trips)
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  ClickHouse   │  ← Data Warehouse
└───────┬───────┘
        │
        ▼
┌───────────────┐
│      dbt      │  ← Transform (staging → marts)
└───────┬───────┘
        │
        ▼
┌───────────────┐
│   Metabase    │  ← Visualization & Dashboards
└───────────────┘
```

All steps are orchestrated by **Apache Airflow** via the `citibike_elt_dag` DAG.

## dbt Models

| Model         | Type      | Description                                   |
| ------------- | --------- | --------------------------------------------- |
| `stg_trips`   | Staging   | Renames columns from raw format to snake_case |
| `fact_trips`  | Fact      | Trip records with surrogate keys              |
| `dim_station` | Dimension | Deduplicated station information              |
| `dim_user`    | Dimension | Aggregated user profiles                      |

## Service Ports

| Service      | Port | URL                   |
| ------------ | ---- | --------------------- |
| Airflow      | 8080 | http://localhost:8080 |
| Metabase     | 3000 | http://localhost:3000 |
| ClickHouse   | 8123 | http://localhost:8123 |
| Spark Master | 8081 | http://localhost:8081 |
