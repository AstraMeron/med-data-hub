# med-data-hub

# Medical Data Hub: End-to-End Orchestrated Pipeline

This repository contains a production-ready data engineering pipeline developed for the 10 Academy KAIM training. It automates the extraction, transformation, and enrichment of medical data from Ethiopian Telegram channels.

## 🏗 System Architecture
The pipeline follows a structured flow managed by **Dagster**:
1.  **Extraction**: Scrapes messages and images from Telegram using Telethon.
2.  **Ingestion**: Loads raw data into a PostgreSQL `raw` schema. It uses an "append" strategy to maintain the stability of dependent dbt views.
3.  **Enrichment**: Executes YOLOv8 object detection on scraped images and stores results in the database.
4.  **Transformation**: Utilizes **dbt** to create staging views and analytical marts (Fact/Dimension tables).



## 📁 Project Structure
* `src/scraper.py`: Telethon-based scraper for Task 1.
* `scripts/load_to_postgres.py`: Python script for Task 2 database ingestion.
* `scripts/load_detections.py`: Ingests YOLOv8 results for Task 3.
* `medical_warehouse/`: dbt project for Task 4 transformations.
* `orchestration/pipeline.py`: Dagster job, ops, and daily schedule definitions for Task 5.

## 🚀 Getting Started

### 1. Installation
```bash
pip install dagster dagster-webserver dbt-postgres telethon pandas sqlalchemy psycopg2
```

### 2. Running the Orchestrator
To launch the pipeline and access the UI at [http://localhost:3000](http://localhost:3000):

```bash
dagster dev -f orchestration/pipeline.py
```

## 📅 Scheduling & Automation

* **Schedule**: The pipeline is configured to run **daily** at midnight (`00:00`) Africa/Addis_Ababa time.
* **Order**: Uses `In(Nothing)` to ensure strict sequential execution (Scrape → Load → Detect → Transform).
* **Observability**: Centralized logging and error tracking via the Dagster UI.
