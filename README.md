# med-data-hub

# Medical Telegram Data Warehouse

This project is a robust data pipeline that scrapes medical-related data from Telegram channels, transforms it into a Star Schema, and prepares it for analysis and object detection.

## Project Structure
- `scripts/`: Python scripts for data loading and ingestion.
- `medical_warehouse/`: dbt project containing SQL models and tests.
  - `models/staging/`: Initial data cleaning and type casting.
  - `models/marts/`: Dimensional modeling (Fact and Dimension tables).
- `data/`: Local data lake for raw JSON and image files.

## Tech Stack
- **Database:** PostgreSQL
- **Transformation:** dbt (Data Build Tool)
- **Language:** Python 3.x
- **Orchestration:** Git/GitHub

## Getting Started
1. Install dependencies: `pip install -r requirements.txt`
2. Set up your `.env` with database credentials.
3. Run the scraper (Task 1).
4. Load raw data: `python scripts/load_to_postgres.py`
5. Run transformations: 
   ```powershell
   cd medical_warehouse
   dbt run --profiles-dir .
   dbt test --profiles-dir .