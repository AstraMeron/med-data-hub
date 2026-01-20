from dagster import op, job, schedule, In, Nothing, DefaultScheduleStatus
import subprocess
import os
from pathlib import Path

# Automatically identify the project root: C:\Users\Mer\med-data-hub
ROOT_DIR = Path(__file__).parent.parent.absolute()

def run_python_script(rel_path):
    """Verifies file existence and executes the script from the project root."""
    abs_path = ROOT_DIR / rel_path
    
    if not abs_path.exists():
        raise Exception(f"CRITICAL: File not found at {abs_path}. Check your folder names!")
    
    print(f"Executing: {abs_path}")
    result = subprocess.run(
        ["python", str(abs_path)],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Error in {rel_path}: {result.stderr}")
    return result.stdout

@op
def scrape_telegram_data():
    """Task 1: Run the scraper in src/scraper.py"""
    return run_python_script("src/scraper.py")

@op(ins={"start": In(Nothing)})
def load_raw_to_postgres():
    """Task 2: Load JSON to Postgres in scripts/load_to_postgres.py"""
    # Note: Ensure load_to_postgres.py uses if_exists='append' to avoid dbt conflicts
    return run_python_script("scripts/load_to_postgres.py")

@op(ins={"start": In(Nothing)})
def run_yolo_enrichment():
    """Task 3: Run YOLO detection and load results."""
    # This runs the detection script verified via your terminal 'ls' check
    return run_python_script("scripts/load_detections.py")

@op(ins={"start": In(Nothing)})
def run_dbt_transformations():
    """Task 4: Execute dbt models in medical_warehouse/."""
    dbt_dir = ROOT_DIR / "medical_warehouse"
    if not dbt_dir.exists():
        raise Exception(f"dbt directory not found at {dbt_dir}")
        
    result = subprocess.run(
        ["dbt", "run"],
        cwd=str(dbt_dir),
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"dbt failed: {result.stderr}")
    return "dbt Transformations Complete"

@job
def medical_data_pipeline():
    """
    Job Graph defining the dependencies.
    Order: Scrape -> Load -> YOLO -> dbt
    """
    out1 = scrape_telegram_data()
    out2 = load_raw_to_postgres(start=out1)
    out3 = run_yolo_enrichment(start=out2)
    run_dbt_transformations(start=out3)

@schedule(
    cron_schedule="0 0 * * *", 
    job=medical_data_pipeline, 
    execution_timezone="Africa/Addis_Ababa",
    default_status=DefaultScheduleStatus.RUNNING
)
def daily_medical_schedule():
    """Requirement: Configure the pipeline to run daily."""
    return {}