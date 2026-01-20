import pandas as pd
from sqlalchemy import create_engine

# 1. Connection setup using your exact profiles.yml credentials
# Format: postgresql://user:password@host:port/dbname
DB_URL = 'postgresql://postgres:meron@localhost:5432/medical_db'
engine = create_engine(DB_URL)

# 2. Load the CSV
try:
    df = pd.read_csv('data/detection_results.csv')
    
    # 3. Load to the 'raw' schema
    # Note: Ensure the 'raw' schema exists in your medical_db
    df.to_sql('yolo_detections', engine, schema='raw', if_exists='replace', index=False)
    
    print(f"Successfully loaded {len(df)} detection records to raw.yolo_detections in medical_db")

except Exception as e:
    print(f"Connection failed: {e}")