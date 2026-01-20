import os
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_raw_data():
    # Get DB credentials from .env
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')

    # Create connection engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    
    # Path to your JSON data lake
    data_dir = 'data/raw/telegram_messages'
    all_data = []

    # Check if directory exists
    if not os.path.exists(data_dir):
        print(f"Error: {data_dir} not found. Did you run the scraper?")
        return

    # Loop through date folders and JSON files
    for date_folder in os.listdir(data_dir):
        folder_path = os.path.join(data_dir, date_folder)
        if os.path.isdir(folder_path):
            for file in os.listdir(folder_path):
                if file.endswith('.json'):
                    with open(os.path.join(folder_path, file), 'r', encoding='utf-8') as f:
                        all_data.extend(json.load(f))

    if not all_data:
        print("No data found to load.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Ensure the 'raw' schema exists in Postgres
    with engine.connect() as conn:
        from sqlalchemy import text
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()
    
    # Load into the 'raw' schema as 'telegram_messages'
    df.to_sql('telegram_messages', engine, schema='raw', if_exists='append', index=False)  
    print(f"Successfully loaded {len(df)} rows into raw.telegram_messages!")

if __name__ == "__main__":
    load_raw_data()