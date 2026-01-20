from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import schemas, database

app = FastAPI(title="Medical Data Hub API", description="Analytical API for Ethiopia Medical Telegram Data")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Medical Data Hub API. Visit /docs for documentation."}

# Endpoint 1: Top Products (based on message content)
@app.get("/api/reports/top-products", response_model=List[schemas.ProductCount])
def get_top_products(limit: int = 10, db: Session = Depends(database.get_db)):
    query = text("""
        SELECT message_text as product_name, COUNT(*) as mention_count 
        FROM fct_messages 
        GROUP BY message_text 
        ORDER BY mention_count DESC LIMIT :limit
    """)
    result = db.execute(query, {"limit": limit}).fetchall()
    return result

# Endpoint 2: Channel Activity
@app.get("/api/channels/{channel_name}/activity", response_model=schemas.ChannelActivity)
def get_channel_activity(channel_name: str, db: Session = Depends(database.get_db)):
    query = text("""
        SELECT c.channel_name, COUNT(m.message_id) as message_count, AVG(m.view_count) as avg_views
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        WHERE c.channel_name = :name
        GROUP BY c.channel_name
    """)
    result = db.execute(query, {"name": channel_name}).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Channel not found")
    return result

# Endpoint 3: Message Search
@app.get("/api/search/messages", response_model=List[schemas.MessageResponse])
def search_messages(query: str, limit: int = 20, db: Session = Depends(database.get_db)):
    sql = text("""
        SELECT m.message_id, c.channel_name, m.message_text, m.view_count
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        WHERE m.message_text ILIKE :search
        LIMIT :limit
    """)
    result = db.execute(sql, {"search": f"%{query}%", "limit": limit}).fetchall()
    return result

# Endpoint 4: Visual Content Stats (from Task 3)
@app.get("/api/reports/visual-content", response_model=List[schemas.VisualStats])
def get_visual_stats(db: Session = Depends(database.get_db)):
    query = text("""
        SELECT c.channel_name, COUNT(f.message_id) as image_count, 
               MODE() WITHIN GROUP (ORDER BY f.image_category) as top_category
        FROM fct_image_detections f
        JOIN dim_channels c ON f.channel_key = c.channel_key
        GROUP BY c.channel_name
    """)
    result = db.execute(query).fetchall()
    return result