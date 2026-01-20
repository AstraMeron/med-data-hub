from fastapi import FastAPI, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import schemas, database

# 1. Added a detailed description for the main API
app = FastAPI(
    title="Medical Data Hub API",
    description="""
    This API provides analytical insights into Ethiopian medical Telegram data.
    You can query top products, channel activity, and visual content statistics.
    """,
    version="1.0.0"
)

@app.get("/", tags=["General"])
def read_root():
    """Returns a simple welcome message."""
    return {"message": "Welcome to the Medical Data Hub API. Visit /docs for documentation."}

# Endpoint 1: Top Products
@app.get("/api/reports/top-products", 
         response_model=List[schemas.ProductCount], 
         tags=["Reports"],
         summary="Get top mentioned products")
def get_top_products(
    limit: int = Query(10, description="The maximum number of products to return", gt=0, le=100),
    db: Session = Depends(database.get_db)
):
    """
    Returns the most frequently mentioned terms or products across all captured Telegram channels.
    This helps identify trending medical products in Ethiopia.
    """
    query = text("""
        SELECT message_text as product_name, COUNT(*) as mention_count 
        FROM fct_messages 
        GROUP BY message_text 
        ORDER BY mention_count DESC LIMIT :limit
    """)
    result = db.execute(query, {"limit": limit}).fetchall()
    return result

# Endpoint 2: Channel Activity
@app.get("/api/channels/{channel_name}/activity", 
         response_model=schemas.ChannelActivity, 
         tags=["Channels"],
         summary="Get channel posting trends")
def get_channel_activity(
    channel_name: str = Path(..., description="The name of the Telegram channel (e.g., DoctorsET)"),
    db: Session = Depends(database.get_db)
):
    """
    Returns posting activity and engagement trends (average views) for a specific channel.
    """
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
@app.get("/api/search/messages", 
         response_model=List[schemas.MessageResponse], 
         tags=["Search"],
         summary="Search messages by keyword")
def search_messages(
    query: str = Query(..., description="The keyword to search for (e.g., 'paracetamol')"),
    limit: int = Query(20, description="Number of search results to return"),
    db: Session = Depends(database.get_db)
):
    """
    Search for specific messages containing a keyword. Useful for tracking specific medicine mentions.
    """
    sql = text("""
        SELECT m.message_id, c.channel_name, m.message_text, m.view_count
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        WHERE m.message_text ILIKE :search
        LIMIT :limit
    """)
    result = db.execute(sql, {"search": f"%{query}%", "limit": limit}).fetchall()
    return result

# Endpoint 4: Visual Content Stats
@app.get("/api/reports/visual-content", 
         response_model=List[schemas.VisualStats], 
         tags=["Reports"],
         summary="Get image detection statistics")
def get_visual_stats(db: Session = Depends(database.get_db)):
    """
    Returns statistics about image usage across channels, including the total image count 
    and the most common category of images detected (via YOLOv8).
    """
    query = text("""
        SELECT c.channel_name, COUNT(f.message_id) as image_count, 
               MODE() WITHIN GROUP (ORDER BY f.image_category) as top_category
        FROM fct_image_detections f
        JOIN dim_channels c ON f.channel_key = c.channel_key
        GROUP BY c.channel_name
    """)
    result = db.execute(query).fetchall()
    return result