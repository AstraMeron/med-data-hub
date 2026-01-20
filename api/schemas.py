from pydantic import BaseModel
from typing import List, Optional

class ProductCount(BaseModel):
    product_name: str
    mention_count: int

class ChannelActivity(BaseModel):
    channel_name: str
    message_count: int
    avg_views: float

class MessageResponse(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    view_count: Optional[int]

class VisualStats(BaseModel):
    channel_name: str
    image_count: int
    top_category: str