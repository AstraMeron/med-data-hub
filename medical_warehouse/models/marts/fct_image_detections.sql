{{ config(materialized='table') }}

with yolo_raw as (
    -- This pulls from the table we just loaded via Python
    select * from {{ source('raw_data', 'yolo_detections') }}
),

messages as (
    select * from {{ ref('fct_messages') }}
)

select
    y.message_id,
    m.channel_key,
    m.date_key,
    y.detected_objects,
    y.confidence_score,
    y.image_category,
    m.view_count
from yolo_raw y
left join messages m on cast(y.message_id as integer) = m.message_id