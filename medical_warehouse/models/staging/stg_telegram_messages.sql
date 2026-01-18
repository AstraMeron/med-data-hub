with raw_data as (
    select * from {{ source('raw_data', 'telegram_messages') }}
)

select
    -- 1. Identifiers
    message_id,
    channel_name,

    -- 2. Dates and Timestamps (Casting string to timestamp)
    cast(message_date as timestamp) as message_timestamp,

    -- 3. Content
    message_text,
    length(message_text) as message_length,
    
    -- 4. Flags (Boolean logic)
    case 
        when image_path is not null then true 
        else false 
    end as has_image,

    -- 5. Metrics (Casting to integers)
    cast(coalesce(views, 0) as integer) as view_count,
    cast(coalesce(forwards, 0) as integer) as forward_count

from raw_data
-- 6. Filtering out invalid records
where message_id is not null
  and message_text is not null