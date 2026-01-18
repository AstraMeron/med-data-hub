with stg_data as (
    select * from {{ ref('stg_telegram_messages') }}
)

select
    message_id,
    md5(channel_name) as channel_key, -- Link to dim_channels
    cast(to_char(message_timestamp, 'YYYYMMDD') as integer) as date_key, -- Link to dim_dates
    message_text,
    message_length,
    view_count,
    forward_count,
    has_image
from stg_data