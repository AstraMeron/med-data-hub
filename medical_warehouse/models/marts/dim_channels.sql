with stg_data as (
    select * from {{ ref('stg_telegram_messages') }}
)

select
    -- Create a unique key for each channel
    md5(channel_name) as channel_key,
    channel_name,
    -- Basic categorization (you can expand this later)
    case 
        when channel_name in ('CheMed123', 'tikvahpharma') then 'Pharmaceutical'
        when channel_name = 'lobelia4cosmetics' then 'Cosmetics'
        else 'Medical'
    end as channel_type,
    min(message_timestamp) as first_post_date,
    max(message_timestamp) as last_post_date,
    count(message_id) as total_posts,
    avg(view_count) as avg_views
from stg_data
group by channel_name