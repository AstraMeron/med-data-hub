with stg_data as (
    select * from {{ ref('stg_telegram_messages') }}
)

select
    -- 1. channel_key (surrogate key using md5)
    md5(channel_name) as channel_key,
    
    -- 2. channel_name
    channel_name,
    
    -- 3. channel_type (Pharmaceutical, Cosmetics, Medical)
    case 
        when channel_name in ('CheMed123', 'tikvahpharma') then 'Pharmaceutical'
        when channel_name = 'lobelia4cosmetics' then 'Cosmetics'
        else 'Medical'
    end as channel_type,
    
    -- 4. Dates
    min(message_timestamp) as first_post_date,
    max(message_timestamp) as last_post_date,
    
    -- 5. Metrics
    count(message_id) as total_posts,
    avg(view_count) as avg_views

from stg_data
group by channel_name