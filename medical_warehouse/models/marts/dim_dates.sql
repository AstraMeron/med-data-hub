with date_series as (
    -- Generates a list of unique dates from our data
    select distinct 
        cast(message_timestamp as date) as full_date
    from {{ ref('stg_telegram_messages') }}
)

select
    -- Create a unique key like 20260118
    cast(to_char(full_date, 'YYYYMMDD') as integer) as date_key,
    full_date,
    extract(dow from full_date) as day_of_week,
    to_char(full_date, 'Day') as day_name,
    extract(week from full_date) as week_of_year,
    extract(month from full_date) as month,
    to_char(full_date, 'Month') as month_name,
    extract(quarter from full_date) as quarter,
    extract(year from full_date) as year,
    case 
        when extract(dow from full_date) in (0, 6) then true 
        else false 
    end as is_weekend
from date_series