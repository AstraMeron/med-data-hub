-- This test should return 0 rows. 
-- If it returns rows, the test fails.
select *
from {{ ref('fct_messages') }}
where date_key > cast(to_char(current_date, 'YYYYMMDD') as integer)