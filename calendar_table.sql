select 
  calendar_date
  ,regexp_replace(substring_index(calendar_date,'-',2), '-', '/') as year_month
  ,regexp_replace(concat_ws('/',year(calendar_date),weekofyear(calendar_date)), '-', '/') as year_week
from (
select
  explode(
    sequence(to_date('2022-01-01'), 
    to_date('2022-12-31'), 
    interval 1 day) 
  ) as calendar_date
)
