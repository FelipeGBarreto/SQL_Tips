with
t_calendar as (
select 
  calendar_date
  ,regexp_replace(substring_index(calendar_date,'-',2), '-', '/') as year_month
  ,regexp_replace(concat_ws('/',year(calendar_date),weekofyear(calendar_date)), '-', '/') as year_week
from (
select
  explode(
    sequence(to_date('2022-01-01'), 
    to_date('2023-12-31'), 
    interval 1 day) 
  ) as calendar_date
)
)


,base as (
select *
  ,dense_rank() over(order by year_month desc)-1 as Order
  ,concat("M-", dense_rank() over(order by year_month desc) -1) as Name
from t_calendar
where 1=1
  and calendar_date < date(now())
  and calendar_date >= add_months(date_trunc('month',now()), -3) -- Latest 3+1 months
)


select distinct year_month `Month`, Name, Order from base




