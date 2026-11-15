{{ config(materialized='table') }}
select cast(range as date) as date_day
from range(date '2018-01-01', date '2018-03-01', interval 1 day)
